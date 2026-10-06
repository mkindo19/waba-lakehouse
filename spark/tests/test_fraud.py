"""Tests du job streaming 2 (common/fraud.py) sur des DataFrames statiques."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark" / "jobs"))

from common import fraud  # noqa: E402

BANK_COLS = ("transaction_id STRING, account_id STRING, beneficiary_account STRING, country_code STRING, "
             "transaction_type STRING, amount DOUBLE, currency STRING, amount_eur DOUBLE, transaction_status STRING, "
             "channel STRING, event_time TIMESTAMP, txn_date DATE, is_aml_candidate BOOLEAN")


def ts(hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(2026, 10, 6, hh, mm, ss)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests-fraud")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


def bank(spark, rows):
    """rows : (id, compte, type, montant XOF, statut, heure)"""
    data = [(i, acc, "B-1", "SN", typ, float(amt), "XOF", round(amt / 655.957, 2), st, "MOBILE", t, t.date(),
             typ == "TRANSFER" and amt > 1_000_000)
            for i, acc, typ, amt, st, t in rows]
    return spark.createDataFrame(data, BANK_COLS)


def test_burst_gives_one_alert_despite_sliding_windows(spark):
    df = bank(spark, [
        ("T1", "A", "TRANSFER", 600_000, "SUCCESS", ts(10, 1, 10)),
        ("T2", "A", "WITHDRAWAL", 700_000, "SUCCESS", ts(10, 2, 30)),
        ("T3", "A", "TRANSFER", 800_000, "SUCCESS", ts(10, 3, 50)),
        ("T4", "A", "TRANSFER", 100_000, "SUCCESS", ts(10, 3, 55)),     # sous le seuil
        ("T5", "B", "TRANSFER", 900_000, "SUCCESS", ts(10, 2, 0)),      # compte seul : pas de rafale
        ("T6", "A", "TRANSFER", 900_000, "SUCCESS", ts(11, 30, 0)),     # isolée, 1 h plus tard
    ])
    windows = fraud.burst_windows(df)
    assert windows.count() > 1                       # la rafale apparaît dans plusieurs fenêtres glissantes
    alerts = fraud.burst_alerts(windows).collect()
    assert len(alerts) == 1                          # ... mais ne donne qu'UNE alerte
    a = alerts[0]
    assert a.entity_id == "A" and a.event_count == 3 and a.rule == "BURST_HIGH_VALUE"
    assert sorted(e.event_id for e in a.events) == ["T1", "T2", "T3"]
    assert a.first_event_time == ts(10, 1, 10) and a.last_event_time == ts(10, 3, 50)


def test_burst_extends_existing_alert_instead_of_duplicating(spark):
    first = fraud.burst_alerts(fraud.burst_windows(bank(spark, [
        ("T1", "A", "TRANSFER", 600_000, "SUCCESS", ts(10, 1)),
        ("T2", "A", "TRANSFER", 600_000, "SUCCESS", ts(10, 2)),
    ])))
    prev = first.withColumn("alert_status", fraud.F.lit("NEW"))
    later = fraud.burst_windows(bank(spark, [         # le micro-lot suivant apporte une 3e transaction
        ("T2", "A", "TRANSFER", 600_000, "SUCCESS", ts(10, 2)),
        ("T3", "A", "TRANSFER", 600_000, "SUCCESS", ts(10, 4)),
    ]))
    alerts = fraud.burst_alerts(later, prev)
    changed = fraud.changed_alerts(alerts, prev, "event_count", higher_is_worse=True).collect()
    assert len(changed) == 1
    assert changed[0].alert_id == first.first().alert_id and changed[0].alert_status == "UPDATED"
    assert changed[0].event_count == 3
    # rejouer le même micro-lot : rien de nouveau à publier
    assert fraud.changed_alerts(first, prev, "event_count", higher_is_worse=True).count() == 0


def test_unusual_country(spark):
    mm = spark.createDataFrame(
        [("P1", "C-CI-1", "R", "SN", "SN", "SN", 10_000.0, "XOF", 15.24, "P2P", "SUCCESS", ts(9, 0)),
         ("P2", "C-SN-1", "R", "SN", "SN", "SN", 10_000.0, "XOF", 15.24, "P2P", "SUCCESS", ts(9, 1))],
        fraud.SILVER_SCHEMAS["mobile_money"])
    customers = spark.createDataFrame([("C-CI-1", "CI"), ("C-SN-1", "SN")], "customer_id STRING, country_code STRING")
    rows = fraud.unusual_country_alerts(mm, customers).collect()
    assert [r.entity_id for r in rows] == ["C-CI-1"]
    assert json.loads(rows[0].details)["home_country"] == "CI"


def test_claim_over_three_annual_premiums(spark):
    ins = spark.createDataFrame(
        [("O1", "C1", "POL-1", "SN", "CLAIM_PAYMENT", "IARD_AUTO", 400_000.0, "XOF", 609.8, ts(9, 0)),   # > 3 x 120 000
         ("O2", "C1", "POL-1", "SN", "CLAIM_PAYMENT", "IARD_AUTO", 300_000.0, "XOF", 457.3, ts(9, 5)),   # < 3 x 120 000
         ("O3", "C1", "POL-1", "SN", "PREMIUM_PAYMENT", "IARD_AUTO", 900_000.0, "XOF", 1372.0, ts(9, 6))],
        fraud.SILVER_SCHEMAS["insurance"])
    accounts = spark.createDataFrame([("POL-1", "INSURANCE_POLICY", "ACTIVE", "SN", 10_000.0, 15.24)],
                                     "account_id STRING, account_type STRING, status STRING, country_code STRING, "
                                     "balance DOUBLE, balance_eur DOUBLE")
    rows = fraud.claim_alerts(ins, accounts).collect()
    assert [r.events[0].event_id for r in rows] == ["O1"]
    assert json.loads(rows[0].details)["annual_premium"] == 120_000.0


def test_aml_only_successful_transfers_above_threshold(spark):
    df = bank(spark, [
        ("T1", "A", "TRANSFER", 1_500_000, "SUCCESS", ts(9, 0)),
        ("T2", "A", "TRANSFER", 1_500_000, "FAILED", ts(9, 1)),
        ("T3", "A", "WITHDRAWAL", 5_000_000, "SUCCESS", ts(9, 2)),
        ("T4", "A", "TRANSFER", 900_000, "SUCCESS", ts(9, 3)),
    ])
    rows = fraud.aml_events(df).collect()
    assert [r.event_id for r in rows] == ["T1"] and rows[0].regulator == "BCEAO"


def test_liquidity_alert_one_per_episode(spark):
    df = bank(spark, [
        ("T1", "A", "WITHDRAWAL", 65_595_700, "SUCCESS", ts(10, 2)),    # 100 000 EUR de sorties
        ("T2", "B", "DEPOSIT", 6_559_570, "SUCCESS", ts(10, 3)),        # 10 000 EUR d'entrées
        ("T3", "C", "WITHDRAWAL", 655_957, "SUCCESS", ts(12, 0)),       # 1 000 EUR : sans effet
    ])
    reference = spark.createDataFrame([("SN", 1_000_000.0)], "country_code STRING, reference_liquidity_eur DOUBLE")
    rows = fraud.liquidity_alerts(fraud.liquidity_windows(df), reference, min_coverage=0.98).collect()
    assert len(rows) == 1                             # 5 fenêtres glissantes sous le seuil -> 1 épisode
    # pire fenêtre : celle qui contient le retrait mais pas encore le dépôt (10:02 inclus, 10:03 exclu)
    assert rows[0].net_outflow_eur == pytest.approx(100_000.0) and rows[0].coverage_ratio == pytest.approx(0.90)
    assert rows[0].severity == "HIGH"
