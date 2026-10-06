"""Tests du job streaming 1 (common/streaming.py) sur des DataFrames statiques.

Les messages imitent exactement ce que publie NiFi : une ligne CSV en JSON, valeurs en texte,
+ ingestion_timestamp et source_file.
"""

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

from common.fx import XOF_PER_EUR  # noqa: E402
from common.quality import REASON_COL  # noqa: E402
from common.streaming import (  # noqa: E402
    dlq_messages,
    kafka_messages,
    parse_raw,
    spec_for,
    to_silver,
    with_reason,
)

BASE = {
    "transaction_id": "T-1", "timestamp": "2026-10-05T10:00:00", "account_id": "WABA-CI-A-1",
    "beneficiary_account": "WABA-CI-A-2", "branch_id": "WABA-CI-B-001", "country_code": "CI",
    "transaction_type": "TRANSFER", "amount": "1500000.0", "currency": "XOF", "channel": "MOBILE",
    "transaction_status": "SUCCESS", "fee_amount": "1500.0", "entity_type": "BANK",
    "source_file": "s3://raw-landing/bank_transactions/CI/x.csv", "ingestion_timestamp": "2026-10-06T01:00:00.000Z",
}


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests-streaming")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _kafka_df(spark, values: list[str]):
    ts = datetime(2026, 10, 6, 1, 0, 0)
    return spark.createDataFrame([(v, ts) for v in values], "value string, _kafka_ts timestamp")


def _checked(spark, values):
    ds, _ = spec_for("bank_transactions")
    return with_reason(parse_raw(_kafka_df(spark, values), ds), ds)


def test_valid_message_is_typed_and_converted(spark):
    ds, _ = spec_for("bank_transactions")
    row = _checked(spark, [json.dumps(BASE)]).first()
    assert row[REASON_COL] is None
    assert row["amount"] == 1500000.0 and row["timestamp"] == datetime(2026, 10, 5, 10, 0, 0)
    silver = to_silver(_checked(spark, [json.dumps(BASE)]), ds, processed_at="2026-10-06T01:00:05").first()
    assert silver["amount_eur"] == pytest.approx(round(1500000 / XOF_PER_EUR, 2))
    assert silver["streaming_amount_eur"] == silver["amount_eur"]
    assert silver["is_aml_candidate"] is True            # virement > 1 000 000 XOF
    assert str(silver["txn_date"]) == "2026-10-05"


@pytest.mark.parametrize("change, reason", [
    ({"amount": "12a"}, "MALFORMED_RECORD"),             # montant non numérique
    ({"currency": "GHS"}, "CURRENCY_MISMATCH"),          # GHS en Côte d'Ivoire
    ({"transaction_id": ""}, "MISSING_KEY"),
    ({"transaction_status": "BIZARRE"}, "INVALID_TRANSACTION_STATUS"),
])
def test_bad_messages_get_a_reason(spark, change, reason):
    assert _checked(spark, [json.dumps(BASE | change)]).first()[REASON_COL] == reason


def test_invalid_json_goes_to_dlq_with_original_message(spark):
    ds, spec = spec_for("bank_transactions")
    rejected = _checked(spark, ["{pas du json"])
    assert rejected.first()[REASON_COL] == "MALFORMED_RECORD"
    msg = json.loads(dlq_messages(rejected, ds, spec, "2026-10-06T01:00:05").first()["value"])
    assert msg["raw_message"] == "{pas du json" and msg["source_topic"] == "raw-bank-transactions"


def test_dedup_key_and_kafka_key(spark):
    ds, _ = spec_for("bank_transactions")
    rows = _checked(spark, [json.dumps(BASE), "{pas du json"]).collect()
    assert rows[0]["_dedup_key"] == "T-1"
    assert len(rows[1]["_dedup_key"]) == 64             # empreinte SHA-256 du message illisible
    out = kafka_messages(to_silver(_checked(spark, [json.dumps(BASE)]), ds, "2026-10-06T01:00:05")).first()
    assert out["key"] == "CI" and json.loads(out["value"])["transaction_id"] == "T-1"
