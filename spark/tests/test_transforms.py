"""Tests des transformations Silver et Gold sur des données produites par le générateur.

Exécution locale (Java + pyspark requis) :
    cd spark && python -m pytest -q tests
Les tests sont ignorés si pyspark n'est pas installé.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "spark" / "jobs"), str(ROOT / "generator")]

from common import transforms_gold as G  # noqa: E402
from common import transforms_silver as T  # noqa: E402
from common.datasets import DATASETS  # noqa: E402
from common.fx import XOF_PER_EUR  # noqa: E402
from waba_gen import config as cfg  # noqa: E402
from waba_gen.engine import (  # noqa: E402
    load_referentials,  # noqa: E402
    publish_referentials,
    run_one_time,
)
from waba_gen.storage import LocalSink  # noqa: E402
from waba_gen.transactions import RefIndex  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def bronze(spark, tmp_path_factory):
    """Données générées puis lues comme le ferait la couche Bronze (schéma explicite + colonnes techniques)."""
    root = tmp_path_factory.mktemp("landing")
    sink = LocalSink(root)
    publish_referentials(sink, seed=11, scale=0.02)
    idx = RefIndex.build(load_referentials(sink))
    rows = {ds: n for ds, n in cfg.DEFAULT_ROWS.items()}
    run_one_time(sink, idx, list(cfg.DATASETS), rows, cfg.COUNTRIES, cfg.ENTITY_TYPES,
                 date(2026, 4, 1), date(2026, 6, 30), seed=11)
    out = {}
    for name, ds in DATASETS.items():
        df = (spark.read.option("header", "true").option("recursiveFileLookup", "true")
              .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
              .schema(ds.schema).csv(str(root / ds.folder))
              .withColumn("_source_file", F.input_file_name())
              .withColumn("_ingested_at", F.current_timestamp()))
        out[name] = df.withColumn("ingestion_date", F.to_date("_ingested_at")).cache()
    # doublon volontaire : la même transaction ingérée deux fois
    out["bank_transactions"] = out["bank_transactions"].unionByName(out["bank_transactions"].limit(5))
    return out


@pytest.fixture(scope="session")
def silver(bronze):
    customers = T.silver_customers(bronze["customers"]).cache()
    products = T.silver_products(bronze["products"])
    branches = T.silver_branches(bronze["branches"])
    accounts = T.silver_accounts(bronze["accounts"], customers, products).cache()
    return {
        "customers": customers,
        "accounts": accounts,
        "bank": T.silver_bank_transactions(bronze["bank_transactions"], accounts, branches).cache(),
        "insurance": T.silver_insurance_operations(bronze["insurance_operations"], customers).cache(),
        "mobile": T.silver_mobile_money_payments(bronze["mobile_money_payments"], customers).cache(),
        "loans": T.silver_loan_repayments(bronze["loan_repayments"], accounts).cache(),
    }


# ------------------------------------------------------------------- silver
def test_dedup_on_transaction_id(bronze, silver):
    raw_n = bronze["bank_transactions"].count()
    distinct_n = bronze["bank_transactions"].select("transaction_id").distinct().count()
    assert raw_n == distinct_n + 5
    assert silver["bank"].count() == distinct_n
    assert silver["bank"].select("transaction_id").distinct().count() == silver["bank"].count()


def test_eur_conversion_xof_is_fixed_peg(silver):
    row = silver["bank"].filter("currency = 'XOF' AND amount > 0").select("amount", "amount_eur").first()
    assert row.amount_eur == pytest.approx(row.amount / XOF_PER_EUR, abs=0.01)


def test_enrichment_has_no_missing_customer(silver):
    assert silver["bank"].filter(F.col("customer_id").isNull()).count() == 0
    assert silver["loans"].filter(F.col("outstanding_eur") <= 0).count() == 0


def test_flags(silver):
    mm = silver["mobile"]
    assert mm.filter("is_cross_border").count() > 0
    assert mm.filter("is_unusual_country").count() > 0          # anomalies injectées par le générateur
    assert silver["bank"].filter("is_aml_candidate").count() > 0


# ------------------------------------------------------------------- gold
def test_daily_volume_totals(silver):
    gold = G.daily_transaction_volume(silver["bank"])
    assert gold.agg(F.sum("txn_count")).first()[0] == silver["bank"].count()


def test_npl_ratio_realistic(silver):
    npl = G.npl_ratio_by_country(silver["loans"]).filter("loan_type = 'ALL'")
    by_country = (npl.groupBy("country_code")
                  .agg((F.sum("npl_outstanding_eur") / F.sum("total_outstanding_eur")).alias("r")).collect())
    ratios = {r.country_code: r.r for r in by_country}
    assert all(0.0 <= v <= 0.15 for v in ratios.values()), ratios
    assert set(npl.select("risk_level").distinct().toPandas()["risk_level"]) <= {"GREEN", "ORANGE", "RED"}


def test_loss_ratio_realistic(silver):
    lr = G.loss_ratio_by_product(silver["insurance"]).filter("product_line = 'ALL'")
    agg = lr.groupBy("country_code").agg((F.sum("claims_paid_eur") / F.sum("premiums_eur")).alias("r")).collect()
    assert all(0.45 <= r.r <= 0.95 for r in agg), {r.country_code: r.r for r in agg}


def test_arpc_positive(silver):
    arpc = G.customer_arpu_monthly(silver["bank"], silver["mobile"], silver["loans"])
    assert arpc.filter("arpc_eur IS NULL OR arpc_eur <= 0").count() == 0
    assert arpc.count() > 0


def test_claims_and_mobile_and_cross_border(silver):
    claims = G.claims_processing_time(silver["insurance"])
    assert set(r.line_group for r in claims.select("line_group").distinct().collect()) == {"IARD", "VIE"}
    flow = G.mobile_money_daily_flow(silver["mobile"])
    assert flow.filter("failure_rate < 0 OR failure_rate > 1").count() == 0
    cb = G.cross_border_transfers(silver["mobile"])
    assert cb.filter("transfers_count <= 0").count() == 0
    assert "wow_change_pct" in cb.columns


def test_regulatory_report(silver):
    npl = G.npl_ratio_by_country(silver["loans"])
    rep = G.regulatory_daily_report("2026-05-15", silver["bank"], npl, silver["insurance"])
    rows = {r.country_code: r for r in rep.collect()}
    assert set(rows) == set(cfg.COUNTRIES)
    assert rows["GH"].banking_regulator == "BOG" and rows["CI"].banking_regulator == "BCEAO"
    assert all(r.npl_ratio is not None for r in rows.values())
