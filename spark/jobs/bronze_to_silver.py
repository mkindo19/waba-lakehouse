"""Niveau 2 — bronze.* -> silver.* (nettoyage, dédoublonnage, enrichissement, EUR).

Chaque domaine est RECALCULÉ pour les pays demandés puis écrit en écrasant
uniquement ses partitions (pays x mois) : rejouer le job ne crée jamais de doublon
(idempotence par construction) et permet un backfill sélectif par pays.

Usage :
    spark-submit bronze_to_silver.py --domain all
    spark-submit bronze_to_silver.py --domain bank_transactions --country SN --ghs-per-eur 14.6
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import transforms_silver as T
from common.fx import DEFAULT_GHS_PER_EUR, fx_rates_df
from common.logging_utils import get_logger
from common.session import get_spark
from common.tables import filter_countries, fq, overwrite_partitions, parse_countries

log = get_logger("bronze_to_silver")

FLOW_PARTITIONS = ["country_code", "txn_month"]
REF_PARTITIONS = ["country_code"]


def bronze(spark: SparkSession, name: str, countries: list[str] | None = None) -> DataFrame:
    df = spark.table(fq("bronze", name))
    return filter_countries(df, countries) if countries else df


def silver(spark: SparkSession, name: str) -> DataFrame:
    return spark.table(fq("silver", name))


def _stamp(df: DataFrame) -> DataFrame:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return df.withColumn("_silver_processed_at", F.lit(now).cast("timestamp"))


def build_referentials(spark: SparkSession, countries: list[str], ghs: float) -> dict[str, int]:
    out = {}
    customers = T.silver_customers(bronze(spark, "customers", countries))
    products = T.silver_products(bronze(spark, "products"))           # catalogue complet (petit)
    branches = T.silver_branches(bronze(spark, "branches", countries))
    out["customers"] = overwrite_partitions(spark, _stamp(customers), fq("silver", "customers"), REF_PARTITIONS)
    out["products"] = overwrite_partitions(spark, _stamp(products), fq("silver", "products"), REF_PARTITIONS)
    out["branches"] = overwrite_partitions(spark, _stamp(branches), fq("silver", "branches"), REF_PARTITIONS)
    accounts = T.silver_accounts(bronze(spark, "accounts", countries), silver(spark, "customers"),
                                 silver(spark, "products"), ghs)
    out["accounts"] = overwrite_partitions(spark, _stamp(accounts), fq("silver", "accounts"), REF_PARTITIONS)
    # Taux de change utilisés : petite table de référence (traçabilité des conversions)
    fx = fx_rates_df(spark, ghs)
    fx.writeTo(fq("silver", "fx_rates")).using("iceberg").createOrReplace()
    out["fx_rates"] = fx.count()
    return out


def build_bank(spark: SparkSession, countries: list[str], ghs: float) -> dict[str, int]:
    df = T.silver_bank_transactions(bronze(spark, "bank_transactions", countries),
                                    silver(spark, "accounts"),        # tous pays : bénéficiaires étrangers
                                    silver(spark, "branches"), ghs)
    return {"bank_transactions": overwrite_partitions(spark, _stamp(df), fq("silver", "bank_transactions"),
                                                      FLOW_PARTITIONS)}


def build_insurance(spark: SparkSession, countries: list[str], ghs: float) -> dict[str, int]:
    df = T.silver_insurance_operations(bronze(spark, "insurance_operations", countries),
                                       silver(spark, "customers"), ghs)
    return {"insurance_operations": overwrite_partitions(spark, _stamp(df), fq("silver", "insurance_operations"),
                                                         FLOW_PARTITIONS)}


def build_mobile_money(spark: SparkSession, countries: list[str], ghs: float) -> dict[str, int]:
    df = T.silver_mobile_money_payments(bronze(spark, "mobile_money_payments", countries),
                                        silver(spark, "customers"), ghs)  # tous pays : expéditeurs étrangers
    return {"mobile_money_payments": overwrite_partitions(spark, _stamp(df), fq("silver", "mobile_money_payments"),
                                                          FLOW_PARTITIONS)}


def build_loans(spark: SparkSession, countries: list[str], ghs: float) -> dict[str, int]:
    df = T.silver_loan_repayments(bronze(spark, "loan_repayments", countries), silver(spark, "accounts"), ghs)
    return {"loan_repayments": overwrite_partitions(spark, _stamp(df), fq("silver", "loan_repayments"),
                                                    FLOW_PARTITIONS)}


DOMAINS: dict[str, Callable[[SparkSession, list[str], float], dict[str, int]]] = {
    "referentials": build_referentials,
    "bank_transactions": build_bank,
    "insurance_operations": build_insurance,
    "mobile_money_payments": build_mobile_money,
    "loan_repayments": build_loans,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="bronze.* -> silver.*")
    p.add_argument("--domain", default="all", choices=["all", *DOMAINS])
    p.add_argument("--country", default="ALL")
    p.add_argument("--ghs-per-eur", type=float, default=DEFAULT_GHS_PER_EUR)
    args = p.parse_args(argv)
    countries = parse_countries(args.country)
    names = list(DOMAINS) if args.domain == "all" else [args.domain]

    spark = get_spark(f"bronze_to_silver_{args.domain}")
    failures = 0
    for name in names:
        started = time.monotonic()
        try:
            rows = DOMAINS[name](spark, countries, args.ghs_per_eur)
            log.info("silver construit", extra={"ctx": {"domain": name, "rows": rows, "countries": countries,
                                                         "duration_s": round(time.monotonic() - started, 1)}})
        except Exception:
            failures += 1
            log.exception("échec silver", extra={"ctx": {"domain": name}})
            if name == "referentials":
                break   # les flux dépendent des référentiels silver
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
