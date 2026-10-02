"""Niveau 2 — silver.* -> gold.* : les 7 KPIs financiers et réglementaires.

Chaque KPI est recalculé pour les pays demandés et écrit en écrasant ses
partitions pays (idempotent, backfill sélectif possible).

Usage :
    spark-submit silver_to_gold.py --kpi all
    spark-submit silver_to_gold.py --kpi banking --country CI
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import transforms_gold as G
from common.logging_utils import get_logger
from common.session import get_spark
from common.tables import filter_countries, fq, overwrite_partitions, parse_countries

log = get_logger("silver_to_gold")
GOLD_PARTITIONS = ["country_code"]


def silver(spark: SparkSession, name: str, countries: list[str]) -> DataFrame:
    return filter_countries(spark.table(fq("silver", name)), countries)


def write(spark: SparkSession, df: DataFrame, name: str, comment: str) -> int:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = df.withColumn("_gold_computed_at", F.lit(now).cast("timestamp"))
    return overwrite_partitions(spark, df, fq("gold", name), GOLD_PARTITIONS, comment)


def gold_banking(spark: SparkSession, countries: list[str]) -> dict[str, int]:
    bank = silver(spark, "bank_transactions", countries)
    loans = silver(spark, "loan_repayments", countries)
    mobile = silver(spark, "mobile_money_payments", countries)
    return {
        "daily_transaction_volume": write(spark, G.daily_transaction_volume(bank), "daily_transaction_volume",
                                          "Volume et montant des transactions par jour, pays, entite et type"),
        "npl_ratio_by_country": write(spark, G.npl_ratio_by_country(loans), "npl_ratio_by_country",
                                      "Taux de creances douteuses (seuil BCEAO 5 pct)"),
        "customer_arpu_monthly": write(spark, G.customer_arpu_monthly(bank, mobile, loans), "customer_arpu_monthly",
                                       "Revenu moyen par client actif (commissions + interets)"),
    }


def gold_insurance(spark: SparkSession, countries: list[str]) -> dict[str, int]:
    ins = silver(spark, "insurance_operations", countries)
    return {
        "loss_ratio_by_product": write(spark, G.loss_ratio_by_product(ins), "loss_ratio_by_product",
                                       "Ratio sinistres / primes (seuil de vigilance CIMA 70 pct)"),
        "claims_processing_time": write(spark, G.claims_processing_time(ins), "claims_processing_time",
                                        "Delai de traitement des sinistres (jours ouvres)"),
    }


def gold_mobile_money(spark: SparkSession, countries: list[str]) -> dict[str, int]:
    mm = silver(spark, "mobile_money_payments", countries)
    return {
        "mobile_money_daily_flow": write(spark, G.mobile_money_daily_flow(mm), "mobile_money_daily_flow",
                                         "Flux journalier mobile money par pays"),
        "cross_border_transfers": write(spark, G.cross_border_transfers(mm), "cross_border_transfers",
                                        "Transferts transfrontaliers par corridor et semaine"),
    }


GROUPS: dict[str, Callable[[SparkSession, list[str]], dict[str, int]]] = {
    "banking": gold_banking,
    "insurance": gold_insurance,
    "mobile_money": gold_mobile_money,
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="silver.* -> gold.*")
    p.add_argument("--kpi", default="all", choices=["all", *GROUPS])
    p.add_argument("--country", default="ALL")
    args = p.parse_args(argv)
    countries = parse_countries(args.country)
    names = list(GROUPS) if args.kpi == "all" else [args.kpi]

    spark = get_spark(f"silver_to_gold_{args.kpi}")
    failures = 0
    for name in names:
        started = time.monotonic()
        try:
            rows = GROUPS[name](spark, countries)
            log.info("gold calculé", extra={"ctx": {"group": name, "rows": rows, "countries": countries,
                                                     "duration_s": round(time.monotonic() - started, 1)}})
        except Exception:
            failures += 1
            log.exception("échec gold", extra={"ctx": {"group": name}})
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
