"""Niveau 2 — raw.* -> bronze.* (zone Bronze de l'architecture médaillon).

Bronze = données brutes ayant passé la validation de schéma, SANS transformation
métier, partitionnées par pays et par DATE D'INGESTION (traçabilité des
chargements, rejeu d'une journée d'ingestion).

Incrémental : seules les lignes raw ingérées après le dernier chargement Bronze
(watermark par pays sur _ingested_at) sont copiées. Le MERGE sur la clé garantit
l'idempotence même si le watermark est rejoué.

Usage : spark-submit raw_to_bronze.py [--dataset all|<nom>] [--country ALL|CI|CI,SN]
"""

from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.datasets import DATASETS, INGESTION_ORDER, Dataset
from common.logging_utils import get_logger
from common.session import get_spark
from common.tables import filter_countries, fq, merge_insert, parse_countries, table_exists

log = get_logger("raw_to_bronze")


def new_rows(spark: SparkSession, ds: Dataset, countries: list[str]) -> DataFrame:
    """Lignes raw postérieures au watermark Bronze de leur pays."""
    raw = filter_countries(spark.table(ds.table), countries)
    target = fq("bronze", ds.name)
    if table_exists(spark, target):
        wm = (spark.table(target).groupBy("country_code")
              .agg(F.max("_ingested_at").alias("_wm")))
        raw = (raw.join(F.broadcast(wm), "country_code", "left")
                  .filter(F.col("_wm").isNull() | (F.col("_ingested_at") > F.col("_wm")))
                  .drop("_wm"))
    return raw.withColumn("ingestion_date", F.to_date("_ingested_at"))


def load_dataset(spark: SparkSession, ds: Dataset, countries: list[str]) -> int:
    started = time.monotonic()
    df = new_rows(spark, ds, countries).cache()
    n = df.count()
    if n:
        partitions = ["country_code"] if ds.is_referential else ["country_code", "ingestion_date"]
        merge_insert(spark, df, fq("bronze", ds.name), keys=[ds.key, "country_code"],
                     partition_cols=partitions, upsert=ds.is_referential)
    df.unpersist()
    log.info("bronze chargé", extra={"ctx": {"dataset": ds.name, "rows": n, "countries": countries,
                                              "duration_s": round(time.monotonic() - started, 1)}})
    return n


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="raw.* -> bronze.*")
    p.add_argument("--dataset", default="all", choices=["all", *DATASETS])
    p.add_argument("--country", default="ALL")
    args = p.parse_args(argv)
    countries = parse_countries(args.country)
    names = INGESTION_ORDER if args.dataset == "all" else [args.dataset]

    spark = get_spark("raw_to_bronze")
    failures = 0
    for name in names:
        try:
            if not table_exists(spark, DATASETS[name].table):
                log.warning("table raw absente, ignorée", extra={"ctx": {"dataset": name}})
                continue
            load_dataset(spark, DATASETS[name], countries)
        except Exception:
            failures += 1
            log.exception("échec bronze", extra={"ctx": {"dataset": name}})
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
