"""Niveau 3 — Job Spark Structured Streaming 2 : silver-* (Kafka) -> gold-* (Kafka) + gold.rt_* (Iceberg).

Cinq requêtes de streaming dans la même application Spark :

    silver-bank-transactions ──┬─ R1 rafales > 500 000 XOF (fenêtres 5 min / 1 min) ─┐
                               ├─ AML : virement > seuil déclaratif ─────────────────┼─► gold-aml-events
                               └─ liquidité par pays (fenêtres 5 min / 1 min) ───────┼─► gold-liquidity-alerts
    silver-mobile-money ─────── R2 pays inhabituel (+ référentiel clients) ──────────┤
    silver-insurance-operations ─ R3 sinistre > 3 x prime annuelle (+ comptes) ──────┴─► gold-fraud-alerts

- Fenêtres glissantes sur l'HEURE MÉTIER (event_time), watermark de 2 jours (paramétrable) : le
  générateur horodate ses lots sur toute la journée en cours (cf. D23).
- Référentiels (clients, comptes) lus dans Iceberg silver.*, gardés en cache 10 min (jointure stream-static).
- Publication : seules les alertes NOUVELLES ou AGGRAVÉES sont publiées (comparaison avec gold.rt_*),
  d'abord dans Kafka puis MERGE Iceberg. Si le job tombe entre les deux, le micro-lot rejoué republie
  la même alerte (même alert_id, clé Kafka) : au moins une fois, jamais perdue.

Usage (service Docker `stream-fraud-aml`, profil streaming) :
    spark-submit stream_fraud_aml.py [--queries bursts aml liquidity unusual_country claims] [--trigger "15 seconds"]
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import fraud
from common.datasets import CATALOG
from common.logging_utils import get_logger
from common.stream_monitor import StreamMonitor
from common.stream_sinks import CHECKPOINT_ROOT, KAFKA, merge_iceberg, write_kafka

log = get_logger("stream_fraud_aml")

JOB = "fraud_aml"
WATERMARK = os.environ.get("STREAM_EVENT_WATERMARK", "2 days")
REF_TTL_S = int(os.environ.get("STREAM_REF_TTL_SECONDS", "600"))
MIN_COVERAGE = float(os.environ.get("LIQUIDITY_MIN_COVERAGE", fraud.LIQUIDITY_MIN_COVERAGE))

FRAUD_TABLE = f"{CATALOG}.gold.rt_fraud_alerts"
AML_TABLE = f"{CATALOG}.gold.rt_aml_events"
LIQUIDITY_TABLE = f"{CATALOG}.gold.rt_liquidity_alerts"
TOPICS = {"bank": "silver-bank-transactions", "mobile_money": "silver-mobile-money",
          "insurance": "silver-insurance-operations"}


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


class RefCache:
    """Référentiel Iceberg gardé en mémoire, relu toutes les `ttl` secondes (nouveaux clients, soldes)."""

    def __init__(self, loader: Callable[[SparkSession], DataFrame], ttl: int = REF_TTL_S):
        self.loader, self.ttl = loader, ttl
        self.df: DataFrame | None = None
        self.loaded_at = 0.0
        self._lock = threading.Lock()                      # requêtes concurrentes (threads du driver)

    def get(self, spark: SparkSession) -> DataFrame:
        with self._lock:
            if self.df is None or time.monotonic() - self.loaded_at > self.ttl:
                fresh = self.loader(spark).persist()
                fresh.count()                              # matérialise le cache
                old, self.df, self.loaded_at = self.df, fresh, time.monotonic()
                if old is not None:
                    old.unpersist()
            return self.df


CUSTOMERS = RefCache(lambda s: s.table(f"{CATALOG}.silver.customers").select("customer_id", "country_code"))
ACCOUNTS = RefCache(lambda s: s.table(f"{CATALOG}.silver.accounts")
                    .select("account_id", "account_type", "status", "country_code", "balance", "balance_eur"))
REFERENCE = RefCache(lambda s: fraud.reference_liquidity(ACCOUNTS.get(s)))


def existing(spark: SparkSession, table: str) -> DataFrame | None:
    return spark.table(table) if spark.catalog.tableExists(table) else None


def publish(spark: SparkSession, changed: DataFrame, topic: str, table: str, key: str,
            partition_cols: list[str], comment: str) -> int:
    """Kafka puis Iceberg (upsert). La source est figée (localCheckpoint) : le MERGE modifie la table
    qui a servi à calculer `changed`, il ne faut pas que Spark la recalcule après coup."""
    frozen = changed.withColumn("detected_at", F.lit(_now())).localCheckpoint(eager=True)
    n = frozen.count()
    if n:
        write_kafka(fraud.to_kafka(frozen, key), topic)
        merge_iceberg(spark, frozen, table, key, partition_cols, upsert=True, comment=comment)
    return n


# ---------------------------------------------------------------------------- traitements par micro-lot
def _log(query: str, batch_id: int, published: int) -> None:
    log.info("micro-lot traité", extra={"ctx": {"query": query, "batch_id": batch_id, "published": published}})


def _fraud_writer(query: str, build: Callable[[SparkSession, DataFrame, DataFrame | None], DataFrame]):
    def process(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        spark = batch.sparkSession
        prev = existing(spark, FRAUD_TABLE)
        alerts = build(spark, batch, prev)
        changed = fraud.changed_alerts(alerts, prev, "event_count", higher_is_worse=True)
        n = publish(spark, changed.select(*fraud.ALERT_COLUMNS, "alert_status"), fraud.FRAUD_TOPIC,
                    FRAUD_TABLE, "alert_id", ["rule", "country_code"],
                    "Alertes de fraude temps réel (job streaming 2)")
        _log(query, batch_id, n)
    return process


def bursts_writer():
    return _fraud_writer("bursts", lambda spark, b, prev: fraud.burst_alerts(b, prev))


def unusual_country_writer():
    return _fraud_writer("unusual_country", lambda spark, b, prev: fraud.unusual_country_alerts(b, CUSTOMERS.get(spark)))


def claims_writer():
    return _fraud_writer("claims", lambda spark, b, prev: fraud.claim_alerts(b, ACCOUNTS.get(spark)))


def aml_writer(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    spark = batch.sparkSession
    events = fraud.aml_events(batch)
    changed = fraud.changed_alerts(events.withColumnRenamed("event_id", "alert_id"),
                                   _rename(existing(spark, AML_TABLE)), "amount_eur", higher_is_worse=True)
    n = publish(spark, changed.withColumnRenamed("alert_id", "event_id").drop("alert_status"), fraud.AML_TOPIC,
                AML_TABLE, "event_id", ["country_code", "txn_date"], "Événements AML temps réel (job streaming 2)")
    _log("aml", batch_id, n)


def _rename(df: DataFrame | None) -> DataFrame | None:
    return None if df is None else df.withColumnRenamed("event_id", "alert_id")


def liquidity_writer(batch: DataFrame, batch_id: int) -> None:
    if batch.isEmpty():
        return
    spark = batch.sparkSession
    prev = existing(spark, LIQUIDITY_TABLE)
    alerts = fraud.liquidity_alerts(batch, REFERENCE.get(spark), prev, MIN_COVERAGE)
    changed = fraud.changed_alerts(alerts, prev, "coverage_ratio", higher_is_worse=False)
    n = publish(spark, changed, fraud.LIQUIDITY_TOPIC, LIQUIDITY_TABLE, "alert_id", ["country_code"],
                "Alertes de liquidité temps réel (job streaming 2)")
    _log("liquidity", batch_id, n)


# ---------------------------------------------------------------------------- requêtes
def silver_stream(spark: SparkSession, kind: str) -> DataFrame:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", TOPICS[kind])
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 20000)
           .option("failOnDataLoss", "false")
           .load())
    return fraud.parse_silver(raw, kind)


def build_queries(spark: SparkSession) -> dict[str, tuple[DataFrame, Callable, str]]:
    """nom -> (DataFrame en streaming, fonction foreachBatch, mode de sortie)."""
    bank = lambda: silver_stream(spark, "bank").withWatermark("event_time", WATERMARK)  # noqa: E731
    return {
        "bursts": (fraud.burst_windows(bank()), bursts_writer(), "update"),
        "aml": (silver_stream(spark, "bank"), aml_writer, "append"),
        "liquidity": (fraud.liquidity_windows(bank()), liquidity_writer, "update"),
        "unusual_country": (silver_stream(spark, "mobile_money"), unusual_country_writer(), "append"),
        "claims": (silver_stream(spark, "insurance"), claims_writer(), "append"),
    }


def main(argv: list[str] | None = None) -> int:
    names = ["bursts", "aml", "liquidity", "unusual_country", "claims"]
    parser = argparse.ArgumentParser(description="Streaming silver-* -> gold-* (fraude, AML, liquidité)")
    parser.add_argument("--queries", nargs="+", default=names, choices=names)
    parser.add_argument("--trigger", default="15 seconds", help="intervalle des micro-lots")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("stream_fraud_aml").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    spark.streams.addListener(StreamMonitor(spark, "fraud_aml", KAFKA))   # supervision (niveau 4.4)
    log.info("règles", extra={"ctx": {"rules": fraud.describe_rules(), "watermark": WATERMARK,
                                      "min_coverage": MIN_COVERAGE}})
    queries = build_queries(spark)
    for name in args.queries:
        df, writer, mode = queries[name]
        (df.writeStream.queryName(f"fraud_aml_{name}")
           .outputMode(mode)
           .foreachBatch(writer)
           .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{JOB}/{name}")
           .trigger(processingTime=args.trigger)
           .start())
        log.info("requête de streaming démarrée", extra={"ctx": {"query": name, "mode": mode}})
    spark.streams.awaitAnyTermination()     # une requête en échec -> code 1 -> Docker redémarre
    return 1


if __name__ == "__main__":
    sys.exit(main())
