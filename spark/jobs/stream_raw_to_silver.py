"""Niveau 3 — Job Spark Structured Streaming 1 : raw-* (Kafka) -> silver-* (Kafka) + silver.rt_* (Iceberg).

Une requête de streaming par flux (4 au total), dans la même application Spark :

    raw-bank-transactions ─┐                                 ┌─► dlq-financial-events   (rejets + motif)
    raw-insurance-ops ─────┤  JSON -> types -> règles        │
    raw-mobile-money ──────┼─ qualité -> dédoublonnage ──────┼─► silver-* (Kafka)       } double sink
    raw-loan-repayments ───┘  (10 min) -> EUR                └─► silver.rt_* (Iceberg)  }

- Dédoublonnage par identifiant sur 10 minutes (exigence 3.3) : watermark sur l'heure d'arrivée
  dans Kafka + dropDuplicatesWithinWatermark. L'heure métier n'est pas utilisée comme watermark :
  le générateur simule des journées passées, qui seraient toutes jugées « en retard » (cf. D21).
- Double sink via foreachBatch : chaque micro-lot est écrit dans Kafka ET dans Iceberg.
  Iceberg : MERGE sur l'identifiant -> rejouer un micro-lot après une panne ne crée pas de doublon.
- Checkpoints (offsets + état du dédoublonnage) sur un volume partagé driver / executors :
  un redémarrage reprend exactement là où le job s'était arrêté.

Usage (service Docker `stream-raw-to-silver`, profil streaming) :
    spark-submit stream_raw_to_silver.py [--datasets bank_transactions ...] [--trigger "15 seconds"]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.datasets import CATALOG
from common.fx import DEFAULT_GHS_PER_EUR
from common.logging_utils import get_logger
from common.quality import REASON_COL
from common.stream_monitor import StreamMonitor
from common.stream_sinks import CHECKPOINT_ROOT, KAFKA, SinkLedger, merge_iceberg, write_kafka
from common.streaming import (
    DLQ_TOPIC,
    STREAMS,
    dlq_messages,
    kafka_messages,
    parse_raw,
    spec_for,
    to_silver,
    with_reason,
)

log = get_logger("stream_raw_to_silver")

DEDUP_WINDOW = "10 minutes"
RT_COMMENT = "Silver temps réel (job streaming 1), remplacé par la couche batch"


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def make_batch_writer(dataset: str, ghs_per_eur: float):
    ds, spec = spec_for(dataset)
    table = f"{CATALOG}.{spec.rt_table}"

    ledger = SinkLedger("raw_to_silver", dataset)

    def process(batch: DataFrame, batch_id: int) -> None:
        if batch_id <= ledger.last():
            log.info("micro-lot déjà écrit avant la panne : ignoré",
                     extra={"ctx": {"dataset": dataset, "batch_id": batch_id}})
            return
        if batch.isEmpty():
            ledger.done(batch_id)
            return
        spark = batch.sparkSession
        batch = batch.persist()
        now = _now()
        rejected = batch.filter(F.col(REASON_COL).isNotNull())
        valid = batch.filter(F.col(REASON_COL).isNull())
        silver = to_silver(valid, ds, processed_at=now, ghs_per_eur=ghs_per_eur).persist()
        n_rejected, n_valid = rejected.count(), silver.count()

        # 1) Iceberg d'abord (idempotent), 2) puis Kafka, 3) puis le registre : si une étape échoue,
        # le micro-lot est rejoué en entier ; s'il avait abouti, le registre évite de republier.
        if n_valid:
            merge_iceberg(spark, silver, table, ds.key, comment=RT_COMMENT)
        if n_rejected:
            write_kafka(dlq_messages(rejected, ds, spec, now), DLQ_TOPIC)
        if n_valid and spec.silver_topic:
            write_kafka(kafka_messages(silver), spec.silver_topic)
        ledger.done(batch_id)

        # Latence par pays (niveau 4.4, Grafana via Loki) : du dépôt dans Kafka à la fin de l'écriture silver,
        # pour le message le plus ancien du micro-lot (pire cas) et le plus récent.
        done_ts = datetime.now(timezone.utc).timestamp()
        per_country = (batch.filter(F.col("country_code").isNotNull()).groupBy("country_code")
                       .agg(F.min(F.col("_kafka_ts").cast("double")).alias("oldest"),
                            F.max(F.col("_kafka_ts").cast("double")).alias("newest"),
                            F.count("*").alias("rows"))
                       .collect())
        for r in per_country:
            log.info("latence par pays", extra={"ctx": {
                "dataset": dataset, "batch_id": batch_id, "country_code": r["country_code"], "rows": r["rows"],
                "lag_s": round(done_ts - r["oldest"], 1), "lag_min_s": round(done_ts - r["newest"], 1),
            }})
        log.info("micro-lot traité", extra={"ctx": {
            "dataset": dataset, "batch_id": batch_id, "valid": n_valid, "rejected": n_rejected,
            "lag_s": max((round(done_ts - r["oldest"], 1) for r in per_country), default=None),
        }})
        silver.unpersist()
        batch.unpersist()

    return process


def start_query(spark: SparkSession, dataset: str, trigger: str, ghs_per_eur: float):
    ds, spec = spec_for(dataset)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", spec.raw_topic)
           .option("startingOffsets", "earliest")      # 1er démarrage : tout l'historique du topic
           .option("maxOffsetsPerTrigger", 20000)      # borne la taille d'un micro-lot
           .option("failOnDataLoss", "false")
           .load()
           .select(F.col("value").cast("string").alias("value"), F.col("timestamp").alias("_kafka_ts")))
    checked = (with_reason(parse_raw(raw, ds), ds)
               .withWatermark("_kafka_ts", DEDUP_WINDOW)
               .dropDuplicatesWithinWatermark(["_dedup_key"]))
    return (checked.writeStream
            .queryName(f"raw_to_silver_{dataset}")
            .foreachBatch(make_batch_writer(dataset, ghs_per_eur))
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw_to_silver/{dataset}")
            .trigger(processingTime=trigger)
            .start())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Streaming raw-* -> silver-* + Iceberg silver.rt_*")
    parser.add_argument("--datasets", nargs="+", default=list(STREAMS), choices=list(STREAMS))
    parser.add_argument("--trigger", default="15 seconds", help="intervalle des micro-lots")
    parser.add_argument("--ghs-per-eur", type=float, default=float(os.environ.get("WABA_GHS_PER_EUR",
                                                                                   DEFAULT_GHS_PER_EUR)))
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("stream_raw_to_silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    spark.streams.addListener(StreamMonitor(spark, "raw_to_silver", KAFKA))   # supervision (niveau 4.4)
    for dataset in args.datasets:
        start_query(spark, dataset, args.trigger, args.ghs_per_eur)
        log.info("requête de streaming démarrée", extra={"ctx": {"dataset": dataset, "trigger": args.trigger}})
    spark.streams.awaitAnyTermination()     # s'arrête (code 1) si une requête échoue -> Docker redémarre
    return 1


if __name__ == "__main__":
    sys.exit(main())
