"""Écritures partagées par les jobs Spark Streaming (niveau 3) : Kafka, Iceberg, registre des micro-lots.

- write_kafka   : publication d'un DataFrame (colonnes key, value) dans un topic, acks=all.
- merge_iceberg : MERGE idempotent dans une table Iceberg, un commit à la fois (verrou) et nouvelles
                  tentatives si le catalogue SQLite est occupé (cf. D22).
- SinkLedger    : mémorise le dernier micro-lot entièrement écrit (évite de republier après une panne).
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterable
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .logging_utils import get_logger
from .tables import ensure_table

log = get_logger("stream_sinks")

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINT_ROOT = os.environ.get("STREAM_CHECKPOINT_DIR", "file:///opt/spark-checkpoints")

# Les requêtes d'une application appellent foreachBatch dans des threads parallèles du driver. Le
# catalogue Iceberg (REST + SQLite, D6) n'accepte qu'une écriture à la fois : on sérialise les commits.
_CATALOG_LOCK = threading.Lock()
# ValidationException : une autre requête de l'application a commité dans la même table entre la lecture
# du snapshot et le commit du MERGE (ex. gold.rt_fraud_alerts, alimentée par 3 requêtes) -> on relit et on rejoue.
_RETRYABLE = ("ServiceFailureException", "CommitFailedException", "ValidationException", "database is locked",
              "SQLITE_BUSY")


def write_kafka(df: DataFrame, topic: str) -> None:
    (df.write.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("kafka.acks", "all")
       .option("topic", topic)
       .save())


def merge_iceberg(spark: SparkSession, df: DataFrame, table: str, key: str,
                  partition_cols: Iterable[str] = ("country_code", "txn_date"), upsert: bool = False,
                  comment: str = "", attempts: int = 6) -> None:
    """MERGE sur `key` : insère les lignes absentes (et remplace les présentes si upsert).

    Rejouer le même MERGE ne crée aucun doublon. Un commit à la fois (verrou), table relue juste avant
    (REFRESH : le cache Spark peut garder un snapshot périmé), nouvelles tentatives avec attente
    croissante (2, 4, 8... s) si un autre écrivain occupe le catalogue ou a commité entre-temps.
    """
    partition_cols = list(partition_cols)
    view = f"_src_{table.replace('.', '_')}"
    action = "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *" if upsert \
        else "WHEN NOT MATCHED THEN INSERT *"
    for attempt in range(1, attempts + 1):
        try:
            with _CATALOG_LOCK:
                df.createOrReplaceTempView(view)       # sous le verrou : vue propre à ce MERGE
                ensure_table(spark, table, df.schema, partition_cols, comment=comment)
                spark.sql(f"REFRESH TABLE {table}")      # dernier snapshot, pas la version en cache
                spark.sql(f"MERGE INTO {table} t USING {view} s ON t.`{key}` = s.`{key}` {action}")
            return
        except Exception as exc:
            if attempt == attempts or not any(m in str(exc) for m in _RETRYABLE):
                raise
            log.warning("catalogue Iceberg occupé, nouvelle tentative",
                        extra={"ctx": {"table": table, "attempt": attempt}})
            time.sleep(2 ** attempt)


class SinkLedger:
    """Dernier micro-lot entièrement écrit (Iceberg + Kafka), rangé à côté du checkpoint.

    Après une panne, Spark rejoue le micro-lot inachevé avec le MÊME batch_id : s'il avait déjà
    été entièrement écrit, on ne le republie pas dans Kafka.
    """

    def __init__(self, job: str, query: str):
        root = CHECKPOINT_ROOT.removeprefix("file://")
        self.path = Path(root) / job / query / "_sinks_last_batch"

    def last(self) -> int:
        try:
            return int(self.path.read_text().strip())
        except (FileNotFoundError, ValueError):
            return -1

    def done(self, batch_id: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(str(batch_id))
        tmp.replace(self.path)                      # remplacement atomique
