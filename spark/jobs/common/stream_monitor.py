"""Observabilité des jobs Spark Structured Streaming (niveau 4.4).

StreamMonitor est un StreamingQueryListener (appelé par Spark à chaque micro-lot de chaque requête) qui :

1. écrit une ligne JSON « progression » par source Kafka (durée du micro-lot, lignes lues, retard en offsets
   par rapport au dernier message du topic) : Promtail l'envoie à Loki, Grafana la lit (| json | unwrap) ;
2. COMMITTE les offsets traités dans un groupe de consommateurs Kafka « waba-spark.<requête> ».
   Spark ne committe jamais ses offsets dans Kafka (il les garde dans ses checkpoints) : sans cela, le retard
   des consommateurs serait invisible pour kafka-exporter / Prometheus. Ce commit sert uniquement à la
   supervision : la reprise après panne continue de s'appuyer sur les checkpoints. Avantage décisif : si le
   job tombe, le groupe garde ses derniers offsets et le retard continue de grandir -> l'alerte se déclenche
   (une métrique exposée par le job lui-même disparaîtrait avec lui).

Le client Kafka utilisé est celui de la JVM du driver (kafka-clients, déjà dans l'image pour le connecteur
Spark-Kafka), appelé via py4j : aucune dépendance Python supplémentaire.
"""

from __future__ import annotations

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from .logging_utils import get_logger

log = get_logger("stream_monitor")

GROUP_PREFIX = os.environ.get("STREAM_MONITOR_GROUP_PREFIX", "waba-spark")
COMMIT_TIMEOUT_S = 10


def parse_kafka_offsets(offsets: str | None) -> dict[str, dict[int, int]]:
    """'{"topic":{"0":12,"1":7}}' (format des offsets Kafka de Spark) -> {"topic": {0: 12, 1: 7}}."""
    if not offsets:
        return {}
    try:
        data = json.loads(offsets)
    except (TypeError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {topic: {int(p): int(o) for p, o in parts.items() if int(o) >= 0}
            for topic, parts in data.items() if isinstance(parts, dict)}


def group_id(query_name: str | None) -> str:
    return f"{GROUP_PREFIX}.{query_name or 'sans-nom'}"


class StreamMonitor(StreamingQueryListener):
    """À enregistrer une fois par application : spark.streams.addListener(StreamMonitor(spark, "fraud_aml"))."""

    def __init__(self, spark: SparkSession, job: str, bootstrap: str):
        # « _session » et non « spark » : StreamingQueryListener définit déjà une propriété `spark` (Spark 3.5)
        self._session, self.job, self.bootstrap = spark, job, bootstrap
        self._admin = None

    # ------------------------------------------------------------------ client Kafka de la JVM
    def _kafka_admin(self):
        if self._admin is None:
            jvm = self._session.sparkContext._jvm
            props = jvm.java.util.Properties()
            props.put("bootstrap.servers", self.bootstrap)
            props.put("client.id", f"{GROUP_PREFIX}-monitor-{self.job}")
            self._admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
        return self._admin

    def commit(self, query_name: str, offsets: dict[str, dict[int, int]]) -> None:
        if not offsets:
            return
        jvm = self._session.sparkContext._jvm
        java_offsets = jvm.java.util.HashMap()
        for topic, parts in offsets.items():
            for partition, offset in parts.items():
                java_offsets.put(jvm.org.apache.kafka.common.TopicPartition(topic, partition),
                                 jvm.org.apache.kafka.clients.consumer.OffsetAndMetadata(offset))
        (self._kafka_admin().alterConsumerGroupOffsets(group_id(query_name), java_offsets)
         .all().get(COMMIT_TIMEOUT_S, jvm.java.util.concurrent.TimeUnit.SECONDS))

    # ------------------------------------------------------------------ événements Spark
    def onQueryStarted(self, event) -> None:
        log.info("requête démarrée", extra={"ctx": {"job": self.job, "query": event.name}})

    def onQueryProgress(self, event) -> None:
        p = event.progress
        for source in p.sources:
            metrics = source.metrics or {}
            behind = metrics.get("maxOffsetsBehindLatest")
            log.info("progression", extra={"ctx": {
                "job": self.job, "query": p.name, "batch_id": p.batchId,
                "input_rows": source.numInputRows, "batch_duration_ms": p.batchDuration,
                "offsets_behind": int(float(behind)) if behind is not None else None,
                "consumer_group": group_id(p.name),
            }})
            try:
                self.commit(p.name, parse_kafka_offsets(source.endOffset))
            except Exception as exc:  # la supervision ne doit jamais faire tomber le traitement
                log.warning("commit des offsets de supervision impossible",
                            extra={"ctx": {"job": self.job, "query": p.name, "error": str(exc)[:300]}})

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        log.warning("requête arrêtée", extra={"ctx": {"job": self.job, "query_id": str(event.id),
                                                      "exception": (event.exception or "")[:500]}})
