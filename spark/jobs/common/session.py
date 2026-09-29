"""Création de la SparkSession (la config Iceberg/S3 est dans conf/spark-defaults.conf)."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

CHECKPOINT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "s3a://lakehouse/_spark_checkpoints")


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Répertoire des checkpoints fiables (cf. ingest_raw.materialize)
    spark.sparkContext.setCheckpointDir(f"{CHECKPOINT_DIR}/{app_name}")
    return spark
