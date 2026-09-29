"""Création de la SparkSession (la config Iceberg/S3 est dans conf/spark-defaults.conf)."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
