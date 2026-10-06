"""Fonctions pures du job Spark Streaming 1 (raw-* Kafka -> silver-* Kafka + Iceberg silver.rt_*).

Séparées du job pour être testées en local sur des DataFrames statiques (spark/tests) :
le même code tourne ensuite en streaming, Spark ne fait pas la différence.

Chaque message raw-* est une ligne CSV convertie en JSON par NiFi (toutes les valeurs en texte,
+ ingestion_timestamp et source_file). Le job :
  1. décode le JSON et type chaque colonne selon le schéma du lot batch (common/datasets.py) ;
  2. applique les MÊMES règles de qualité qu'en batch (common/quality.py) -> motif de rejet ;
  3. rejets -> topic dlq-financial-events ; lignes valides -> silver (EUR, date, indicateurs).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .datasets import DATASETS, Dataset
from .fx import DEFAULT_GHS_PER_EUR, to_eur
from .quality import CORRUPT_COL, REASON_COL, _rules

DLQ_TOPIC = "dlq-financial-events"
NIFI_FIELDS = ("ingestion_timestamp", "source_file")


@dataclass(frozen=True)
class StreamSpec:
    dataset: str
    raw_topic: str
    silver_topic: str | None          # l'énoncé ne prévoit pas de topic silver pour les crédits
    rt_table: str                     # table Iceberg temps réel (séparée de la table batch, cf. D21)


STREAMS = {
    "bank_transactions": StreamSpec("bank_transactions", "raw-bank-transactions",
                                    "silver-bank-transactions", "silver.rt_bank_transactions"),
    "insurance_operations": StreamSpec("insurance_operations", "raw-insurance-operations",
                                       "silver-insurance-operations", "silver.rt_insurance_operations"),
    "mobile_money_payments": StreamSpec("mobile_money_payments", "raw-mobile-money-payments",
                                        "silver-mobile-money", "silver.rt_mobile_money_payments"),
    "loan_repayments": StreamSpec("loan_repayments", "raw-loan-repayments",
                                  None, "silver.rt_loan_repayments"),
}

# Seuils déclaratifs AML (mêmes valeurs que la couche silver batch)
AML_THRESHOLD_LOCAL = {"XOF": 1_000_000.0, "GHS": 5_000.0}


def _blank_to_null(col: Column) -> Column:
    return F.when(F.trim(col) == "", F.lit(None)).otherwise(col)


def text_schema(ds: Dataset) -> StructType:
    """Schéma de lecture du JSON NiFi : toutes les colonnes en texte (+ 2 champs ajoutés par NiFi)."""
    fields = [StructField(f.name, StringType(), True) for f in ds.schema.fields]
    return StructType(fields + [StructField(n, StringType(), True) for n in NIFI_FIELDS])


def parse_raw(kafka_df: DataFrame, ds: Dataset) -> DataFrame:
    """value (JSON texte) -> colonnes typées + _corrupt_record si le message est illisible.

    Entrée : colonnes `value` (string) et `_kafka_ts` (timestamp) au minimum.
    Un message est « corrompu » si le JSON est invalide, ou si une valeur présente ne se convertit
    pas dans son type (montant « 12a », date impossible...) : même logique que le mode PERMISSIVE.
    """
    j = F.from_json(F.col("value"), text_schema(ds))
    df = kafka_df.withColumn("_j", j)
    cast_failed = F.lit(False)
    for f in ds.schema.fields:
        raw = _blank_to_null(F.col(f"_j.{f.name}"))
        typed = raw.cast(f.dataType)
        df = df.withColumn(f.name, typed)
        cast_failed = cast_failed | (raw.isNotNull() & typed.isNull())
    # illisible : JSON invalide, ou aucun champ reconnaissable (ni identifiant ni pays)
    unreadable = F.col("_j").isNull() | (F.col(f"_j.{ds.key}").isNull() & F.col("_j.country_code").isNull())
    return (df
            .withColumn("ingestion_timestamp", F.to_timestamp(F.col("_j.ingestion_timestamp")))
            .withColumn("source_file", F.col("_j.source_file"))
            .withColumn(CORRUPT_COL, F.when(unreadable | cast_failed, F.col("value")))
            # clé de dédoublonnage : l'identifiant métier, ou l'empreinte du message s'il est illisible
            .withColumn("_dedup_key", F.coalesce(F.col(ds.key), F.sha2(F.col("value"), 256)))
            .drop("_j"))


def with_reason(df: DataFrame, ds: Dataset) -> DataFrame:
    """Ajoute _reject_reason avec les règles du batch (hors clés étrangères, cf. D21)."""
    reason = None
    for label, cond in _rules(ds, df.columns):
        reason = F.when(cond, F.lit(label)) if reason is None else reason.when(cond, F.lit(label))
    return df.withColumn(REASON_COL, reason)


def dlq_messages(rejected: DataFrame, ds: Dataset, spec: StreamSpec, detected_at: str) -> DataFrame:
    """Messages pour dlq-financial-events : motif + message d'origine intact (rien n'est perdu)."""
    payload = F.struct(
        F.lit(spec.raw_topic).alias("source_topic"),
        F.lit(ds.name).alias("dataset"),
        F.col(REASON_COL).alias("reject_reason"),
        F.col("value").alias("raw_message"),
        F.col("source_file"),
        F.lit(detected_at).alias("detected_at"),
    )
    return rejected.select(F.col("country_code").cast("string").alias("key"), F.to_json(payload).alias("value"))


def to_silver(valid: DataFrame, ds: Dataset, processed_at: str,
              ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> DataFrame:
    """Normalisation silver temps réel : montants en EUR, date, indicateurs (sans jointure).

    `processed_at` est une valeur fixe calculée par le driver (pas current_timestamp()) :
    Iceberg refuse une source MERGE non déterministe (cf. D10).
    """
    tech = {"value", "_kafka_ts", "_dedup_key", CORRUPT_COL, REASON_COL, "topic", "partition", "offset"}
    df = valid.drop(*[c for c in valid.columns if c in tech])
    for col in ds.amount_cols:
        df = df.withColumn(f"{col}_eur", to_eur(F.col(col), F.col("currency"), ghs_per_eur))
    df = (df.withColumn("event_time", F.col(ds.event_ts))
            .withColumn("txn_date", F.to_date(ds.event_ts))
            .withColumn("processing_time", F.lit(processed_at).cast("timestamp")))
    if ds.name == "bank_transactions":
        threshold = F.when(F.col("currency") == "GHS", F.lit(AML_THRESHOLD_LOCAL["GHS"])) \
                     .otherwise(F.lit(AML_THRESHOLD_LOCAL["XOF"]))
        df = (df.withColumn("streaming_amount_eur", F.col("amount_eur"))     # nom attendu par la requête Lambda
                .withColumn("is_aml_candidate",
                            F.col("transaction_type").isin("TRANSFER", "INTERNATIONAL_WIRE")
                            & (F.col("amount") > threshold)))
    return df


def kafka_messages(silver: DataFrame) -> DataFrame:
    """1 ligne silver -> 1 message JSON, clé = country_code (même partition par pays)."""
    cols = [c for c in silver.columns]
    return silver.select(F.col("country_code").alias("key"), F.to_json(F.struct(*cols)).alias("value"))


def spec_for(dataset: str) -> tuple[Dataset, StreamSpec]:
    return DATASETS[dataset], STREAMS[dataset]
