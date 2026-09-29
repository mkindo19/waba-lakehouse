"""Niveau 1.3 — Ingestion raw-landing (CSV) -> tables Iceberg lakehouse.raw.*

Étapes pour chaque jeu de données :
  1. lister les CSV en attente dans raw-landing (optionnellement pour un pays) ;
  2. lire avec un schéma EXPLICITE (mode PERMISSIVE + colonne _corrupt_record) ;
  3. valider : format, clé, pays, entité, devise, montants, énumérations, clés étrangères ;
  4. écrire les rejets dans raw.rejected_records (avec le motif) ;
  5. pseudonymiser les colonnes sensibles (IBAN, numéro de compte) ;
  6. MERGE dans la table Iceberg sur la clé technique -> idempotence ;
  7. archiver les fichiers traités (copie vérifiée puis suppression).

Usage :
    spark-submit ingest_raw.py --dataset all
    spark-submit ingest_raw.py --dataset bank_transactions --country CI
    spark-submit ingest_raw.py --dataset bank_transactions --no-archive
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common.datasets import CATALOG, DATASETS, INGESTION_ORDER, Dataset
from common.landing import archive_files, list_landing_files, to_s3a
from common.logging_utils import get_logger
from common.pii import protect
from common.quality import CORRUPT_COL, REASON_COL, with_reject_reason
from common.session import get_spark

log = get_logger("ingest_raw")

REJECTS_TABLE = f"{CATALOG}.raw.rejected_records"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"


def _now_utc() -> datetime:
    """Horodatage d'exécution figé côté driver (littéral déterministe pour MERGE)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def materialize(df: DataFrame) -> DataFrame:
    """Fige le DataFrame (checkpoint fiable sur MinIO) avant un MERGE Iceberg.

    Iceberg refuse une source MERGE contenant des expressions non déterministes
    (input_file_name(), fenêtres...). Le checkpoint écrit les données calculées
    dans le répertoire de checkpoint (S3) et coupe le lignage : la source du
    MERGE devient stable, même si un executor est perdu en cours de route
    (contrairement à localCheckpoint, stocké en mémoire des executors).
    """
    return df.checkpoint(eager=True)


# ----------------------------------------------------------------------------- DDL
def _ddl_type(dtype) -> str:
    return dtype.simpleString().upper()


def ensure_tables(spark: SparkSession, ds: Dataset, output_schema: StructType) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.raw")
    cols = ",\n  ".join(f"`{f.name}` {_ddl_type(f.dataType)}" for f in output_schema.fields)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ds.table} (
          {cols}
        ) USING iceberg
        {ds.partition_sql}
        TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true')
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REJECTS_TABLE} (
          dataset STRING, source_file STRING, reject_reason STRING,
          record_hash STRING, raw_record STRING, rejected_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (dataset, days(rejected_at))
        TBLPROPERTIES ('format-version'='2')
    """)


# ----------------------------------------------------------------------------- lecture
def read_landing(spark: SparkSession, ds: Dataset, paths: list[str]) -> DataFrame:
    schema = StructType(ds.schema.fields + [StructField(CORRUPT_COL, StringType(), True)])
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .option("timestampFormat", TS_FORMAT)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(paths)
        .withColumn("_source_file", F.input_file_name())
    )
    # Spark interdit de filtrer sur _corrupt_record sans matérialisation préalable
    return df.cache()


# ----------------------------------------------------------------------------- écriture
def write_rejects(spark: SparkSession, ds: Dataset, rejected: DataFrame) -> int:
    payload_cols = [c for c in rejected.columns if c not in (REASON_COL, "_source_file")]
    out = (
        rejected
        .withColumn("raw_record", F.coalesce(F.col(CORRUPT_COL), F.to_json(F.struct(*[c for c in payload_cols if c != CORRUPT_COL]))))
        .select(
            F.lit(ds.name).alias("dataset"),
            F.col("_source_file").alias("source_file"),
            F.col(REASON_COL).alias("reject_reason"),
            F.sha2(F.concat_ws("|", F.col("_source_file"), F.col("raw_record")), 256).alias("record_hash"),
            F.col("raw_record"),
            F.lit(_now_utc()).cast("timestamp").alias("rejected_at"),
        )
        .dropDuplicates(["record_hash"])
    )
    out = materialize(out)
    n = out.count()
    if n:
        out.createOrReplaceTempView("staged_rejects")
        spark.sql(f"""
            MERGE INTO {REJECTS_TABLE} t
            USING staged_rejects s
            ON t.dataset = s.dataset AND t.record_hash = s.record_hash
            WHEN NOT MATCHED THEN INSERT *
        """)
    return n


def merge_valid(spark: SparkSession, ds: Dataset, valid: DataFrame) -> int:
    """MERGE sur la clé : ré-ingérer le même fichier ne crée aucun doublon."""
    # Dédoublonnage intra-lot (même clé présente dans plusieurs fichiers)
    w = Window.partitionBy(ds.key).orderBy(F.col("_source_file").desc())
    staged = (
        valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", CORRUPT_COL, REASON_COL)
        .withColumn("_ingested_at", F.lit(_now_utc()).cast("timestamp"))
    )
    staged = materialize(protect(staged, ds.pii_columns))
    ensure_tables(spark, ds, staged.schema)
    staged.createOrReplaceTempView("staged")

    if ds.is_referential:
        # Référentiel : mise à jour si la ligne existe déjà (SCD type 1)
        action = "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    else:
        # Événement financier : immuable, insertion seule
        action = "WHEN NOT MATCHED THEN INSERT *"
    spark.sql(f"""
        MERGE INTO {ds.table} t
        USING staged s
        ON t.{ds.key} = s.{ds.key} AND t.country_code = s.country_code
        {action}
    """)
    return staged.count()


# ----------------------------------------------------------------------------- orchestration
def ingest_dataset(spark: SparkSession, ds: Dataset, country: str | None, archive: bool) -> dict:
    started = time.monotonic()
    prefix = f"{ds.folder}/" + (f"{country}/" if country and not ds.is_referential else "")
    keys = list_landing_files(prefix)
    ctx = {"dataset": ds.name, "country": country or "ALL", "files": len(keys)}
    if not keys:
        log.info("aucun fichier en attente", extra={"ctx": ctx})
        return ctx | {"valid": 0, "rejected": 0}

    raw = read_landing(spark, ds, [to_s3a(k) for k in keys])
    checked = with_reject_reason(spark, raw, ds).cache()
    rejected = checked.filter(F.col(REASON_COL).isNotNull())
    valid = checked.filter(F.col(REASON_COL).isNull())

    n_rejected = write_rejects(spark, ds, rejected)
    n_valid = merge_valid(spark, ds, valid)
    if n_rejected:
        top = rejected.groupBy(REASON_COL).count().orderBy(F.desc("count")).limit(5).collect()
        ctx["reject_reasons"] = {r[REASON_COL]: r["count"] for r in top}

    checked.unpersist()
    raw.unpersist()
    if archive:
        archive_files(keys)

    result = ctx | {"valid": n_valid, "rejected": n_rejected, "duration_s": round(time.monotonic() - started, 1)}
    log.info("ingestion terminée", extra={"ctx": result})
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingestion raw-landing -> Iceberg raw.*")
    parser.add_argument("--dataset", default="all", choices=["all", *DATASETS])
    parser.add_argument("--country", default=None, help="Code ISO (ex: CI). Défaut : tous")
    parser.add_argument("--no-archive", dest="archive", action="store_false",
                        help="Ne pas déplacer les fichiers traités vers le bucket archive")
    args = parser.parse_args(argv)

    names = INGESTION_ORDER if args.dataset == "all" else [args.dataset]
    spark = get_spark(f"ingest_raw_{args.dataset}")
    failures = 0
    for name in names:
        try:
            ingest_dataset(spark, DATASETS[name], args.country, args.archive)
        except Exception:
            failures += 1
            log.exception("échec d'ingestion", extra={"ctx": {"dataset": name}})
            if DATASETS[name].is_referential:
                break  # inutile d'ingérer les transactions sans référentiels
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
