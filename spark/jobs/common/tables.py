"""Écriture dans les tables Iceberg : création, écrasement de partitions, MERGE.

Deux stratégies d'écriture idempotentes sont utilisées dans le projet :
- MERGE (insertion seule ou upsert) : quand on reçoit des lignes nouvelles à
  ajouter à une table existante (raw, bronze) ;
- overwritePartitions : quand on RECALCULE entièrement un périmètre (un pays,
  un mois) à partir de la couche précédente (silver, gold). Rejouer le calcul
  remplace les partitions concernées : jamais de doublon, par construction.
"""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .datasets import CATALOG, COUNTRIES


def fq(layer: str, name: str) -> str:
    """Nom complet d'une table : lakehouse.<couche>.<nom>."""
    return f"{CATALOG}.{layer}.{name}"


def parse_countries(value: str | None) -> list[str]:
    """'ALL' / None -> tous les pays ; 'CI' ou 'CI,SN' -> sous-ensemble validé."""
    if not value or value.upper() == "ALL":
        return list(COUNTRIES)
    countries = [c.strip().upper() for c in value.split(",") if c.strip()]
    unknown = sorted(set(countries) - set(COUNTRIES))
    if unknown:
        raise ValueError(f"Codes pays inconnus : {unknown}")
    return countries


def filter_countries(df: DataFrame, countries: Iterable[str], column: str = "country_code") -> DataFrame:
    countries = list(countries)
    return df if set(countries) == set(COUNTRIES) else df.filter(F.col(column).isin(countries))


def _ddl(schema: StructType) -> str:
    return ",\n  ".join(f"`{f.name}` {f.dataType.simpleString().upper()}" for f in schema.fields)


def ensure_table(spark: SparkSession, table: str, schema: StructType, partition_cols: Iterable[str],
                 comment: str = "") -> None:
    """CREATE TABLE IF NOT EXISTS à partir du schéma d'un DataFrame."""
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    parts = ", ".join(f"`{c}`" for c in partition_cols)
    partition_sql = f"PARTITIONED BY ({parts})" if parts else ""
    comment_sql = f"COMMENT '{comment}'" if comment else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
          {_ddl(schema)}
        ) USING iceberg
        {partition_sql}
        {comment_sql}
        TBLPROPERTIES ('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true')
    """)


def overwrite_partitions(spark: SparkSession, df: DataFrame, table: str, partition_cols: Iterable[str],
                         comment: str = "") -> int:
    """Remplace uniquement les partitions présentes dans `df` (idempotent).

    Les colonnes sont alignées sur la table par leur nom (writeTo = byName).
    """
    partition_cols = list(partition_cols)
    ensure_table(spark, table, df.schema, partition_cols, comment)
    df = df.cache()
    rows = df.count()
    if rows:  # un DataFrame vide n'écrase rien (évite de vider une table par erreur)
        df.writeTo(table).overwritePartitions()
    df.unpersist()
    return rows


def merge_insert(spark: SparkSession, df: DataFrame, table: str, keys: Iterable[str],
                 partition_cols: Iterable[str], upsert: bool = False) -> None:
    """MERGE sur les clés : insertion des lignes absentes (et mise à jour si upsert).

    La source est figée par un checkpoint (Iceberg refuse une source MERGE non
    déterministe) : cf. docs/DECISIONS.md.
    """
    keys = list(keys)
    ensure_table(spark, table, df.schema, partition_cols)
    staged = df.checkpoint(eager=True)
    view = f"_merge_src_{table.replace('.', '_')}"
    staged.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    action = "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *" if upsert \
        else "WHEN NOT MATCHED THEN INSERT *"
    spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} {action}")


def table_exists(spark: SparkSession, table: str) -> bool:
    return spark.catalog.tableExists(table)
