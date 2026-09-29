"""Règles de validation : chaque ligne reçoit un motif de rejet (ou NULL si valide).

Les règles sont évaluées dans l'ordre ; seul le premier motif est conservé.
Les lignes rejetées ne sont jamais perdues : elles vont dans raw.rejected_records.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .datasets import COUNTRIES, CURRENCY_BY_COUNTRY, DATASETS, ENTITY_TYPES, Dataset

CORRUPT_COL = "_corrupt_record"
REASON_COL = "_reject_reason"


def _expected_currency(country: Column) -> Column:
    expr = None
    for c, ccy in CURRENCY_BY_COUNTRY.items():
        expr = F.when(country == c, F.lit(ccy)) if expr is None else expr.when(country == c, F.lit(ccy))
    return expr


def _rules(ds: Dataset, columns: list[str]) -> list[tuple[str, Column]]:
    """(motif, condition d'invalidité) dans l'ordre de priorité."""
    rules: list[tuple[str, Column]] = [
        ("MALFORMED_RECORD", F.col(CORRUPT_COL).isNotNull()),
        ("MISSING_KEY", F.col(ds.key).isNull() | (F.trim(F.col(ds.key)) == "")),
        ("INVALID_COUNTRY", ~F.coalesce(F.col("country_code").isin(COUNTRIES), F.lit(False))),
        ("INVALID_ENTITY_TYPE", ~F.coalesce(F.col("entity_type").isin(ENTITY_TYPES), F.lit(False))),
    ]
    for col in columns:
        if col.endswith("_country"):
            rules.append((f"INVALID_{col.upper()}", ~F.coalesce(F.col(col).isin(COUNTRIES), F.lit(False))))
    if ds.event_ts:
        rules.append(("INVALID_TIMESTAMP", F.col(ds.event_ts).isNull()))
    if "currency" in columns:
        # Contrôle devise : XOF en zone UEMOA, GHS au Ghana
        rules.append(("CURRENCY_MISMATCH",
                      ~F.coalesce(F.col("currency") == _expected_currency(F.col("country_code")), F.lit(False))))
    for col in ds.amount_cols:
        rules.append(("INVALID_AMOUNT", F.col(col).isNull() | (F.col(col) < 0)))
    for col, allowed in ds.enum_checks.items():
        rules.append((f"INVALID_{col.upper()}", ~F.coalesce(F.col(col).isin(list(allowed)), F.lit(False))))
    return rules


def with_reject_reason(spark: SparkSession, df: DataFrame, ds: Dataset) -> DataFrame:
    """Ajoute la colonne _reject_reason (NULL = ligne valide)."""
    reason: Column | None = None
    for label, cond in _rules(ds, df.columns):
        reason = F.when(cond, F.lit(label)) if reason is None else reason.when(cond, F.lit(label))

    # Intégrité référentielle : toute clé étrangère doit exister dans le référentiel raw.*
    for i, fk in enumerate(ds.foreign_keys):
        ref = DATASETS[fk.ref_dataset]
        if not spark.catalog.tableExists(ref.table):
            raise RuntimeError(f"{ref.table} absente : ingérez les référentiels avant {ds.name}")
        flag = f"__fk_ok_{i}"
        ref_keys = spark.table(ref.table).select(F.col(fk.ref_column).alias(f"__fk_{i}")).distinct()
        if ref.name in ("branches", "products"):
            ref_keys = F.broadcast(ref_keys)
        df = (df.join(ref_keys, df[fk.column] == ref_keys[f"__fk_{i}"], "left")
                .withColumn(flag, F.col(f"__fk_{i}").isNotNull())
                .drop(f"__fk_{i}"))
        cond = F.col(fk.column).isNotNull() & ~F.col(flag)
        reason = reason.when(cond, F.lit(f"ORPHAN_{fk.column.upper()}"))

    df = df.withColumn(REASON_COL, reason)
    return df.drop(*[c for c in df.columns if c.startswith("__fk_ok_")])
