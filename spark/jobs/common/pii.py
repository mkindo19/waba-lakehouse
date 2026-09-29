"""Protection des données personnelles (IBAN, numéros de compte).

Deux techniques complémentaires, appliquées AVANT toute écriture dans le lakehouse :
- pseudonymisation : hachage SHA-256 salé (sel secret lu dans l'environnement).
  Même valeur -> même hash : les jointures restent possibles, la valeur
  d'origine n'est pas récupérable sans le sel ;
- masquage : on ne garde que le début et la fin (lisible pour un humain,
  irréversible), ex. CI93************************1234.

La valeur en clair n'existe que dans raw-landing (zone d'atterrissage à accès restreint).
"""

from __future__ import annotations

import os

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _salt() -> str:
    salt = os.environ.get("PII_HASH_SALT")
    if not salt:
        raise RuntimeError("PII_HASH_SALT absent : refus d'écrire des données personnelles non protégées")
    return salt


def pseudonymize(col: Column, salt: str) -> Column:
    return F.when(col.isNull(), F.lit(None)).otherwise(F.sha2(F.concat(F.lit(salt), col), 256))


def mask(col: Column, keep_start: int = 4, keep_end: int = 4) -> Column:
    length = F.length(col)
    return F.when(col.isNull(), F.lit(None)).otherwise(
        F.concat(
            F.substring(col, 1, keep_start),
            F.array_join(F.array_repeat(F.lit("*"), F.greatest(length - keep_start - keep_end, F.lit(0))), ""),
            F.substring(col, -keep_end, keep_end),
        )
    )


def protect(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Remplace chaque colonne sensible par <col>_hash + <col>_masked."""
    if not columns:
        return df
    salt = _salt()
    for c in columns:
        df = (
            df.withColumn(f"{c}_hash", pseudonymize(F.col(c), salt))
              .withColumn(f"{c}_masked", mask(F.col(c)))
              .drop(c)
        )
    return df
