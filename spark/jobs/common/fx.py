"""Conversion des montants en EUR (analyses cross-pays).

- XOF : parité FIXE avec l'euro garantie par le Trésor français (1 EUR = 655,957 XOF).
- GHS : devise flottante. Taux de référence SIMULÉ, cohérent avec l'hypothèse du
  générateur (1 GHS ≈ 45 XOF -> 1 EUR ≈ 14,6 GHS). Il est paramétrable (Airflow
  Variable `waba_ghs_per_eur`) ; en production il viendrait d'un flux de taux
  quotidien (ex. Bank of Ghana / BCE) stocké dans une table de référence.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

XOF_PER_EUR = 655.957
DEFAULT_GHS_PER_EUR = 14.6


def units_per_eur(ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> dict[str, float]:
    return {"XOF": XOF_PER_EUR, "GHS": ghs_per_eur, "EUR": 1.0}


def to_eur(amount: Column, currency: Column, ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> Column:
    """Montant en EUR arrondi au centime (NULL si devise inconnue)."""
    expr = None
    for ccy, rate in units_per_eur(ghs_per_eur).items():
        cond = currency == F.lit(ccy)
        expr = F.when(cond, amount / F.lit(rate)) if expr is None else expr.when(cond, amount / F.lit(rate))
    return F.round(expr, 2)


def fx_rates_df(spark: SparkSession, ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> DataFrame:
    """Table de référence des taux utilisés (traçabilité des conversions)."""
    rows = [(ccy, float(rate), "FIXED_PEG" if ccy in ("XOF", "EUR") else "SIMULATED_REFERENCE")
            for ccy, rate in units_per_eur(ghs_per_eur).items()]
    return (spark.createDataFrame(rows, "currency STRING, units_per_eur DOUBLE, rate_type STRING")
            .withColumn("eur_per_unit", F.round(1 / F.col("units_per_eur"), 8)))
