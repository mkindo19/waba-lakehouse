"""Transformations Bronze -> Silver (fonctions PURES : DataFrame en entrée, DataFrame en sortie).

Aucune lecture/écriture ici : les jobs s'en chargent. Les fonctions sont donc
testables en local avec de petits DataFrames (spark/tests/).

Silver = données nettoyées, dédoublonnées, normalisées et enrichies :
- dédoublonnage sur la clé (dernière version ingérée) ;
- jointures avec les référentiels (client, compte, agence, produit) ;
- montants convertis en EUR (analyses cross-pays) ;
- valeurs nulles normalisées, cas aberrants SIGNALÉS (colonne is_outlier) mais conservés.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .fx import DEFAULT_GHS_PER_EUR, to_eur

AML_THRESHOLD_LOCAL = {"XOF": 1_000_000.0, "GHS": 5_000.0}  # seuils déclaratifs BCEAO / Ghana
NPL_DAYS_OVERDUE = 90                                       # norme prudentielle : > 90 jours = douteux
LIFE_LINES = ("VIE", "PREVOYANCE")


# ---------------------------------------------------------------------------- helpers
def dedup_latest(df: DataFrame, key: str, order_col: str = "_ingested_at") -> DataFrame:
    """Une ligne par clé : la plus récemment ingérée."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def with_period_cols(df: DataFrame, ts_col: str = "timestamp") -> DataFrame:
    return (df.withColumn("txn_date", F.to_date(ts_col))
              .withColumn("txn_month", F.trunc(F.col(ts_col), "month")))


def _aml_threshold(currency: Column) -> Column:
    expr = None
    for ccy, thr in AML_THRESHOLD_LOCAL.items():
        expr = F.when(currency == ccy, F.lit(thr)) if expr is None else expr.when(currency == ccy, F.lit(thr))
    return expr


def flag_outliers(df: DataFrame, amount_col: str, group_cols: list[str], factor: float = 3.0) -> DataFrame:
    """is_outlier = montant <= 0 ou > factor x p99 de son groupe (pays, type...)."""
    p99 = df.groupBy(*group_cols).agg(F.percentile_approx(amount_col, 0.99).alias("_p99"))
    return (df.join(F.broadcast(p99), on=group_cols, how="left")
              .withColumn("is_outlier", (F.col(amount_col) <= 0) | (F.col(amount_col) > F.col("_p99") * factor))
              .drop("_p99"))


def _drop_tech(df: DataFrame) -> DataFrame:
    return df.drop("_source_file", "ingestion_date")


# ---------------------------------------------------------------------------- référentiels
def silver_customers(customers: DataFrame) -> DataFrame:
    return (_drop_tech(dedup_latest(customers, "customer_id"))
            .withColumn("region", F.coalesce("region", F.lit("INCONNUE")))
            .withColumn("segment", F.coalesce("segment", F.lit("RETAIL")))
            .withColumn("is_active", F.coalesce("is_active", F.lit(False))))


def silver_products(products: DataFrame) -> DataFrame:
    return _drop_tech(dedup_latest(products, "product_id")).fillna({"interest_rate": 0.0, "monthly_fee": 0.0})


def silver_branches(branches: DataFrame) -> DataFrame:
    return _drop_tech(dedup_latest(branches, "branch_id"))


def silver_accounts(accounts: DataFrame, customers: DataFrame, products: DataFrame,
                    ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> DataFrame:
    acc = _drop_tech(dedup_latest(accounts, "account_id"))
    cust = customers.select("customer_id", "segment", "region", "kyc_level")
    prod = products.select("product_id", "product_name", "interest_rate")
    return (acc.join(cust, "customer_id", "left")
               .join(F.broadcast(prod), "product_id", "left")
               .withColumn("balance", F.coalesce("balance", F.lit(0.0)))
               .withColumn("credit_limit", F.coalesce("credit_limit", F.lit(0.0)))
               .withColumn("interest_rate", F.coalesce("interest_rate", F.lit(0.0)))
               .withColumn("balance_eur", to_eur(F.col("balance"), F.col("currency"), ghs_per_eur))
               .withColumn("credit_limit_eur", to_eur(F.col("credit_limit"), F.col("currency"), ghs_per_eur)))


# ---------------------------------------------------------------------------- flux
def silver_bank_transactions(txn: DataFrame, accounts: DataFrame, branches: DataFrame,
                             ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> DataFrame:
    acc = accounts.select("account_id", "customer_id", "account_type", "segment")
    benef = accounts.select(F.col("account_id").alias("beneficiary_account"),
                            F.col("country_code").alias("beneficiary_country"))
    br = branches.select("branch_id", F.col("city").alias("branch_city"), "branch_type")
    df = (with_period_cols(_drop_tech(dedup_latest(txn, "transaction_id")))
          .join(acc, "account_id", "left")
          .join(benef, "beneficiary_account", "left")
          .join(F.broadcast(br), "branch_id", "left")
          .withColumn("channel", F.coalesce("channel", F.lit("UNKNOWN")))
          .withColumn("fee_amount", F.coalesce("fee_amount", F.lit(0.0)))
          .withColumn("amount_eur", to_eur(F.col("amount"), F.col("currency"), ghs_per_eur))
          .withColumn("fee_eur", to_eur(F.col("fee_amount"), F.col("currency"), ghs_per_eur))
          .withColumn("is_success", F.col("transaction_status") == "SUCCESS")
          .withColumn("is_international", F.col("beneficiary_country") != F.col("country_code"))
          # Candidat déclaration AML : virement >= seuil BCEAO (1 000 000 XOF) / Ghana (5 000 GHS)
          .withColumn("is_aml_candidate",
                      F.col("transaction_type").isin("TRANSFER", "INTERNATIONAL_WIRE")
                      & (F.col("amount") >= _aml_threshold(F.col("currency")))))
    return flag_outliers(df, "amount_eur", ["country_code", "transaction_type"])


def silver_insurance_operations(ops: DataFrame, customers: DataFrame,
                                ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> DataFrame:
    cust = customers.select("customer_id", "segment")
    df = (with_period_cols(_drop_tech(dedup_latest(ops, "operation_id")))
          .join(cust, "customer_id", "left")
          .withColumn("amount_eur", to_eur(F.col("amount"), F.col("currency"), ghs_per_eur))
          .withColumn("line_group", F.when(F.col("product_line").isin(*LIFE_LINES), "VIE").otherwise("IARD"))
          .withColumn("is_premium", F.col("operation_type").isin("PREMIUM_PAYMENT", "POLICY_RENEWAL"))
          .withColumn("is_claim_paid", F.col("operation_type") == "CLAIM_PAYMENT"))
    return flag_outliers(df, "amount_eur", ["country_code", "operation_type"])


def silver_mobile_money_payments(mm: DataFrame, customers: DataFrame,
                                 ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> DataFrame:
    sender = customers.select(F.col("customer_id").alias("sender_id"),
                              F.col("country_code").alias("sender_home_country"),
                              F.col("segment").alias("sender_segment"))
    df = (with_period_cols(_drop_tech(dedup_latest(mm, "payment_id")))
          .join(sender, "sender_id", "left")
          .withColumn("fee_amount", F.coalesce("fee_amount", F.lit(0.0)))
          .withColumn("amount_eur", to_eur(F.col("amount"), F.col("currency"), ghs_per_eur))
          .withColumn("fee_eur", to_eur(F.col("fee_amount"), F.col("currency"), ghs_per_eur))
          .withColumn("is_success", F.col("status") == "SUCCESS")
          .withColumn("is_cross_border", F.col("receiver_country") != F.col("sender_country"))
          .withColumn("corridor", F.concat_ws("-", "sender_country", "receiver_country"))
          # Profil inhabituel : le client émet depuis un pays différent de son pays de rattachement
          .withColumn("is_unusual_country", F.col("sender_home_country") != F.col("sender_country"))
          .withColumn("payment_hour", F.hour("timestamp")))
    return flag_outliers(df, "amount_eur", ["country_code", "payment_type"])


def silver_loan_repayments(rep: DataFrame, accounts: DataFrame,
                           ghs_per_eur: float = DEFAULT_GHS_PER_EUR) -> DataFrame:
    acc = accounts.select(F.col("account_id").alias("loan_account_id"),
                          F.col("balance_eur").alias("outstanding_eur"),
                          F.col("interest_rate"), F.col("segment"))
    return (with_period_cols(_drop_tech(dedup_latest(rep, "repayment_id")))
            .join(acc, "loan_account_id", "left")
            .withColumn("amount_due_eur", to_eur(F.col("amount_due"), F.col("currency"), ghs_per_eur))
            .withColumn("amount_paid_eur", to_eur(F.col("amount_paid"), F.col("currency"), ghs_per_eur))
            .withColumn("days_overdue", F.coalesce("days_overdue", F.lit(0)))
            .withColumn("outstanding_eur", F.coalesce("outstanding_eur", F.lit(0.0)))
            # Créance douteuse : défaut déclaré ou retard > 90 jours
            .withColumn("is_npl", (F.col("repayment_status") == "DEFAULT")
                        | (F.col("days_overdue") > NPL_DAYS_OVERDUE)))
