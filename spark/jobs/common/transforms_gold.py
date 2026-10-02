"""Transformations Silver -> Gold : les 7 KPIs (fonctions PURES, testables en local).

| Table Gold                     | Grain                                   | Source silver            |
|--------------------------------|-----------------------------------------|--------------------------|
| daily_transaction_volume       | jour x pays x entité x type             | bank_transactions        |
| npl_ratio_by_country           | mois x pays x type de prêt (+ ALL)      | loan_repayments          |
| customer_arpu_monthly          | mois x pays x segment                   | bank + mobile + loans    |
| loss_ratio_by_product          | mois x pays x ligne produit (+ ALL)     | insurance_operations     |
| claims_processing_time         | mois x pays x IARD/VIE                  | insurance_operations     |
| mobile_money_daily_flow        | jour x pays                             | mobile_money_payments    |
| cross_border_transfers         | semaine x corridor                      | mobile_money_payments    |
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BCEAO_NPL_THRESHOLD = 0.05      # seuil réglementaire BCEAO : NPL < 5 %
CIMA_LOSS_RATIO_ALERT = 0.70    # seuil de vigilance CIMA : sinistres / primes > 70 %
CLAIMS_SLA_DAYS = {"IARD": 30, "VIE": 45}   # SLA contractuel (jours ouvrés) — hypothèse documentée
UEMOA = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]


def _ratio(num: str, den: str):
    return F.when(F.col(den) > 0, F.round(F.col(num) / F.col(den), 4))


# ---------------------------------------------------------------------------- banque
def daily_transaction_volume(bank: DataFrame) -> DataFrame:
    """Volume et montant des transactions par jour, pays, entité et type."""
    return (bank.groupBy("txn_date", "country_code", "entity_type", "transaction_type", "currency")
            .agg(F.count("*").alias("txn_count"),
                 F.sum(F.col("is_success").cast("int")).alias("success_count"),
                 F.sum((F.col("transaction_status") == "FAILED").cast("int")).alias("failed_count"),
                 F.sum((F.col("transaction_status") == "REVERSED").cast("int")).alias("reversed_count"),
                 F.sum(F.when(F.col("is_success"), F.col("amount"))).alias("total_amount_local"),
                 F.round(F.sum(F.when(F.col("is_success"), F.col("amount_eur"))), 2).alias("total_amount_eur"),
                 F.round(F.avg(F.when(F.col("is_success"), F.col("amount_eur"))), 2).alias("avg_amount_eur"))
            .withColumn("failure_rate", _ratio("failed_count", "txn_count")))


def npl_ratio_by_country(loans: DataFrame) -> DataFrame:
    """NPL = encours des prêts en défaut / encours total, par mois, pays et type de prêt.

    Un prêt est "douteux" sur le mois s'il a au moins une échéance en défaut ou > 90 j de retard.
    L'encours est le solde restant dû du compte prêt (référentiel accounts, en EUR).
    """
    per_loan = (loans.groupBy("country_code", "txn_month", "loan_type", "loan_account_id")
                .agg(F.max(F.col("is_npl").cast("int")).alias("is_npl"),
                     F.max("outstanding_eur").alias("outstanding_eur")))

    def agg(df: DataFrame) -> DataFrame:
        return df.agg(F.count("*").alias("loans_count"),
                      F.sum("is_npl").alias("npl_loans_count"),
                      F.round(F.sum("outstanding_eur"), 2).alias("total_outstanding_eur"),
                      F.round(F.sum(F.col("outstanding_eur") * F.col("is_npl")), 2).alias("npl_outstanding_eur"))

    by_type = agg(per_loan.groupBy("country_code", "txn_month", "loan_type"))
    all_types = agg(per_loan.groupBy("country_code", "txn_month")).withColumn("loan_type", F.lit("ALL"))
    out = by_type.unionByName(all_types).withColumnRenamed("txn_month", "period_month")
    return (out.withColumn("npl_ratio", _ratio("npl_outstanding_eur", "total_outstanding_eur"))
               .withColumn("bceao_threshold", F.lit(BCEAO_NPL_THRESHOLD))
               .withColumn("is_above_threshold", F.col("npl_ratio") > BCEAO_NPL_THRESHOLD)
               .withColumn("risk_level", F.when(F.col("npl_ratio") < 0.03, "GREEN")
                           .when(F.col("npl_ratio") <= BCEAO_NPL_THRESHOLD, "ORANGE").otherwise("RED")))


def customer_arpu_monthly(bank: DataFrame, mobile: DataFrame, loans: DataFrame) -> DataFrame:
    """ARPC = (commissions + intérêts perçus) / nombre de clients distincts actifs, par mois/pays/segment.

    - commissions : frais des transactions bancaires et mobile money réussies ;
    - intérêts : encours x taux annuel / 12 pour chaque échéance honorée (hors défaut) ;
    - clients actifs : clients ayant au moins une de ces opérations dans le mois.
    """
    fees_bank = (bank.filter("is_success")
                 .select("customer_id", "country_code", "segment", "txn_month",
                         F.col("fee_eur").alias("commission_eur"), F.lit(0.0).alias("interest_eur")))
    fees_mm = (mobile.filter("is_success")
               .select(F.col("sender_id").alias("customer_id"), "country_code",
                       F.col("sender_segment").alias("segment"), "txn_month",
                       F.col("fee_eur").alias("commission_eur"), F.lit(0.0).alias("interest_eur")))
    interest = (loans.filter(F.col("repayment_status") != "DEFAULT")
                .select("customer_id", "country_code", "segment", "txn_month",
                        F.lit(0.0).alias("commission_eur"),
                        F.round(F.col("outstanding_eur") * F.col("interest_rate") / 12, 2).alias("interest_eur")))
    revenue = fees_bank.unionByName(fees_mm).unionByName(interest).fillna({"segment": "RETAIL"})
    return (revenue.groupBy("txn_month", "country_code", "segment")
            .agg(F.round(F.sum("commission_eur"), 2).alias("commission_eur"),
                 F.round(F.sum("interest_eur"), 2).alias("interest_eur"),
                 F.countDistinct("customer_id").alias("active_customers"))
            .withColumn("total_revenue_eur", F.round(F.col("commission_eur") + F.col("interest_eur"), 2))
            .withColumn("arpc_eur", _ratio("total_revenue_eur", "active_customers"))
            .withColumnRenamed("txn_month", "period_month"))


# ---------------------------------------------------------------------------- assurance
def loss_ratio_by_product(ins: DataFrame) -> DataFrame:
    """Loss ratio = sinistres payés / primes acquises, par mois, pays et ligne produit (+ ALL)."""
    def agg(df: DataFrame) -> DataFrame:
        return df.agg(
            F.round(F.sum(F.when(F.col("is_premium"), F.col("amount_eur")).otherwise(0)), 2).alias("premiums_eur"),
            F.round(F.sum(F.when(F.col("is_claim_paid"), F.col("amount_eur")).otherwise(0)), 2).alias("claims_paid_eur"),
            F.sum(F.col("is_claim_paid").cast("int")).alias("claims_paid_count"))

    by_line = agg(ins.groupBy("txn_month", "country_code", "product_line"))
    all_lines = agg(ins.groupBy("txn_month", "country_code")).withColumn("product_line", F.lit("ALL"))
    return (by_line.unionByName(all_lines)
            .withColumn("loss_ratio", _ratio("claims_paid_eur", "premiums_eur"))
            .withColumn("cima_alert_threshold", F.lit(CIMA_LOSS_RATIO_ALERT))
            .withColumn("is_above_threshold", F.col("loss_ratio") > CIMA_LOSS_RATIO_ALERT)
            .withColumnRenamed("txn_month", "period_month"))


def claims_processing_time(ins: DataFrame) -> DataFrame:
    """Délai moyen de traitement des sinistres (jours ouvrés) par mois, pays et IARD/VIE."""
    claims = ins.filter(F.col("operation_type").isin("CLAIM_SUBMISSION", "CLAIM_PAYMENT")
                        & F.col("processing_days").isNotNull())
    sla = F.when(F.col("line_group") == "VIE", CLAIMS_SLA_DAYS["VIE"]).otherwise(CLAIMS_SLA_DAYS["IARD"])
    return (claims.withColumn("sla_days", sla)
            .groupBy("txn_month", "country_code", "line_group", "sla_days")
            .agg(F.count("*").alias("claims_count"),
                 F.round(F.avg("processing_days"), 1).alias("avg_processing_days"),
                 F.percentile_approx("processing_days", 0.5).alias("p50_processing_days"),
                 F.percentile_approx("processing_days", 0.9).alias("p90_processing_days"),
                 F.round(F.avg((F.col("processing_days") <= F.col("sla_days")).cast("double")), 4)
                 .alias("within_sla_rate"))
            .withColumnRenamed("txn_month", "period_month"))


# ---------------------------------------------------------------------------- mobile money
def mobile_money_daily_flow(mm: DataFrame) -> DataFrame:
    """Flux journalier par pays : volume, montant, taux d'échec, utilisateurs actifs."""
    return (mm.groupBy("txn_date", "country_code", "currency")
            .agg(F.count("*").alias("txn_count"),
                 F.sum(F.col("is_success").cast("int")).alias("success_count"),
                 F.sum((F.col("status") == "FAILED").cast("int")).alias("failed_count"),
                 F.round(F.sum(F.when(F.col("is_success"), F.col("amount_eur"))), 2).alias("total_amount_eur"),
                 F.countDistinct("sender_id").alias("active_users"),
                 F.sum(F.col("is_unusual_country").cast("int")).alias("unusual_country_count"))
            .withColumn("failure_rate", _ratio("failed_count", "txn_count")))


def cross_border_transfers(mm: DataFrame) -> DataFrame:
    """Transferts transfrontaliers réussis par semaine et corridor, avec évolution hebdomadaire."""
    weekly = (mm.filter(F.col("is_cross_border") & F.col("is_success"))
              .withColumn("week_start", F.to_date(F.date_trunc("week", "timestamp")))
              .groupBy("week_start", "sender_country", "receiver_country", "corridor")
              .agg(F.count("*").alias("transfers_count"),
                   F.round(F.sum("amount_eur"), 2).alias("total_amount_eur"),
                   F.round(F.avg("amount_eur"), 2).alias("avg_amount_eur")))
    w = Window.partitionBy("corridor").orderBy("week_start")
    prev = F.lag("total_amount_eur").over(w)
    return (weekly.withColumn("prev_week_amount_eur", prev)
                  .withColumn("wow_change_pct",
                              F.when(prev > 0, F.round((F.col("total_amount_eur") - prev) / prev * 100, 2)))
                  .withColumn("is_uemoa_corridor",
                              F.col("sender_country").isin(UEMOA) & F.col("receiver_country").isin(UEMOA))
                  .withColumn("country_code", F.col("sender_country")))


# ---------------------------------------------------------------------------- réglementaire
def regulatory_daily_report(report_date: str, bank: DataFrame, npl: DataFrame, ins: DataFrame) -> DataFrame:
    """Agrégats J-1 par pays pour les déclarations BCEAO / CIMA.

    - activité bancaire du jour J-1 et nombre de déclarations AML (virements >= seuil) ;
    - dernier ratio NPL mensuel connu (tous types de prêts) ;
    - loss ratio cumulé du mois en cours jusqu'à J-1.
    """
    day = F.to_date(F.lit(report_date))
    month = F.trunc(day, "month")
    activity = (bank.filter(F.col("txn_date") == day).groupBy("country_code")
                .agg(F.count("*").alias("txn_count"),
                     F.round(F.sum(F.when(F.col("is_success"), F.col("amount_eur"))), 2).alias("txn_amount_eur"),
                     F.sum((F.col("is_aml_candidate") & F.col("is_success")).cast("int")).alias("aml_declarations")))
    w = Window.partitionBy("country_code").orderBy(F.col("period_month").desc())
    latest_npl = (npl.filter((F.col("loan_type") == "ALL") & (F.col("period_month") <= month))
                  .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
                  .select("country_code", F.col("period_month").alias("npl_period_month"), "npl_ratio"))
    mtd = (ins.filter((F.col("txn_date") >= month) & (F.col("txn_date") <= day)).groupBy("country_code")
           .agg(F.sum(F.when(F.col("is_premium"), F.col("amount_eur")).otherwise(0)).alias("_prem"),
                F.sum(F.when(F.col("is_claim_paid"), F.col("amount_eur")).otherwise(0)).alias("_claims"))
           .withColumn("loss_ratio_mtd", _ratio("_claims", "_prem")).drop("_prem", "_claims"))

    countries = activity.select("country_code").union(latest_npl.select("country_code")) \
                        .union(mtd.select("country_code")).distinct()
    return (countries.join(activity, "country_code", "left")
            .join(latest_npl, "country_code", "left")
            .join(mtd, "country_code", "left")
            .fillna({"txn_count": 0, "aml_declarations": 0, "txn_amount_eur": 0.0})
            .withColumn("report_date", day)
            .withColumn("banking_regulator", F.when(F.col("country_code") == "GH", "BOG").otherwise("BCEAO"))
            .withColumn("insurance_regulator", F.when(F.col("country_code") == "GH", "NIC").otherwise("CIMA"))
            .withColumn("npl_above_threshold", F.col("npl_ratio") > BCEAO_NPL_THRESHOLD)
            .withColumn("loss_ratio_above_threshold", F.col("loss_ratio_mtd") > CIMA_LOSS_RATIO_ALERT))
