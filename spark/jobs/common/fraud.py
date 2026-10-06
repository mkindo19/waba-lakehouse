"""Fonctions pures du job Spark Streaming 2 (silver-* Kafka -> gold-* Kafka + Iceberg gold.rt_*).

Testables sur des DataFrames statiques (spark/tests/test_fraud.py) : le job ne fait qu'assembler.

Règles (énoncé 3.3, job 2) :
  R1 BURST_HIGH_VALUE   plusieurs transactions > 500 000 XOF d'un même compte en moins de 5 min
                        (fenêtres glissantes 5 min, pas de 1 min) ;
  R2 UNUSUAL_COUNTRY    paiement mobile money émis depuis un pays autre que celui du client ;
  R3 CLAIM_OVER_PREMIUM sinistre > 3 x la prime annuelle de la police (prime mensuelle x 12) ;
  AML                   virement > seuil déclaratif (1 000 000 XOF UEMOA, 5 000 GHS Ghana) ;
  LIQUIDITÉ             sorties nettes d'un pays sur 5 min rapportées à ses dépôts : alerte si le
                        taux de couverture passe sous le minimum.

Une même rafale apparaît dans plusieurs fenêtres glissantes (jusqu'à 5) : les fenêtres qui se
chevauchent sont fusionnées en UNE alerte (« îlots »), puis rattachées à une alerte déjà émise si
elles la recoupent. Identifiant d'alerte stable -> MERGE Iceberg et clé Kafka (cf. D23).
"""

from __future__ import annotations

import json

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .fx import XOF_PER_EUR

FRAUD_TOPIC = "gold-fraud-alerts"
AML_TOPIC = "gold-aml-events"
LIQUIDITY_TOPIC = "gold-liquidity-alerts"

WINDOW, SLIDE = "5 minutes", "1 minute"
BURST_THRESHOLD_XOF = 500_000.0
BURST_THRESHOLD_EUR = round(BURST_THRESHOLD_XOF / XOF_PER_EUR, 2)   # même seuil pour le Ghana (GHS)
BURST_MIN_TXN = 2                                                    # « transactions multiples »
CLAIM_PREMIUM_FACTOR = 3.0
CLAIM_OPERATIONS = ("CLAIM_SUBMISSION", "CLAIM_PAYMENT")
AML_THRESHOLD_LOCAL = {"XOF": 1_000_000.0, "GHS": 5_000.0}
OUTFLOW_TYPES = ("WITHDRAWAL", "TRANSFER", "INTERNATIONAL_WIRE", "PAYMENT")
INFLOW_TYPES = ("DEPOSIT",)
DEPOSIT_ACCOUNT_TYPES = ("CURRENT", "SAVINGS", "MOBILE_WALLET")
LIQUIDITY_MIN_COVERAGE = 0.98     # alerte si plus de 2 % des dépôts d'un pays sortent en 5 min

# Colonnes lues dans les messages silver-* (JSON publié par le job 1)
SILVER_SCHEMAS = {
    "bank": ("transaction_id STRING, account_id STRING, beneficiary_account STRING, country_code STRING, "
             "transaction_type STRING, amount DOUBLE, currency STRING, amount_eur DOUBLE, "
             "transaction_status STRING, channel STRING, event_time TIMESTAMP, txn_date DATE, "
             "is_aml_candidate BOOLEAN"),
    "mobile_money": ("payment_id STRING, sender_id STRING, receiver_id STRING, sender_country STRING, "
                     "receiver_country STRING, country_code STRING, amount DOUBLE, currency STRING, "
                     "amount_eur DOUBLE, payment_type STRING, status STRING, event_time TIMESTAMP"),
    "insurance": ("operation_id STRING, customer_id STRING, account_id STRING, country_code STRING, "
                  "operation_type STRING, product_line STRING, amount DOUBLE, currency STRING, "
                  "amount_eur DOUBLE, event_time TIMESTAMP"),
}

EVENT = "struct<event_id:string,event_time:timestamp,amount_eur:double>"
ALERT_COLUMNS = ["alert_id", "rule", "severity", "country_code", "entity_type", "entity_id",
                 "first_event_time", "last_event_time", "event_count", "amount_eur", "events", "details"]


# ---------------------------------------------------------------------------- lecture
def parse_silver(kafka_df: DataFrame, kind: str) -> DataFrame:
    """value (JSON silver) -> colonnes typées. Les messages illisibles sont ignorés (déjà filtrés au job 1)."""
    parsed = kafka_df.select(F.from_json(F.col("value").cast("string"), SILVER_SCHEMAS[kind]).alias("m"))
    return parsed.select("m.*").filter(F.col("event_time").isNotNull())


def _event(id_col: str) -> Column:
    return F.struct(F.col(id_col).alias("event_id"), F.col("event_time"),
                    F.col("amount_eur").cast("double").alias("amount_eur"))


def _alert_id(rule: str, *cols: Column) -> Column:
    return F.sha2(F.concat_ws("|", F.lit(rule), *[c.cast("string") for c in cols]), 256)


def _details(**cols: Column) -> Column:
    return F.to_json(F.struct(*[c.alias(k) for k, c in cols.items()]))


def _single_event_alert(df: DataFrame, rule: str, severity: str, entity_type: str, entity_col: str,
                        id_col: str, details: Column) -> DataFrame:
    """Alerte portant sur un seul événement (R2, R3) : identifiant = règle + identifiant de l'événement."""
    return df.select(
        _alert_id(rule, F.col(id_col)).alias("alert_id"), F.lit(rule).alias("rule"),
        F.lit(severity).alias("severity"), "country_code", F.lit(entity_type).alias("entity_type"),
        F.col(entity_col).alias("entity_id"), F.col("event_time").alias("first_event_time"),
        F.col("event_time").alias("last_event_time"), F.lit(1).alias("event_count"),
        F.round("amount_eur", 2).alias("amount_eur"), F.array(_event(id_col)).alias("events"),
        details.alias("details"))


# ---------------------------------------------------------------------------- R1 : rafales
def burst_windows(bank: DataFrame) -> DataFrame:
    """Agrégat sur fenêtres glissantes (5 min, pas 1 min) des transactions > 500 000 XOF par compte.

    En streaming : agrégation avec état (watermark posé par l'appelant), mode « update ».
    """
    big = bank.filter((F.col("transaction_status") == "SUCCESS") & (F.col("amount_eur") > BURST_THRESHOLD_EUR))
    return (big.groupBy(F.window("event_time", WINDOW, SLIDE).alias("w"), "account_id", "country_code")
               .agg(F.count("*").alias("txn_count"), F.collect_list(_event("transaction_id")).alias("events"))
               .filter(F.col("txn_count") >= BURST_MIN_TXN)
               .select("account_id", "country_code", "txn_count", "events",
                       F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end")))


def _islands(df: DataFrame, keys: list[str], start: str, end: str) -> DataFrame:
    """Numérote les groupes de lignes dont les intervalles [start, end] se chevauchent (par clé)."""
    w = Window.partitionBy(*keys).orderBy(start, end)
    prev_end = F.max(end).over(w.rowsBetween(Window.unboundedPreceding, -1))
    new = (prev_end.isNull() | (F.col(start) > prev_end)).cast("int")
    return df.withColumn("_island", F.sum(new).over(w.rowsBetween(Window.unboundedPreceding, 0)))


def _with_event_stats(df: DataFrame) -> DataFrame:
    return (df.withColumn("event_count", F.size("events"))
              .withColumn("amount_eur", F.round(F.aggregate("events", F.lit(0.0), lambda a, e: a + F.coalesce(e.amount_eur, F.lit(0.0))), 2))
              .withColumn("first_event_time", F.array_min(F.transform("events", lambda e: e.event_time)))
              .withColumn("last_event_time", F.array_max(F.transform("events", lambda e: e.event_time))))


def burst_alerts(windows: DataFrame, existing: DataFrame | None = None) -> DataFrame:
    """Fenêtres d'un micro-lot -> une alerte par rafale.

    1. îlots : les fenêtres d'un même compte qui partagent des transactions sont fusionnées ;
    2. rattachement : si une alerte déjà émise (table Iceberg) recoupe la rafale, on garde son
       identifiant et on fusionne les transactions (l'alerte est mise à jour, pas dupliquée).
    """
    ranged = (windows
              .withColumn("_first", F.array_min(F.transform("events", lambda e: e.event_time)))
              .withColumn("_last", F.array_max(F.transform("events", lambda e: e.event_time))))
    bursts = (_islands(ranged, ["account_id"], "_first", "_last")
              .groupBy("account_id", "country_code", "_island")
              .agg(F.array_distinct(F.flatten(F.collect_list("events"))).alias("events"))
              .transform(_with_event_stats))

    if existing is not None:
        prev = (existing.filter(F.col("rule") == "BURST_HIGH_VALUE")
                .select(F.col("alert_id").alias("_prev_id"), F.col("entity_id").alias("account_id"),
                        F.col("first_event_time").alias("_pf"), F.col("last_event_time").alias("_pl"),
                        F.col("events").alias("_prev_events")))
        b, p = bursts.alias("b"), prev.alias("p")
        cond = ((F.col("b.account_id") == F.col("p.account_id"))
                & (F.col("p._pf") <= F.col("b.last_event_time")) & (F.col("b.first_event_time") <= F.col("p._pl")))
        matched = b.join(p, cond, "left").select("b.*", "p._prev_id", "p._pf", "p._prev_events")
        # une rafale qui recoupe plusieurs alertes : on garde la plus ancienne
        w = Window.partitionBy("account_id", "_island").orderBy(F.col("_pf").asc_nulls_last())
        bursts = (matched.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
                  .withColumn("events", F.when(F.col("_prev_id").isNull(), F.col("events"))
                              .otherwise(F.array_distinct(F.concat("_prev_events", "events"))))
                  .transform(_with_event_stats))
    else:
        bursts = bursts.withColumn("_prev_id", F.lit(None).cast("string"))

    return bursts.select(
        F.coalesce("_prev_id", _alert_id("BURST_HIGH_VALUE", F.col("account_id"),
                                         F.col("first_event_time"))).alias("alert_id"),
        F.lit("BURST_HIGH_VALUE").alias("rule"), F.lit("HIGH").alias("severity"), "country_code",
        F.lit("ACCOUNT").alias("entity_type"), F.col("account_id").alias("entity_id"),
        "first_event_time", "last_event_time", "event_count", "amount_eur", "events",
        _details(threshold_xof=F.lit(BURST_THRESHOLD_XOF), threshold_eur=F.lit(BURST_THRESHOLD_EUR),
                 window=F.lit(f"{WINDOW} / pas {SLIDE}")).alias("details"))


# ---------------------------------------------------------------------------- R2 : pays inhabituel
def unusual_country_alerts(mm: DataFrame, customers: DataFrame) -> DataFrame:
    """Paiement émis depuis un pays différent du pays de rattachement du client (référentiel)."""
    home = customers.select(F.col("customer_id").alias("sender_id"), F.col("country_code").alias("home_country"))
    hits = (mm.join(home, "sender_id", "inner")
              .filter(F.col("sender_country") != F.col("home_country")))
    return _single_event_alert(
        hits, "UNUSUAL_COUNTRY", "MEDIUM", "CUSTOMER", "sender_id", "payment_id",
        _details(home_country=F.col("home_country"), sender_country=F.col("sender_country"),
                 receiver_country=F.col("receiver_country"), payment_type=F.col("payment_type"),
                 status=F.col("status")))


# ---------------------------------------------------------------------------- R3 : sinistre
def claim_alerts(ins: DataFrame, accounts: DataFrame) -> DataFrame:
    """Sinistre > 3 x prime annuelle. Prime annuelle = 12 x prime mensuelle (solde du compte police)."""
    policies = (accounts.filter(F.col("account_type") == "INSURANCE_POLICY")
                .select("account_id", (F.col("balance") * 12).alias("annual_premium")))
    hits = (ins.filter(F.col("operation_type").isin(*CLAIM_OPERATIONS))
               .join(policies, "account_id", "inner")
               .filter((F.col("annual_premium") > 0)
                       & (F.col("amount") > CLAIM_PREMIUM_FACTOR * F.col("annual_premium"))))
    return _single_event_alert(
        hits, "CLAIM_OVER_PREMIUM", "HIGH", "ACCOUNT", "account_id", "operation_id",
        _details(operation_type=F.col("operation_type"), product_line=F.col("product_line"),
                 claim_amount=F.col("amount"), annual_premium=F.round("annual_premium", 2),
                 ratio=F.round(F.col("amount") / F.col("annual_premium"), 2), currency=F.col("currency")))


# ---------------------------------------------------------------------------- AML
def aml_events(bank: DataFrame) -> DataFrame:
    """Virements exécutés au-dessus du seuil déclaratif (indicateur is_aml_candidate posé par le job 1)."""
    threshold = F.when(F.col("currency") == "GHS", F.lit(AML_THRESHOLD_LOCAL["GHS"])) \
                 .otherwise(F.lit(AML_THRESHOLD_LOCAL["XOF"]))
    return (bank.filter(F.col("is_aml_candidate") & (F.col("transaction_status") == "SUCCESS"))
                .select(F.col("transaction_id").alias("event_id"), "country_code", "account_id",
                        "beneficiary_account", "transaction_type", "amount", "currency", "amount_eur",
                        threshold.alias("threshold_local"),
                        F.when(F.col("country_code") == "GH", "BOG").otherwise("BCEAO").alias("regulator"),
                        F.lit("DECLARATION_SEUIL").alias("declaration_type"), "channel", "event_time",
                        F.to_date("event_time").alias("txn_date")))


# ---------------------------------------------------------------------------- liquidité
def liquidity_windows(bank: DataFrame) -> DataFrame:
    """Entrées / sorties par pays sur fenêtres glissantes (5 min, pas 1 min), opérations réussies."""
    ok = bank.filter(F.col("transaction_status") == "SUCCESS")
    inflow = F.when(F.col("transaction_type").isin(*INFLOW_TYPES), F.col("amount_eur")).otherwise(0.0)
    outflow = F.when(F.col("transaction_type").isin(*OUTFLOW_TYPES), F.col("amount_eur")).otherwise(0.0)
    return (ok.groupBy(F.window("event_time", WINDOW, SLIDE).alias("w"), "country_code")
              .agg(F.round(F.sum(inflow), 2).alias("inflow_eur"), F.round(F.sum(outflow), 2).alias("outflow_eur"),
                   F.count("*").alias("txn_count"))
              .select("country_code", F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                      "inflow_eur", "outflow_eur", "txn_count"))


def reference_liquidity(accounts: DataFrame) -> DataFrame:
    """Dépôts de référence par pays : somme des soldes (EUR) des comptes de dépôt actifs."""
    return (accounts.filter(F.col("account_type").isin(*DEPOSIT_ACCOUNT_TYPES) & (F.col("status") == "ACTIVE"))
                    .groupBy("country_code").agg(F.round(F.sum("balance_eur"), 2).alias("reference_liquidity_eur")))


def liquidity_alerts(windows: DataFrame, reference: DataFrame, existing: DataFrame | None = None,
                     min_coverage: float = LIQUIDITY_MIN_COVERAGE) -> DataFrame:
    """Taux de couverture = (dépôts de référence - sorties nettes de la fenêtre) / dépôts de référence.

    Fenêtres sous le minimum qui se chevauchent = un même épisode de tension -> une alerte, qui garde
    la pire fenêtre ; rattachée à l'alerte déjà émise pour le même pays si les périodes se recoupent.
    """
    cov = (windows.join(F.broadcast(reference), "country_code", "inner")
           .withColumn("net_outflow_eur", F.round(F.col("outflow_eur") - F.col("inflow_eur"), 2))
           .withColumn("coverage_ratio", F.round((F.col("reference_liquidity_eur") - F.col("net_outflow_eur"))
                                                 / F.col("reference_liquidity_eur"), 4))
           .filter(F.col("coverage_ratio") < min_coverage))
    isl = _islands(cov, ["country_code"], "window_start", "window_end")
    worst = Window.partitionBy("country_code", "_island").orderBy("coverage_ratio", "window_start")
    span = Window.partitionBy("country_code", "_island")
    episodes = (isl.withColumn("period_start", F.min("window_start").over(span))
                   .withColumn("period_end", F.max("window_end").over(span))
                   .withColumn("_rn", F.row_number().over(worst)).filter("_rn = 1").drop("_rn", "_island"))

    if existing is not None:
        prev = existing.select(F.col("alert_id").alias("_prev_id"), "country_code",
                               F.col("period_start").alias("_ps"), F.col("period_end").alias("_pe"),
                               F.col("coverage_ratio").alias("_prev_cov"))
        e, p = episodes.alias("e"), prev.alias("p")
        cond = ((F.col("e.country_code") == F.col("p.country_code"))
                & (F.col("p._ps") <= F.col("e.period_end")) & (F.col("e.period_start") <= F.col("p._pe")))
        matched = e.join(p, cond, "left").select("e.*", "p._prev_id", "p._ps", "p._pe")
        w = Window.partitionBy("country_code", "period_start").orderBy(F.col("_ps").asc_nulls_last())
        episodes = (matched.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
                    .withColumn("period_start", F.least("period_start", F.coalesce("_ps", "period_start")))
                    .withColumn("period_end", F.greatest("period_end", F.coalesce("_pe", "period_end"))))
    else:
        episodes = episodes.withColumn("_prev_id", F.lit(None).cast("string"))

    return episodes.select(
        F.coalesce("_prev_id", _alert_id("LIQUIDITY", F.col("country_code"), F.col("period_start"))).alias("alert_id"),
        "country_code", "period_start", "period_end",
        F.col("window_start").alias("worst_window_start"), F.col("window_end").alias("worst_window_end"),
        "inflow_eur", "outflow_eur", "net_outflow_eur", "reference_liquidity_eur", "coverage_ratio",
        F.lit(min_coverage).alias("min_coverage"),
        F.when(F.col("coverage_ratio") < min_coverage - 0.08, "CRITICAL").otherwise("HIGH").alias("severity"),
        F.when(F.col("country_code") == "GH", "BOG").otherwise("BCEAO").alias("regulator"))


# ---------------------------------------------------------------------------- publication
def changed_alerts(candidates: DataFrame, existing: DataFrame | None, metric: str, higher_is_worse: bool) -> DataFrame:
    """Ne garde que les alertes nouvelles ou aggravées (plus d'événements, couverture plus basse...).

    Ajoute alert_status = NEW / UPDATED. Une alerte inchangée n'est pas republiée.
    """
    one = Window.partitionBy("alert_id").orderBy(F.col(metric).desc() if higher_is_worse else F.col(metric).asc())
    candidates = candidates.withColumn("_rn", F.row_number().over(one)).filter("_rn = 1").drop("_rn")
    if existing is None:
        return candidates.withColumn("alert_status", F.lit("NEW"))
    prev = existing.select("alert_id", F.col(metric).alias("_prev_metric"))
    worse = F.col(metric) > F.col("_prev_metric") if higher_is_worse else F.col(metric) < F.col("_prev_metric")
    return (candidates.join(prev, "alert_id", "left")
            .filter(F.col("_prev_metric").isNull() | worse)
            .withColumn("alert_status", F.when(F.col("_prev_metric").isNull(), "NEW").otherwise("UPDATED"))
            .drop("_prev_metric"))


def to_kafka(df: DataFrame, key_col: str) -> DataFrame:
    return df.select(F.col(key_col).cast("string").alias("key"), F.to_json(F.struct(*df.columns)).alias("value"))


def describe_rules() -> str:
    """Paramètres des règles (journalisés au démarrage du job)."""
    return json.dumps({"window": WINDOW, "slide": SLIDE, "burst_threshold_eur": BURST_THRESHOLD_EUR,
                       "burst_min_txn": BURST_MIN_TXN, "claim_factor": CLAIM_PREMIUM_FACTOR,
                       "aml_threshold_local": AML_THRESHOLD_LOCAL, "liquidity_min_coverage": LIQUIDITY_MIN_COVERAGE})
