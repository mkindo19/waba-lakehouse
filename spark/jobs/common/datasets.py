"""Catalogue des jeux de données : schéma explicite, clé, partitionnement, contrôles.

Une seule source de vérité pour les jobs Spark : ajouter un flux = ajouter une
entrée ici, sans toucher au code d'ingestion.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Nom du catalogue Iceberg côté Spark (surchargé à "spark_catalog" dans les tests locaux)
CATALOG = os.environ.get("LAKEHOUSE_CATALOG", "lakehouse")

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
ENTITY_TYPES = ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"]
CURRENCY_BY_COUNTRY = {c: "XOF" for c in COUNTRIES if c != "GH"} | {"GH": "GHS"}


def _s(*fields: tuple[str, object]) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


S, T, D, F, INT, B = StringType(), TimestampType(), DateType(), DoubleType(), IntegerType(), BooleanType()


@dataclass(frozen=True)
class ForeignKey:
    column: str
    ref_dataset: str
    ref_column: str


@dataclass(frozen=True)
class Dataset:
    name: str
    folder: str                      # sous-dossier dans raw-landing
    schema: StructType
    key: str                         # clé technique (idempotence)
    partition_sql: str               # clause PARTITIONED BY Iceberg
    is_referential: bool = False
    event_ts: str | None = None      # colonne horodatage (partition par jour)
    amount_cols: tuple[str, ...] = ()
    enum_checks: dict[str, tuple[str, ...]] = field(default_factory=dict)
    foreign_keys: tuple[ForeignKey, ...] = ()
    pii_columns: tuple[str, ...] = ()  # à pseudonymiser avant écriture

    @property
    def table(self) -> str:
        return f"{CATALOG}.raw.{self.name}"


DATASETS: dict[str, Dataset] = {}


def _register(ds: Dataset) -> None:
    DATASETS[ds.name] = ds


# ---------------------------------------------------------------- référentiels
_register(Dataset(
    name="customers", folder="referentials/customers", is_referential=True, key="customer_id",
    partition_sql="PARTITIONED BY (country_code)",
    schema=_s(("customer_id", S), ("country_code", S), ("entity_type", S), ("segment", S), ("kyc_level", S),
              ("onboarding_date", D), ("region", S), ("is_active", B)),
    enum_checks={"segment": ("RETAIL", "SME", "CORPORATE", "PREMIUM"), "kyc_level": ("BASIC", "STANDARD", "ENHANCED")},
))
_register(Dataset(
    name="products", folder="referentials/products", is_referential=True, key="product_id",
    partition_sql="PARTITIONED BY (country_code)",
    schema=_s(("product_id", S), ("country_code", S), ("entity_type", S), ("account_type", S), ("product_name", S),
              ("currency", S), ("interest_rate", F), ("monthly_fee", F), ("is_active", B)),
))
_register(Dataset(
    name="branches", folder="referentials/branches", is_referential=True, key="branch_id",
    partition_sql="PARTITIONED BY (country_code)",
    schema=_s(("branch_id", S), ("country_code", S), ("entity_type", S), ("city", S), ("region", S),
              ("branch_type", S), ("is_active", B)),
))
_register(Dataset(
    name="accounts", folder="referentials/accounts", is_referential=True, key="account_id",
    partition_sql="PARTITIONED BY (country_code)",
    schema=_s(("account_id", S), ("customer_id", S), ("country_code", S), ("account_type", S), ("currency", S),
              ("balance", F), ("credit_limit", F), ("opened_date", D), ("status", S), ("entity_type", S),
              ("product_id", S), ("account_number", S), ("iban", S)),
    enum_checks={"status": ("ACTIVE", "FROZEN", "CLOSED", "DORMANT"),
                 "account_type": ("CURRENT", "SAVINGS", "LOAN", "MOBILE_WALLET", "INSURANCE_POLICY")},
    foreign_keys=(ForeignKey("customer_id", "customers", "customer_id"),
                  ForeignKey("product_id", "products", "product_id")),
    pii_columns=("account_number", "iban"),
))

# ---------------------------------------------------------------- transactions
_register(Dataset(
    name="bank_transactions", folder="bank_transactions", key="transaction_id", event_ts="timestamp",
    partition_sql="PARTITIONED BY (country_code, days(`timestamp`))",
    schema=_s(("transaction_id", S), ("timestamp", T), ("account_id", S), ("beneficiary_account", S),
              ("branch_id", S), ("country_code", S), ("transaction_type", S), ("amount", F), ("currency", S),
              ("channel", S), ("transaction_status", S), ("fee_amount", F), ("entity_type", S)),
    amount_cols=("amount", "fee_amount"),
    enum_checks={"transaction_status": ("SUCCESS", "FAILED", "REVERSED"),
                 "transaction_type": ("TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE")},
    foreign_keys=(ForeignKey("account_id", "accounts", "account_id"),
                  ForeignKey("beneficiary_account", "accounts", "account_id"),
                  ForeignKey("branch_id", "branches", "branch_id")),
))
_register(Dataset(
    name="insurance_operations", folder="insurance_operations", key="operation_id", event_ts="timestamp",
    partition_sql="PARTITIONED BY (country_code, days(`timestamp`))",
    schema=_s(("operation_id", S), ("timestamp", T), ("customer_id", S), ("account_id", S), ("country_code", S),
              ("operation_type", S), ("product_line", S), ("amount", F), ("currency", S), ("claim_status", S),
              ("processing_days", INT), ("entity_type", S)),
    amount_cols=("amount",),
    enum_checks={"product_line": ("VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE")},
    foreign_keys=(ForeignKey("customer_id", "customers", "customer_id"),
                  ForeignKey("account_id", "accounts", "account_id")),
))
_register(Dataset(
    name="mobile_money_payments", folder="mobile_money_payments", key="payment_id", event_ts="timestamp",
    partition_sql="PARTITIONED BY (country_code, days(`timestamp`))",
    schema=_s(("payment_id", S), ("timestamp", T), ("sender_id", S), ("receiver_id", S), ("sender_country", S),
              ("receiver_country", S), ("amount", F), ("currency", S), ("payment_type", S), ("operator", S),
              ("status", S), ("fee_amount", F), ("entity_type", S), ("country_code", S)),
    amount_cols=("amount", "fee_amount"),
    enum_checks={"status": ("SUCCESS", "FAILED", "PENDING")},
    foreign_keys=(ForeignKey("sender_id", "customers", "customer_id"),
                  ForeignKey("receiver_id", "customers", "customer_id")),
))
_register(Dataset(
    name="loan_repayments", folder="loan_repayments", key="repayment_id", event_ts="timestamp",
    partition_sql="PARTITIONED BY (country_code, days(`timestamp`))",
    schema=_s(("repayment_id", S), ("timestamp", T), ("loan_account_id", S), ("customer_id", S),
              ("country_code", S), ("amount_due", F), ("amount_paid", F), ("currency", S), ("due_date", D),
              ("payment_date", D), ("days_overdue", INT), ("loan_type", S), ("repayment_status", S),
              ("entity_type", S)),
    amount_cols=("amount_due", "amount_paid"),
    enum_checks={"repayment_status": ("ON_TIME", "LATE", "DEFAULT")},
    foreign_keys=(ForeignKey("loan_account_id", "accounts", "account_id"),
                  ForeignKey("customer_id", "customers", "customer_id")),
))

# Ordre d'ingestion : un référentiel doit exister avant ce qui le référence.
INGESTION_ORDER = [
    "customers", "products", "branches", "accounts",
    "bank_transactions", "insurance_operations", "mobile_money_payments", "loan_repayments",
]
