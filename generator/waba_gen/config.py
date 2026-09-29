"""Configuration métier du WABA Group : pays, entités, devises, distributions.

Toutes les constantes de simulation sont centralisées ici pour que le générateur,
les tests et la documentation partagent une seule source de vérité.
"""

from __future__ import annotations

# --- Pays (ISO 3166-1 alpha-2) ---------------------------------------------
COUNTRIES: list[str] = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]

# NB : dans la réalité la Guinée (GN) n'est pas membre de l'UEMOA et utilise le
# franc guinéen (GNF). Le challenge la rattache à la zone XOF : on suit
# l'énoncé et on documente cette simplification dans le write-up.
UEMOA: list[str] = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]
CURRENCY_MAP: dict[str, str] = {c: "XOF" for c in UEMOA} | {"GH": "GHS"}

# Poids de volume par pays (CI = marché principal du groupe)
COUNTRY_WEIGHTS: dict[str, float] = {
    "CI": 0.30, "SN": 0.18, "ML": 0.10, "BF": 0.10,
    "GN": 0.08, "TG": 0.07, "BJ": 0.07, "GH": 0.10,
}

# Facteur de conversion approximatif pour générer des montants GHS réalistes
# à partir d'une distribution calibrée en XOF (simulation uniquement).
XOF_PER_GHS: float = 45.0

# --- Entités et présence géographique (cf. tableau "Contexte & Mission") ----
ENTITY_TYPES: list[str] = ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"]
ENTITY_PROBS: list[float] = [0.55, 0.20, 0.18, 0.07]

ENTITY_COUNTRIES: dict[str, list[str]] = {
    "BANK": COUNTRIES,                        # Retail UEMOA + Corporate Ghana
    "INSURANCE": COUNTRIES,                   # Vie UEMOA + IARD Ghana
    "MOBILE_MONEY": ["CI", "SN", "BF", "GH"],
    "MICROFINANCE": ["ML", "GN", "BF"],
}

# Types de comptes autorisés par entité
ENTITY_ACCOUNT_TYPES: dict[str, list[str]] = {
    "BANK": ["CURRENT", "SAVINGS", "LOAN"],
    "INSURANCE": ["INSURANCE_POLICY"],
    "MOBILE_MONEY": ["MOBILE_WALLET"],
    "MICROFINANCE": ["SAVINGS", "LOAN"],
}
ENTITY_ACCOUNT_PROBS: dict[str, list[float]] = {
    "BANK": [0.55, 0.25, 0.20],
    "INSURANCE": [1.0],
    "MOBILE_MONEY": [1.0],
    "MICROFINANCE": [0.45, 0.55],
}

# --- Clients -----------------------------------------------------------------
SEGMENTS: list[str] = ["RETAIL", "SME", "CORPORATE", "PREMIUM"]
SEGMENT_PROBS: list[float] = [0.65, 0.20, 0.10, 0.05]
KYC_LEVELS: list[str] = ["BASIC", "STANDARD", "ENHANCED"]
KYC_PROBS: list[float] = [0.3, 0.5, 0.2]

REGIONS: dict[str, list[str]] = {
    "CI": ["Abidjan", "Bouake", "Yamoussoukro", "San Pedro", "Korhogo"],
    "SN": ["Dakar", "Thies", "Ziguinchor", "Saint-Louis", "Kaolack"],
    "ML": ["Bamako", "Sikasso", "Segou", "Mopti", "Tombouctou"],
    "BF": ["Ouagadougou", "Bobo-Dioulasso", "Koudougou", "Banfora"],
    "GN": ["Conakry", "Nzerekore", "Kindia", "Kankan"],
    "TG": ["Lome", "Sokode", "Kara", "Atakpame"],
    "BJ": ["Cotonou", "Porto-Novo", "Parakou", "Abomey-Calavi"],
    "GH": ["Accra", "Kumasi", "Tamale", "Cape Coast", "Sunyani"],
}

ACCOUNT_STATUSES: list[str] = ["ACTIVE", "FROZEN", "CLOSED", "DORMANT"]
ACCOUNT_STATUS_PROBS: list[float] = [0.88, 0.02, 0.04, 0.06]

BRANCH_TYPES: list[str] = ["FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT"]
BRANCH_TYPE_PROBS: list[float] = [0.45, 0.10, 0.30, 0.15]

# --- Transactions bancaires (annexe A.4) ------------------------------------
TXN_TYPES: list[str] = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE"]
TXN_TYPE_PROBS: list[float] = [0.35, 0.30, 0.15, 0.15, 0.05]
CHANNELS: list[str] = ["BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD"]
CHANNEL_PROBS: list[float] = [0.20, 0.15, 0.35, 0.20, 0.10]
TXN_STATUSES: list[str] = ["SUCCESS", "FAILED", "REVERSED"]
TXN_STATUS_PROBS: list[float] = [0.92, 0.05, 0.03]

# --- Assurance (annexe A.5) -------------------------------------------------
INSURANCE_OP_TYPES: list[str] = [
    "PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT",
    "POLICY_RENEWAL", "POLICY_CANCELLATION",
]
INSURANCE_OP_PROBS: list[float] = [0.55, 0.14, 0.12, 0.14, 0.05]
PRODUCT_LINES: list[str] = ["VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE"]
PRODUCT_LINE_PROBS: list[float] = [0.25, 0.30, 0.15, 0.20, 0.10]
CLAIM_STATUSES: list[str] = ["PENDING", "APPROVED", "REJECTED", "PAID"]

# Loss ratio cible par pays (réaliste : 50 % – 85 %, certains > seuil CIMA 70 %)
TARGET_LOSS_RATIO: dict[str, float] = {
    "CI": 0.62, "SN": 0.58, "ML": 0.78, "BF": 0.74,
    "GN": 0.81, "TG": 0.66, "BJ": 0.69, "GH": 0.55,
}

# --- Mobile money (annexe A.6) ----------------------------------------------
MM_PAYMENT_TYPES: list[str] = ["P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME", "CROSS_BORDER_TRANSFER"]
MM_PAYMENT_PROBS: list[float] = [0.40, 0.25, 0.15, 0.12, 0.08]
MM_OPERATORS: list[str] = ["WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"]
MM_OPERATOR_PROBS: list[float] = [0.50, 0.30, 0.20]
MM_STATUSES: list[str] = ["SUCCESS", "FAILED", "PENDING"]
MM_STATUS_PROBS: list[float] = [0.94, 0.04, 0.02]

# --- Remboursements de crédit (annexe A.7) ----------------------------------
LOAN_TYPES_BANK: list[str] = ["CONSUMER", "MORTGAGE", "SME"]
LOAN_TYPES_MICRO: list[str] = ["AGRICULTURAL", "MICROCREDIT"]
REPAYMENT_STATUSES: list[str] = ["ON_TIME", "LATE", "DEFAULT"]

# Taux NPL cible par pays (réaliste : 3 % – 8 %, certains > seuil BCEAO 5 %)
TARGET_NPL_RATIO: dict[str, float] = {
    "CI": 0.034, "SN": 0.041, "ML": 0.068, "BF": 0.057,
    "GN": 0.075, "TG": 0.046, "BJ": 0.049, "GH": 0.038,
}

# --- Volumes par défaut (cf. énoncé 1.1) -------------------------------------
DEFAULT_ROWS: dict[str, int] = {
    "bank_transactions": 10_000,
    "insurance_operations": 5_000,
    "mobile_money_payments": 20_000,
    "loan_repayments": 5_000,
}
REFERENTIAL_ROWS: dict[str, int] = {
    "customers": 500_000,
    "accounts": 800_000,
    "branches": 200,
    "products": 50,
}

# --- Seuils réglementaires (utiles pour injecter des cas de test) ------------
AML_THRESHOLD: dict[str, float] = {"XOF": 1_000_000, "GHS": 5_000}
FRAUD_BURST_THRESHOLD_XOF: float = 500_000

# Part des lignes volontairement "anormales" injectées pour tester la
# détection de fraude en aval (niveau 3).
FRAUD_INJECTION_RATE: float = 0.005

# --- Nommage des fichiers / chemins MinIO ------------------------------------
# Préfixe de fichier -> (dossier dans raw-landing, table cible raw.*)
DATASETS: dict[str, dict[str, str]] = {
    "bank_transactions": {"file_prefix": "bank_txn", "folder": "bank_transactions"},
    "insurance_operations": {"file_prefix": "insurance_ops", "folder": "insurance_operations"},
    "mobile_money_payments": {"file_prefix": "mobile_money", "folder": "mobile_money_payments"},
    "loan_repayments": {"file_prefix": "loan_repayments", "folder": "loan_repayments"},
}
REFERENTIALS: list[str] = ["customers", "accounts", "branches", "products"]
