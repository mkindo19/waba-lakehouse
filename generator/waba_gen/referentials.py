"""Génération des référentiels : customers, accounts, branches, products.

Ordre imposé par l'énoncé (annexe A.8) : les référentiels sont générés AVANT les
transactions, et chaque clé étrangère est tirée parmi des clés existantes :
customers -> accounts -> transactions. Aucune clé orpheline n'est possible.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from . import config as cfg
from .utils import get_logger, make_iban

log = get_logger(__name__)

EPOCH_START = date(2010, 1, 1)
EPOCH_END = date(2025, 12, 31)


@dataclass
class Referentials:
    customers: pd.DataFrame
    accounts: pd.DataFrame
    branches: pd.DataFrame
    products: pd.DataFrame

    def as_dict(self) -> dict[str, pd.DataFrame]:
        return {
            "customers": self.customers,
            "accounts": self.accounts,
            "branches": self.branches,
            "products": self.products,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _allocate(total: int, weights: dict[str, float], rng: np.random.Generator) -> dict[str, int]:
    """Répartit `total` lignes entre les clés selon des poids (somme exacte)."""
    keys = list(weights)
    probs = np.array([weights[k] for k in keys], dtype=float)
    probs /= probs.sum()
    counts = rng.multinomial(total, probs)
    return dict(zip(keys, counts.tolist(), strict=True))


def _random_dates(n: int, start: date, end: date, rng: np.random.Generator) -> np.ndarray:
    span = (end - start).days
    offsets = rng.integers(0, span + 1, size=n)
    return (np.datetime64(start) + offsets.astype("timedelta64[D]")).astype("datetime64[D]")


def _entities_for_country(country: str) -> tuple[list[str], np.ndarray]:
    ents = [e for e in cfg.ENTITY_TYPES if country in cfg.ENTITY_COUNTRIES[e]]
    probs = np.array([cfg.ENTITY_PROBS[cfg.ENTITY_TYPES.index(e)] for e in ents])
    return ents, probs / probs.sum()


# ---------------------------------------------------------------------------
# Products (50 lignes)
# ---------------------------------------------------------------------------
_PRODUCT_TEMPLATES: dict[tuple[str, str], list[str]] = {
    ("BANK", "CURRENT"): ["Compte Courant Essentiel", "Compte Courant Premium", "Compte Pro PME"],
    ("BANK", "SAVINGS"): ["Livret Epargne", "Plan Epargne Logement", "Depot a Terme"],
    ("BANK", "LOAN"): ["Credit Consommation", "Credit Immobilier", "Credit Investissement PME"],
    ("INSURANCE", "INSURANCE_POLICY"): ["Assurance Vie Horizon", "Multirisque Habitation", "Auto Tous Risques", "Sante Famille"],
    ("MOBILE_MONEY", "MOBILE_WALLET"): ["WABA Pay Wallet", "WABA Pay Marchand"],
    ("MICROFINANCE", "SAVINGS"): ["Tontine Digitale", "Epargne Solidaire"],
    ("MICROFINANCE", "LOAN"): ["Microcredit Commerce", "Credit Agricole Campagne"],
}


def generate_products(rng: np.random.Generator, n: int = cfg.REFERENTIAL_ROWS["products"]) -> pd.DataFrame:
    """Catalogue produits : au moins un produit par (pays, entité, type de compte)."""
    mandatory: list[tuple[str, str, str, str]] = []
    optional: list[tuple[str, str, str, str]] = []
    for (entity, acc_type), names in _PRODUCT_TEMPLATES.items():
        for country in cfg.ENTITY_COUNTRIES[entity]:
            mandatory.append((country, entity, acc_type, names[0]))
            optional.extend((country, entity, acc_type, name) for name in names[1:])

    if len(mandatory) > n:
        raise ValueError(f"{n} produits insuffisants ({len(mandatory)} obligatoires)")
    extra_idx = rng.choice(len(optional), size=n - len(mandatory), replace=False)
    rows = mandatory + [optional[i] for i in sorted(extra_idx)]

    df = pd.DataFrame(rows, columns=["country_code", "entity_type", "account_type", "product_name"])
    df = df.sort_values(["country_code", "entity_type", "account_type"]).reset_index(drop=True)
    df.insert(0, "product_id", [f"WABA-P-{i:03d}" for i in range(1, len(df) + 1)])
    df["currency"] = df["country_code"].map(cfg.CURRENCY_MAP)
    is_loan = df["account_type"].eq("LOAN")
    df["interest_rate"] = np.where(is_loan, rng.uniform(0.07, 0.24, len(df)).round(4), 0.0)
    df["interest_rate"] = np.where(df["account_type"].eq("SAVINGS"), rng.uniform(0.02, 0.055, len(df)).round(4), df["interest_rate"])
    df["monthly_fee"] = np.where(df["account_type"].eq("CURRENT"), rng.choice([1000, 2500, 5000], len(df)), 0).astype(float)
    df.loc[df["currency"].eq("GHS"), "monthly_fee"] = (df["monthly_fee"] / cfg.XOF_PER_GHS).round(2)
    df["is_active"] = True
    return df


# ---------------------------------------------------------------------------
# Customers (500 000 lignes)
# ---------------------------------------------------------------------------
def generate_customers(rng: np.random.Generator, n: int = cfg.REFERENTIAL_ROWS["customers"]) -> pd.DataFrame:
    parts = []
    for country, count in _allocate(n, cfg.COUNTRY_WEIGHTS, rng).items():
        ents, probs = _entities_for_country(country)
        parts.append(pd.DataFrame({
            "customer_id": [f"WABA-{country}-C-{i:06d}" for i in range(1, count + 1)],
            "country_code": country,
            "entity_type": rng.choice(ents, size=count, p=probs),
            "segment": rng.choice(cfg.SEGMENTS, size=count, p=cfg.SEGMENT_PROBS),
            "kyc_level": rng.choice(cfg.KYC_LEVELS, size=count, p=cfg.KYC_PROBS),
            "onboarding_date": _random_dates(count, EPOCH_START, EPOCH_END, rng),
            "region": rng.choice(cfg.REGIONS[country], size=count),
            "is_active": rng.random(count) > 0.08,
        }))
    df = pd.concat(parts, ignore_index=True)
    log.info("customers générés", extra={"ctx": {"rows": len(df)}})
    return df


# ---------------------------------------------------------------------------
# Branches (200 lignes)
# ---------------------------------------------------------------------------
def generate_branches(rng: np.random.Generator, n: int = cfg.REFERENTIAL_ROWS["branches"]) -> pd.DataFrame:
    """Au moins une agence par (pays, entité présente) pour garantir la cohérence."""
    pairs = [(c, e) for e in cfg.ENTITY_TYPES for c in cfg.ENTITY_COUNTRIES[e]]
    weights = {"BANK": 0.70, "INSURANCE": 0.15, "MOBILE_MONEY": 0.05, "MICROFINANCE": 0.10}
    pair_w = np.array([cfg.COUNTRY_WEIGHTS[c] * weights[e] for c, e in pairs])
    extra = rng.choice(len(pairs), size=n - len(pairs), p=pair_w / pair_w.sum())
    chosen = pairs + [pairs[i] for i in extra]

    df = pd.DataFrame(chosen, columns=["country_code", "entity_type"]).sort_values(["country_code", "entity_type"])
    df["seq"] = df.groupby("country_code").cumcount() + 1
    df.insert(0, "branch_id", "WABA-" + df["country_code"] + "-B-" + df["seq"].map("{:03d}".format))
    df["city"] = [rng.choice(cfg.REGIONS[c]) for c in df["country_code"]]
    capitals = {c: cities[0] for c, cities in cfg.REGIONS.items()}
    df["region"] = np.where(df["city"] == df["country_code"].map(capitals), "Capitale", "Interieur")
    df["branch_type"] = rng.choice(cfg.BRANCH_TYPES, size=len(df), p=cfg.BRANCH_TYPE_PROBS)
    df["is_active"] = rng.random(len(df)) > 0.03
    return df.drop(columns="seq").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Accounts (800 000 lignes)
# ---------------------------------------------------------------------------
_BALANCE_PARAMS = {  # (mu, sigma) lognormal en XOF
    "CURRENT": (12.0, 1.4),
    "SAVINGS": (13.0, 1.3),
    "LOAN": (14.5, 1.1),          # encours restant dû
    "MOBILE_WALLET": (9.5, 1.2),
    "INSURANCE_POLICY": (11.0, 0.8),  # prime mensuelle de référence
}


def generate_accounts(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    rng: np.random.Generator,
    n: int = cfg.REFERENTIAL_ROWS["accounts"],
) -> pd.DataFrame:
    """Chaque client a au moins un compte ; les comptes restants sont répartis au hasard."""
    if n < len(customers):
        raise ValueError("Il faut au moins autant de comptes que de clients")
    owner_idx = np.concatenate([
        np.arange(len(customers)),
        rng.integers(0, len(customers), size=n - len(customers)),
    ])
    owners = customers.iloc[owner_idx].reset_index(drop=True)

    acc = pd.DataFrame({
        "customer_id": owners["customer_id"].to_numpy(),
        "country_code": owners["country_code"].to_numpy(),
        "entity_type": owners["entity_type"].to_numpy(),
    })
    acc["account_type"] = ""
    for entity, types in cfg.ENTITY_ACCOUNT_TYPES.items():
        mask = acc["entity_type"].eq(entity)
        acc.loc[mask, "account_type"] = rng.choice(types, size=int(mask.sum()), p=cfg.ENTITY_ACCOUNT_PROBS[entity])

    acc = acc.sort_values(["country_code", "customer_id"], kind="stable").reset_index(drop=True)
    seq = acc.groupby("country_code").cumcount() + 1
    acc.insert(0, "account_id", "WABA-" + acc["country_code"] + "-A-" + seq.map("{:07d}".format))

    acc["currency"] = acc["country_code"].map(cfg.CURRENCY_MAP)

    # Soldes en devise locale
    balance = np.zeros(len(acc))
    for acc_type, (mu, sigma) in _BALANCE_PARAMS.items():
        mask = acc["account_type"].eq(acc_type).to_numpy()
        balance[mask] = rng.lognormal(mu, sigma, mask.sum())
    ghs = acc["currency"].eq("GHS").to_numpy()
    balance[ghs] /= cfg.XOF_PER_GHS
    # Le franc CFA n'a pas de subdivision : montants XOF entiers, GHS à 2 décimales
    acc["balance"] = np.where(ghs, balance.round(2), balance.round(0))

    # Plafond : montant initial du prêt (>= encours) ou découvert autorisé
    is_loan = acc["account_type"].eq("LOAN").to_numpy()
    is_current = acc["account_type"].eq("CURRENT").to_numpy()
    credit_limit = np.zeros(len(acc))
    credit_limit[is_loan] = balance[is_loan] * rng.uniform(1.1, 2.5, is_loan.sum())
    overdraft = is_current & (rng.random(len(acc)) < 0.3)
    credit_limit[overdraft] = balance[overdraft] * rng.uniform(0.1, 0.5, overdraft.sum())
    acc["credit_limit"] = np.where(ghs, credit_limit.round(2), credit_limit.round(0))

    # Ouverture postérieure à l'entrée en relation
    onboarding = customers.set_index("customer_id").loc[acc["customer_id"], "onboarding_date"].to_numpy()
    onboarding = onboarding.astype("datetime64[D]")
    max_days = (np.datetime64(EPOCH_END) - onboarding).astype(int)
    acc["opened_date"] = onboarding + (rng.random(len(acc)) * (max_days + 1)).astype(int).astype("timedelta64[D]")
    acc["status"] = rng.choice(cfg.ACCOUNT_STATUSES, size=len(acc), p=cfg.ACCOUNT_STATUS_PROBS)

    # Produit cohérent (pays, entité, type de compte)
    prod_groups = products.groupby(["country_code", "entity_type", "account_type"])["product_id"].apply(np.array)
    acc["product_id"] = ""
    for key, idx in acc.groupby(["country_code", "entity_type", "account_type"]).groups.items():
        acc.loc[idx, "product_id"] = rng.choice(prod_groups[key], size=len(idx))

    # Identifiants bancaires sensibles (pseudonymisés dès l'ingestion Spark)
    acc["account_number"] = [f"{x:012d}" for x in rng.integers(10**10, 10**12, size=len(acc))]
    bank_codes = {c: f"{c}{i:03d}" for i, c in enumerate(cfg.COUNTRIES, start=101)}
    branch_codes = rng.integers(1, 99_999, size=len(acc))
    rib_keys = rng.integers(0, 100, size=len(acc))
    acc["iban"] = [
        make_iban(c, bank_codes[c], f"{b:05d}", num, f"{k:02d}") if c != "GH" else None  # pas d'IBAN au Ghana
        for c, b, num, k in zip(acc["country_code"], branch_codes, acc["account_number"], rib_keys, strict=True)
    ]

    cols = [
        "account_id", "customer_id", "country_code", "account_type", "currency", "balance",
        "credit_limit", "opened_date", "status", "entity_type", "product_id", "account_number", "iban",
    ]
    log.info("accounts générés", extra={"ctx": {"rows": len(acc)}})
    return acc[cols]


def generate_referentials(seed: int = 42, scale: float = 1.0) -> Referentials:
    """Génère les 4 référentiels. `scale` < 1 réduit les volumes (tests, démo rapide)."""
    rng = np.random.default_rng(seed)
    n_cust = max(1_000, int(cfg.REFERENTIAL_ROWS["customers"] * scale))
    n_acc = max(n_cust, int(cfg.REFERENTIAL_ROWS["accounts"] * scale))
    products = generate_products(rng)
    customers = generate_customers(rng, n_cust)
    branches = generate_branches(rng)
    accounts = generate_accounts(customers, products, rng, n_acc)
    return Referentials(customers=customers, accounts=accounts, branches=branches, products=products)
