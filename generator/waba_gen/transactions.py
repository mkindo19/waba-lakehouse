"""Génération des flux transactionnels : banque, assurance, mobile money, crédits.

Principes :
- chaque clé (account_id, customer_id, branch_id) est tirée dans les référentiels
  du même pays -> intégrité référentielle garantie par construction ;
- montants en devise locale (XOF zone UEMOA, GHS au Ghana) ;
- distributions calibrées pour produire des KPIs réalistes (NPL 3-8 %, loss ratio
  50-85 %) ;
- une petite part de cas "suspects" est injectée volontairement pour que les
  règles de fraude / AML du niveau 3 aient quelque chose à détecter.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np
import pandas as pd

from . import config as cfg
from .referentials import Referentials
from .utils import get_logger, stable_uniform

log = get_logger(__name__)

# Profil horaire : activité concentrée en journée (poids par heure 0..23)
_HOUR_WEIGHTS = np.array([1, 1, 1, 1, 1, 2, 4, 6, 9, 10, 10, 10, 9, 9, 10, 10, 9, 8, 7, 5, 4, 3, 2, 1], dtype=float)
_HOUR_PROBS = _HOUR_WEIGHTS / _HOUR_WEIGHTS.sum()

# Entités concernées par chaque jeu de données
DATASET_ENTITIES: dict[str, set[str]] = {
    "bank_transactions": {"BANK", "MICROFINANCE"},
    "insurance_operations": {"INSURANCE"},
    "mobile_money_payments": {"MOBILE_MONEY"},
    "loan_repayments": {"BANK", "MICROFINANCE"},
}


# ---------------------------------------------------------------------------
# Index des référentiels (pools de clés valides par pays)
# ---------------------------------------------------------------------------
@dataclass
class RefIndex:
    """Pré-calcule, par pays, les ensembles de clés utilisables par chaque flux."""

    bank_accounts: dict[str, pd.DataFrame] = field(default_factory=dict)
    beneficiaries: dict[str, np.ndarray] = field(default_factory=dict)
    branches: dict[tuple[str, str], np.ndarray] = field(default_factory=dict)
    policies: dict[str, pd.DataFrame] = field(default_factory=dict)
    mm_customers: dict[str, np.ndarray] = field(default_factory=dict)
    customers: dict[str, np.ndarray] = field(default_factory=dict)
    loans: dict[str, pd.DataFrame] = field(default_factory=dict)

    @classmethod
    def build(cls, refs: Referentials) -> RefIndex:
        idx = cls()
        acc = refs.accounts
        active = acc[acc["status"].eq("ACTIVE")]
        cust = refs.customers.set_index("customer_id")

        for country, g in active.groupby("country_code"):
            bank = g[g["account_type"].isin(["CURRENT", "SAVINGS"]) & g["entity_type"].isin(["BANK", "MICROFINANCE"])]
            idx.bank_accounts[country] = bank[["account_id", "entity_type"]].reset_index(drop=True)
            idx.beneficiaries[country] = g.loc[
                g["account_type"].isin(["CURRENT", "SAVINGS", "MOBILE_WALLET"]), "account_id"
            ].to_numpy()
            pol = g[g["account_type"].eq("INSURANCE_POLICY")]
            idx.policies[country] = pol[["account_id", "customer_id", "balance"]].reset_index(drop=True)
            idx.mm_customers[country] = g.loc[g["account_type"].eq("MOBILE_WALLET"), "customer_id"].unique()
            loans = g[g["account_type"].eq("LOAN")][["account_id", "customer_id", "entity_type", "credit_limit"]].copy()
            loans["segment"] = cust.loc[loans["customer_id"], "segment"].to_numpy()
            loans["risk_u"] = stable_uniform(loans["account_id"].to_numpy())
            idx.loans[country] = loans.reset_index(drop=True)

        for country, g in refs.customers.groupby("country_code"):
            idx.customers[country] = g["customer_id"].to_numpy()

        active_branches = refs.branches[refs.branches["is_active"]]
        for (country, entity), g in active_branches.groupby(["country_code", "entity_type"]):
            idx.branches[(country, entity)] = g["branch_id"].to_numpy()
        return idx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_uuids(n: int, rng: np.random.Generator) -> list[str]:
    """UUID v4 reproductibles (dérivés du générateur aléatoire seedé)."""
    raw = rng.bytes(16 * n)
    return [str(uuid.UUID(bytes=raw[i * 16:(i + 1) * 16], version=4)) for i in range(n)]


def random_timestamps(day: date, n: int, rng: np.random.Generator) -> np.ndarray:
    hours = rng.choice(24, size=n, p=_HOUR_PROBS)
    seconds = hours * 3600 + rng.integers(0, 3600, size=n)
    base = np.datetime64(day, "s")
    return np.sort(base + seconds.astype("timedelta64[s]"))


def iso(ts: np.ndarray) -> np.ndarray:
    """Horodatage ISO 8601 (sans fuseau, interprété en UTC)."""
    return np.datetime_as_string(ts, unit="s")


def round_ccy(amount: np.ndarray, currency: str) -> np.ndarray:
    """Arrondi monétaire : XOF sans décimale (pas de centimes), GHS à 2 décimales."""
    return np.round(amount, 2 if currency == "GHS" else 0)


def local_amount(amount_xof: np.ndarray, currency: str) -> np.ndarray:
    """Convertit un montant simulé en XOF vers la devise locale et arrondit."""
    if currency == "GHS":
        return round_ccy(amount_xof / cfg.XOF_PER_GHS, currency)
    return round_ccy(amount_xof, currency)


def _pick(arr: np.ndarray, n: int, rng: np.random.Generator) -> np.ndarray:
    return arr[rng.integers(0, len(arr), size=n)]


def _n_anomalies(n: int, rng: np.random.Generator) -> int:
    return int(rng.poisson(n * cfg.FRAUD_INJECTION_RATE))


# ---------------------------------------------------------------------------
# A.4 — Transactions bancaires
# ---------------------------------------------------------------------------
def gen_bank_transactions(country: str, day: date, n: int, idx: RefIndex, rng: np.random.Generator) -> pd.DataFrame:
    currency = cfg.CURRENCY_MAP[country]
    pool = idx.bank_accounts[country]
    rows = pool.iloc[rng.integers(0, len(pool), size=n)].reset_index(drop=True)
    txn_type = rng.choice(cfg.TXN_TYPES, size=n, p=cfg.TXN_TYPE_PROBS)

    # Bénéficiaire : même pays, sauf virement international
    beneficiary = _pick(idx.beneficiaries[country], n, rng)
    foreign = [c for c in cfg.COUNTRIES if c != country]
    for i in np.flatnonzero(txn_type == "INTERNATIONAL_WIRE"):
        beneficiary[i] = _pick(idx.beneficiaries[rng.choice(foreign)], 1, rng)[0]

    branch = np.array([
        _pick(idx.branches.get((country, e), idx.branches[(country, "BANK")]), 1, rng)[0]
        for e in rows["entity_type"]
    ])
    amount = local_amount(np.maximum(500, rng.lognormal(12, 1.5, n)), currency)
    status = rng.choice(cfg.TXN_STATUSES, size=n, p=cfg.TXN_STATUS_PROBS)

    df = pd.DataFrame({
        "transaction_id": make_uuids(n, rng),
        "timestamp": iso(random_timestamps(day, n, rng)),
        "account_id": rows["account_id"],
        "beneficiary_account": beneficiary,
        "branch_id": branch,
        "country_code": country,
        "transaction_type": txn_type,
        "amount": amount,
        "currency": currency,
        "channel": rng.choice(cfg.CHANNELS, size=n, p=cfg.CHANNEL_PROBS),
        "transaction_status": status,
        "fee_amount": np.where(status == "SUCCESS", round_ccy(amount * 0.001, currency), 0.0),
        "entity_type": rows["entity_type"],
    })
    return pd.concat([df, _bank_fraud_bursts(country, day, df, rng)], ignore_index=True)


def _bank_fraud_bursts(country: str, day: date, base: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Injecte des rafales : 3-4 transactions > 500 000 XOF sur un même compte en < 5 min."""
    n_bursts = _n_anomalies(len(base), rng)
    if n_bursts == 0 or base.empty:
        return base.iloc[0:0]
    currency = cfg.CURRENCY_MAP[country]
    parts = []
    for _ in range(n_bursts):
        src = base.iloc[int(rng.integers(0, len(base)))]
        k = int(rng.integers(3, 5))
        start = np.datetime64(day, "s") + np.timedelta64(int(rng.integers(8 * 3600, 20 * 3600)), "s")
        ts = start + np.sort(rng.integers(0, 240, size=k)).astype("timedelta64[s]")
        amount = local_amount(rng.uniform(550_000, 2_000_000, k), currency)
        burst = pd.DataFrame([src] * k).reset_index(drop=True)
        burst["transaction_id"] = make_uuids(k, rng)
        burst["timestamp"] = iso(ts)
        burst["transaction_type"] = rng.choice(["TRANSFER", "WITHDRAWAL"], size=k)
        burst["amount"] = amount
        burst["transaction_status"] = "SUCCESS"
        burst["fee_amount"] = round_ccy(amount * 0.001, currency)
        parts.append(burst)
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# A.5 — Opérations d'assurance
# ---------------------------------------------------------------------------
def gen_insurance_operations(country: str, day: date, n: int, idx: RefIndex, rng: np.random.Generator) -> pd.DataFrame:
    currency = cfg.CURRENCY_MAP[country]
    pool = idx.policies[country]
    rows = pool.iloc[rng.integers(0, len(pool), size=n)].reset_index(drop=True)
    op = rng.choice(cfg.INSURANCE_OP_TYPES, size=n, p=cfg.INSURANCE_OP_PROBS)

    # Ghana : IARD uniquement (pas de VIE / PREVOYANCE)
    lines = cfg.PRODUCT_LINES
    probs = np.array(cfg.PRODUCT_LINE_PROBS)
    if country == "GH":
        keep = np.array([pl.startswith("IARD") for pl in lines])
        lines = [pl for pl, k in zip(lines, keep, strict=True) if k]
        probs = probs[keep] / probs[keep].sum()
    product_line = rng.choice(lines, size=n, p=probs)

    premium = rows["balance"].to_numpy()          # prime mensuelle de la police
    amount = premium.copy()
    is_sub = op == "CLAIM_SUBMISSION"
    is_pay = op == "CLAIM_PAYMENT"
    is_cancel = op == "POLICY_CANCELLATION"

    # Calibrage du loss ratio : chaque sinistre payé vaut en espérance
    # LR cible x (primes attendues par sinistre) -> le ratio converge vers la cible
    # par pays et par mois, quelle que soit la taille des fichiers.
    probs_op = dict(zip(cfg.INSURANCE_OP_TYPES, cfg.INSURANCE_OP_PROBS, strict=True))
    premiums_per_claim = (probs_op["PREMIUM_PAYMENT"] + probs_op["POLICY_RENEWAL"]) / probs_op["CLAIM_PAYMENT"]
    target_lr = cfg.TARGET_LOSS_RATIO[country]
    sigma = 0.5
    severity = rng.lognormal(-sigma**2 / 2, sigma, is_pay.sum())  # espérance = 1
    amount[is_pay] = target_lr * premiums_per_claim * pool["balance"].mean() * severity
    amount[is_sub] = premium[is_sub] * rng.lognormal(1.2, 0.8, is_sub.sum())
    amount[is_cancel] = premium[is_cancel] * rng.uniform(0.0, 0.5, is_cancel.sum())

    claim_status = np.full(n, None, dtype=object)
    claim_status[is_sub] = rng.choice(["PENDING", "APPROVED", "REJECTED"], size=is_sub.sum(), p=[0.4, 0.4, 0.2])
    claim_status[is_pay] = "PAID"

    # Délai de traitement (jours ouvrés) : plus long en VIE, plus court en IARD_AUTO
    base_days = np.where(product_line == "VIE", 25, np.where(product_line == "IARD_AUTO", 8, 14))
    days = np.maximum(1, np.round(base_days * rng.lognormal(0, 0.5, n))).astype(int)
    processed = is_pay | (is_sub & (claim_status != "PENDING"))
    processing_days = pd.array(np.where(processed, days, 0), dtype="Int64")
    processing_days[~processed] = pd.NA

    df = pd.DataFrame({
        "operation_id": make_uuids(n, rng),
        "timestamp": iso(random_timestamps(day, n, rng)),
        "customer_id": rows["customer_id"],
        "account_id": rows["account_id"],
        "country_code": country,
        "operation_type": op,
        "product_line": product_line,
        "amount": np.round(amount, 2 if currency == "GHS" else 0),
        "currency": currency,
        "claim_status": claim_status,
        "processing_days": processing_days,
        "entity_type": "INSURANCE",
    })

    # Anomalie : sinistre > 3x la prime annuelle (12 x prime mensuelle).
    # Ciblée sur des polices à prime modeste pour ne pas fausser le loss ratio global.
    candidates = np.flatnonzero(is_pay & (premium <= pool["balance"].median()))
    n_bad = min(_n_anomalies(int(is_pay.sum()), rng), len(candidates))
    if n_bad:
        bad = rng.choice(candidates, size=n_bad, replace=False)
        df.loc[bad, "amount"] = np.round(premium[bad] * 12 * rng.uniform(3.2, 4.5, n_bad), 2 if currency == "GHS" else 0)
    return df


# ---------------------------------------------------------------------------
# A.6 — Paiements mobile money
# ---------------------------------------------------------------------------
_MM_AMOUNT = {  # (mu, sigma) lognormal en XOF
    "P2P": (9.8, 1.1), "MERCHANT_PAYMENT": (9.2, 1.0), "BILL_PAYMENT": (9.9, 0.7),
    "AIRTIME": (7.0, 0.7), "CROSS_BORDER_TRANSFER": (11.0, 1.0),
}
_MM_FEE = {"P2P": 0.01, "MERCHANT_PAYMENT": 0.005, "BILL_PAYMENT": 0.0, "AIRTIME": 0.0, "CROSS_BORDER_TRANSFER": 0.02}
# Corridors transfrontaliers privilégiés (diaspora / commerce)
_CORRIDOR_WEIGHTS = {"CI": 3, "SN": 2, "ML": 3, "BF": 3, "GN": 1, "TG": 1, "BJ": 1, "GH": 1}


def gen_mobile_money_payments(country: str, day: date, n: int, idx: RefIndex, rng: np.random.Generator) -> pd.DataFrame:
    currency = cfg.CURRENCY_MAP[country]
    senders = _pick(idx.mm_customers[country], n, rng)
    ptype = rng.choice(cfg.MM_PAYMENT_TYPES, size=n, p=cfg.MM_PAYMENT_PROBS)

    receiver_country = np.full(n, country, dtype=object)
    cross = ptype == "CROSS_BORDER_TRANSFER"
    others = [c for c in cfg.COUNTRIES if c != country]
    w = np.array([_CORRIDOR_WEIGHTS[c] for c in others], dtype=float)
    receiver_country[cross] = rng.choice(others, size=cross.sum(), p=w / w.sum())
    receivers = np.array([_pick(idx.customers[rc], 1, rng)[0] for rc in receiver_country])

    amount_xof = np.array([rng.lognormal(*_MM_AMOUNT[t]) for t in ptype])
    amount = local_amount(np.maximum(100, amount_xof), currency)
    status = rng.choice(cfg.MM_STATUSES, size=n, p=cfg.MM_STATUS_PROBS)
    fee_rate = np.array([_MM_FEE[t] for t in ptype])

    df = pd.DataFrame({
        "payment_id": make_uuids(n, rng),
        "timestamp": iso(random_timestamps(day, n, rng)),
        "sender_id": senders,
        "receiver_id": receivers,
        "sender_country": country,
        "receiver_country": receiver_country,
        "amount": amount,
        "currency": currency,
        "payment_type": ptype,
        "operator": rng.choice(cfg.MM_OPERATORS, size=n, p=cfg.MM_OPERATOR_PROBS),
        "status": status,
        "fee_amount": np.where(status == "SUCCESS", round_ccy(amount * fee_rate, currency), 0.0),
        "entity_type": "MOBILE_MONEY",
        "country_code": country,   # champ commun exigé sur toutes les tables (= pays émetteur)
    })

    # Anomalie : client d'un autre pays qui émet depuis ce pays (profil inhabituel)
    n_bad = _n_anomalies(n, rng)
    mm_others = [c for c in idx.mm_customers if c != country and len(idx.mm_customers[c])]
    if n_bad and mm_others:
        bad = rng.choice(n, size=min(n_bad, n), replace=False)
        df.loc[bad, "sender_id"] = [_pick(idx.mm_customers[rng.choice(mm_others)], 1, rng)[0] for _ in bad]
    return df


# ---------------------------------------------------------------------------
# A.7 — Remboursements de crédit
# ---------------------------------------------------------------------------
_INSTALLMENTS = {"CONSUMER": 24, "MORTGAGE": 180, "SME": 48, "AGRICULTURAL": 12, "MICROCREDIT": 12}


def gen_loan_repayments(country: str, day: date, n: int, idx: RefIndex, rng: np.random.Generator) -> pd.DataFrame:
    currency = cfg.CURRENCY_MAP[country]
    pool = idx.loans[country]
    rows = pool.iloc[rng.integers(0, len(pool), size=n)].reset_index(drop=True)

    # Profil stable par compte : type de prêt et statut "en défaut" (NPL cible)
    u2 = (rows["risk_u"].to_numpy() * 1000) % 1
    is_micro = rows["entity_type"].eq("MICROFINANCE").to_numpy()
    is_pro = rows["segment"].isin(["SME", "CORPORATE"]).to_numpy()
    loan_type = np.where(is_micro, np.where(u2 < 0.4, "AGRICULTURAL", "MICROCREDIT"),
                         np.where(is_pro, "SME", np.where(u2 < 0.3, "MORTGAGE", "CONSUMER")))
    defaulting = rows["risk_u"].to_numpy() < cfg.TARGET_NPL_RATIO[country]

    r = rng.random(n)
    status = np.where(defaulting, np.where(r < 0.8, "DEFAULT", "LATE"), np.where(r < 0.87, "ON_TIME", "LATE"))

    installments = np.array([_INSTALLMENTS[t] for t in loan_type])
    amount_due = round_ccy(rows["credit_limit"].to_numpy() / installments, currency)
    days_overdue = np.select(
        [status == "ON_TIME", status == "LATE"],
        [0, rng.integers(1, 61, n)],
        default=rng.integers(91, 401, n),
    )
    event_day = np.datetime64(day, "D")
    advance = rng.integers(0, 6, n)
    due_date = np.where(status == "ON_TIME", event_day + advance.astype("timedelta64[D]"),
                        event_day - days_overdue.astype("timedelta64[D]"))
    payment_date = np.where(status == "DEFAULT", None, str(day))
    amount_paid = np.where(status == "DEFAULT", 0.0,
                           np.where(status == "LATE", round_ccy(amount_due * rng.uniform(0.5, 1.0, n), currency), amount_due))

    return pd.DataFrame({
        "repayment_id": make_uuids(n, rng),
        "timestamp": iso(random_timestamps(day, n, rng)),
        "loan_account_id": rows["account_id"],
        "customer_id": rows["customer_id"],
        "country_code": country,
        "amount_due": amount_due,
        "amount_paid": amount_paid,
        "currency": currency,
        "due_date": np.datetime_as_string(due_date.astype("datetime64[D]")),
        "payment_date": payment_date,
        "days_overdue": days_overdue,
        "loan_type": loan_type,
        "repayment_status": status,
        "entity_type": rows["entity_type"],
    })


def _calibrate_loss_ratio(batches: list[tuple[str, date, pd.DataFrame]], rng: np.random.Generator) -> None:
    """Ajuste les sinistres payés pour que le loss ratio (pays, mois) tombe près de la cible.

    Sans ce recalage, l'aléa d'échantillonnage sur de petits volumes produit des
    ratios irréalistes (> 100 %) ; l'énoncé demande 50 % – 85 % selon les pays.
    """
    groups: dict[tuple[str, str], list[pd.DataFrame]] = {}
    for country, day, df in batches:
        groups.setdefault((country, f"{day:%Y-%m}"), []).append(df)
    for (country, _), frames in groups.items():
        premiums = sum(f.loc[f["operation_type"].isin(["PREMIUM_PAYMENT", "POLICY_RENEWAL"]), "amount"].sum() for f in frames)
        claims = sum(f.loc[f["operation_type"].eq("CLAIM_PAYMENT"), "amount"].sum() for f in frames)
        if premiums <= 0 or claims <= 0:
            continue
        factor = cfg.TARGET_LOSS_RATIO[country] * rng.uniform(0.95, 1.05) * premiums / claims
        decimals = 2 if cfg.CURRENCY_MAP[country] == "GHS" else 0
        for f in frames:
            mask = f["operation_type"].eq("CLAIM_PAYMENT")
            f.loc[mask, "amount"] = (f.loc[mask, "amount"] * factor).round(decimals)


GENERATORS = {
    "bank_transactions": gen_bank_transactions,
    "insurance_operations": gen_insurance_operations,
    "mobile_money_payments": gen_mobile_money_payments,
    "loan_repayments": gen_loan_repayments,
}


# ---------------------------------------------------------------------------
# Orchestration : répartition des lignes par pays et par jour
# ---------------------------------------------------------------------------
def eligible_countries(dataset: str, countries: list[str], entity_types: list[str]) -> list[str]:
    """Pays où au moins une entité sélectionnée produit ce type de données."""
    entities = DATASET_ENTITIES[dataset] & set(entity_types)
    return [c for c in countries if any(c in cfg.ENTITY_COUNTRIES[e] for e in entities)]


def generate_dataset(
    dataset: str,
    idx: RefIndex,
    n_rows: int,
    countries: list[str],
    entity_types: list[str],
    start: date,
    end: date,
    rng: np.random.Generator,
) -> list[tuple[str, date, pd.DataFrame]]:
    """Retourne une liste de lots (pays, jour, DataFrame) — un fichier par lot."""
    targets = eligible_countries(dataset, countries, entity_types)
    if not targets or n_rows <= 0:
        return []
    weights = np.array([cfg.COUNTRY_WEIGHTS[c] for c in targets])
    per_country = rng.multinomial(n_rows, weights / weights.sum())
    n_days = (end - start).days + 1
    gen = GENERATORS[dataset]
    allowed_entities = DATASET_ENTITIES[dataset] & set(entity_types)

    batches: list[tuple[str, date, pd.DataFrame]] = []
    for country, count in zip(targets, per_country, strict=True):
        per_day = rng.multinomial(count, np.full(n_days, 1 / n_days))
        for offset in np.flatnonzero(per_day):
            day = start + timedelta(days=int(offset))
            df = gen(country, day, int(per_day[offset]), idx, rng)
            df = df[df["entity_type"].isin(allowed_entities)]  # filtre ligne métier
            if not df.empty:
                batches.append((country, day, df.reset_index(drop=True)))
    if dataset == "insurance_operations":
        _calibrate_loss_ratio(batches, rng)
    log.info("dataset généré", extra={"ctx": {
        "dataset": dataset, "rows": sum(len(b[2]) for b in batches), "files": len(batches),
    }})
    return batches
