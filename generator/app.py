"""Application Streamlit — génération de données simulées WABA Group (niveau 1.1)."""

from __future__ import annotations

import streamlit as st

from waba_gen import config as cfg
from waba_gen.cli import last_quarter
from waba_gen.engine import ContinuousRunner, load_referentials, publish_referentials, run_one_time
from waba_gen.storage import S3Sink
from waba_gen.transactions import RefIndex

st.set_page_config(page_title="WABA Data Generator", page_icon="🏦", layout="wide")

DATASET_LABELS = {
    "bank_transactions": "Transactions bancaires",
    "insurance_operations": "Opérations d'assurance",
    "mobile_money_payments": "Paiements mobile money",
    "loan_repayments": "Remboursements de crédit",
}


@st.cache_resource
def get_sink() -> S3Sink:
    return S3Sink()


@st.cache_resource(show_spinner="Chargement des référentiels depuis MinIO…")
def get_index(_version: int) -> RefIndex:
    """Index des clés valides ; `_version` force le rechargement après régénération."""
    return RefIndex.build(load_referentials(get_sink()))


@st.cache_resource
def get_state() -> dict:
    """État partagé entre sessions (un seul flux continu à la fois)."""
    return {"runner": None, "ref_version": 0}


state = get_state()
sink = get_sink()

st.title("🏦 WABA Group — Générateur de données")
st.caption("Simulation de l'activité quotidienne multi-pays · dépôt dans MinIO `raw-landing`")

# --- Paramètres communs -------------------------------------------------------
with st.sidebar:
    st.header("Paramètres")
    countries = st.multiselect("Pays", cfg.COUNTRIES, default=cfg.COUNTRIES)
    entities = st.multiselect("Lignes métier", cfg.ENTITY_TYPES, default=cfg.ENTITY_TYPES)
    q_start, q_end = last_quarter()
    period = st.date_input("Période simulée", value=(q_start, q_end))
    seed = st.number_input("Seed (0 = aléatoire)", min_value=0, value=0, step=1)

tab_ref, tab_once, tab_stream = st.tabs(["1️⃣ Référentiels", "2️⃣ Génération one-time", "3️⃣ Génération continue"])

# --- Référentiels ------------------------------------------------------------------
with tab_ref:
    st.markdown(
        "Les référentiels (**customers, accounts, branches, products**) doivent être générés **avant** "
        "les transactions : toutes les clés des transactions sont tirées parmi eux."
    )
    exists = sink.exists("referentials/customers/customers.csv")
    st.info("Référentiels présents dans MinIO ✅" if exists else "Aucun référentiel dans MinIO pour l'instant.")
    scale = st.select_slider(
        "Volume", options=[0.01, 0.1, 0.5, 1.0], value=1.0,
        format_func=lambda s: f"{s:.0%} ({int(cfg.REFERENTIAL_ROWS['customers'] * s):,} clients)",
    )
    if st.button("Générer les référentiels", type="primary"):
        with st.spinner("Génération et envoi vers MinIO…"):
            refs = publish_referentials(sink, seed=42, scale=scale)
        state["ref_version"] += 1
        st.success(
            f"{len(refs.customers):,} clients · {len(refs.accounts):,} comptes · "
            f"{len(refs.branches)} agences · {len(refs.products)} produits"
        )

# --- One-time ------------------------------------------------------------------------
with tab_once:
    datasets = st.multiselect(
        "Types de données", list(DATASET_LABELS), default=list(DATASET_LABELS), format_func=DATASET_LABELS.get,
    )
    cols = st.columns(len(DATASET_LABELS))
    rows = {
        ds: col.number_input(DATASET_LABELS[ds], min_value=0, value=cfg.DEFAULT_ROWS[ds], step=1000)
        for col, ds in zip(cols, DATASET_LABELS, strict=True)
    }
    if st.button("Générer et envoyer vers MinIO", type="primary", disabled=not (datasets and countries and entities)):
        if not isinstance(period, tuple) or len(period) != 2:
            st.error("Choisissez une date de début et une date de fin.")
        else:
            try:
                idx = get_index(state["ref_version"])
                with st.spinner("Génération en cours…"):
                    keys = run_one_time(sink, idx, datasets, rows, countries, entities,
                                        period[0], period[1], seed or None)
                st.success(f"{len(keys)} fichiers déposés dans `raw-landing`.")
                st.dataframe({"fichier": keys[:200]}, use_container_width=True)
            except RuntimeError as exc:
                st.error(str(exc))

# --- Continu ---------------------------------------------------------------------------
with tab_stream:
    st.markdown("Un lot est généré toutes les **10 à 60 secondes** (horodaté aujourd'hui) pour simuler un flux temps réel.")
    s_datasets = st.multiselect(
        "Types de données ", list(DATASET_LABELS), default=list(DATASET_LABELS), format_func=DATASET_LABELS.get,
    )
    per_tick = st.slider("Lignes par lot et par type", 10, 500, 50)
    interval = st.slider("Intervalle (secondes)", 10, 60, (10, 60))
    runner: ContinuousRunner | None = state["runner"]

    c1, c2 = st.columns(2)
    if c1.button("▶️ Démarrer", disabled=bool(runner and runner.is_running)):
        try:
            runner = ContinuousRunner(
                sink, get_index(state["ref_version"]), s_datasets, {ds: per_tick for ds in DATASET_LABELS},
                countries, entities, interval[0], interval[1], seed or None,
            )
            runner.start()
            state["runner"] = runner
        except RuntimeError as exc:
            st.error(str(exc))
    if c2.button("⏹️ Arrêter", disabled=not (runner and runner.is_running)):
        runner.stop()

    if runner:
        status = "🟢 en cours" if runner.is_running else "⚪ arrêté"
        st.metric("Statut", status)
        m1, m2 = st.columns(2)
        m1.metric("Lots générés", runner.ticks)
        m2.metric("Fichiers déposés", runner.files)
        if runner.last_error:
            st.warning(f"Dernière erreur : {runner.last_error}")
        st.button("🔄 Rafraîchir")
