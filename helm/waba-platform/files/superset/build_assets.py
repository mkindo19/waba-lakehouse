#!/usr/bin/env python3
"""Niveau 4.2 — Dashboards Superset « as code » : génère le paquet d'import (ZIP) des 3 dashboards WABA.

    python build_assets.py <zip de sortie> [--db-uri URI]

Le ZIP suit le format d'export/import v1 de Superset (fichiers YAML ; le JSON étant du YAML valide, on
écrit du JSON sans dépendance) :

    waba_dashboards/metadata.yaml
    waba_dashboards/databases/trino_waba.yaml           connexion SQLAlchemy -> Trino (catalogue iceberg)
    waba_dashboards/datasets/trino_waba/<nom>.yaml      jeux de données (tables gold/silver ou requêtes SQL)
    waba_dashboards/charts/<nom>.yaml                   graphiques
    waba_dashboards/dashboards/<nom>.yaml               mise en page + filtre natif « Pays »

Importé au démarrage de Superset par import_assets.py (type « assets » : TOUT est écrasé, base, jeux de
données, graphiques et dashboards ; `superset import-dashboards` ne met pas à jour les graphiques déjà
présents) : modifier un graphique = modifier ce fichier puis `make k8s-install`. Identifiants stables (uuid5) : un ré-import met à jour les objets
existants au lieu d'en créer de nouveaux.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
import zipfile
from datetime import datetime, timezone

NS = uuid.UUID("6f1c4f0e-2b7a-4c1e-9d4e-57aba0000000")
ROOT = "waba_dashboards"
DB_NAME = "Trino WABA (lakehouse)"
DEFAULT_DB_URI = "trino://superset@trino.serving.svc.cluster.local:8080/iceberg"
VERSION = "1.0.0"


def uid(kind: str, name: str) -> str:
    return str(uuid.uuid5(NS, f"{kind}:{name}"))


# ============================================================================ jeux de données
def col(name: str, type_: str, dttm: bool = False, label: str | None = None) -> dict:
    return {"column_name": name, "verbose_name": label, "is_dttm": dttm, "is_active": True, "type": type_,
            "groupby": True, "filterable": True, "expression": None, "description": None,
            "python_date_format": None, "extra": None}


def metric(name: str, expression: str, label: str, d3format: str | None = None) -> dict:
    return {"metric_name": name, "verbose_name": label, "metric_type": None, "expression": expression,
            "description": None, "d3format": d3format, "extra": None, "warning_text": None}


COUNTRY = col("country_code", "VARCHAR", label="Pays")
COUNT = metric("count", "COUNT(*)", "Nombre")

DATASETS: dict[str, dict] = {
    # --- D1 Performance commerciale -------------------------------------------------------------------
    # Revenu = commissions bancaires + intérêts des prêts sains + primes d'assurance + frais mobile money
    # (mêmes composantes que le KPI ARPC de gold.customer_arpu_monthly, plus les primes d'assurance).
    "revenue_by_business_line": {
        "schema": "silver",
        "sql": """
SELECT country_code, txn_month AS period_month, 'Banque' AS business_line, fee_eur AS revenue_eur
FROM silver.bank_transactions WHERE is_success
UNION ALL
SELECT country_code, txn_month, 'Banque', ROUND(outstanding_eur * interest_rate / 12, 2)
FROM silver.loan_repayments WHERE repayment_status <> 'DEFAULT'
UNION ALL
SELECT country_code, txn_month, 'Assurance', amount_eur
FROM silver.insurance_operations WHERE is_premium
UNION ALL
SELECT country_code, txn_month, 'Mobile Money', fee_eur
FROM silver.mobile_money_payments WHERE is_success""",
        "dttm": "period_month",
        "columns": [COUNTRY, col("period_month", "DATE", True, "Mois"), col("business_line", "VARCHAR", label="Métier"),
                    col("revenue_eur", "DOUBLE", label="Revenu (EUR)")],
        "metrics": [metric("revenue", "SUM(revenue_eur)", "Revenu (EUR)", ",.0f")],
        "description": "Revenu par pays, mois et métier (banque, assurance, mobile money), en EUR.",
    },
    "customer_arpu_monthly": {
        "schema": "gold", "table": "customer_arpu_monthly", "dttm": "period_month",
        "columns": [COUNTRY, col("period_month", "DATE", True, "Mois"), col("segment", "VARCHAR", label="Segment"),
                    col("commission_eur", "DOUBLE"), col("interest_eur", "DOUBLE"),
                    col("active_customers", "BIGINT", label="Clients actifs"),
                    col("total_revenue_eur", "DOUBLE"), col("arpc_eur", "DOUBLE")],
        # ARPC agrégé = revenu total / clients actifs (et non moyenne des ARPC des segments)
        "metrics": [metric("arpc", "SUM(total_revenue_eur) / NULLIF(SUM(active_customers), 0)", "ARPC (EUR)", ",.2f")],
        "description": "KPI gold : revenu moyen par client actif, par mois, pays et segment.",
    },
    "top_products_by_country": {
        "schema": "silver",
        "sql": """
SELECT country_code, product_name, txn_count, amount_eur,
       ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY amount_eur DESC) AS product_rank
FROM (
  SELECT t.country_code, COALESCE(a.product_name, 'Produit inconnu') AS product_name,
         COUNT(*) AS txn_count, ROUND(SUM(t.amount_eur), 2) AS amount_eur
  FROM silver.bank_transactions t
  LEFT JOIN silver.accounts a ON t.account_id = a.account_id
  WHERE t.is_success
  GROUP BY 1, 2
) p""",
        "dttm": None,
        "columns": [COUNTRY, col("product_name", "VARCHAR", label="Produit"),
                    col("txn_count", "BIGINT", label="Transactions"), col("amount_eur", "DOUBLE", label="Montant (EUR)"),
                    col("product_rank", "BIGINT", label="Rang")],
        "metrics": [COUNT],
        "description": "Produits bancaires classés par montant des transactions réussies, dans chaque pays.",
    },
    # --- D2 Risque & Conformité -----------------------------------------------------------------------
    "npl_latest_by_country": {
        "schema": "gold",
        "sql": """
SELECT country_code, period_month, npl_ratio,
       CASE WHEN npl_ratio < 0.03 THEN '< 3 %'
            WHEN npl_ratio <= 0.05 THEN '3 - 5 %'
            ELSE '> 5 % (seuil BCEAO)' END AS npl_band
FROM (
  SELECT country_code, period_month, npl_ratio,
         ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY period_month DESC) AS rn
  FROM gold.npl_ratio_by_country
  WHERE loan_type = 'ALL'
) x
WHERE rn = 1""",
        "dttm": None,
        "columns": [COUNTRY, col("period_month", "DATE", label="Mois"), col("npl_ratio", "DOUBLE", label="Taux NPL"),
                    col("npl_band", "VARCHAR", label="Niveau de risque")],
        "metrics": [metric("npl", "MAX(npl_ratio)", "Taux NPL", ".1%")],
        "description": "Dernier taux de créances douteuses connu par pays, classé par rapport au seuil BCEAO.",
    },
    "loss_ratio_by_product": {
        "schema": "gold", "table": "loss_ratio_by_product", "dttm": "period_month",
        "columns": [COUNTRY, col("period_month", "DATE", True, "Mois"), col("product_line", "VARCHAR", label="Produit"),
                    col("premiums_eur", "DOUBLE"), col("claims_paid_eur", "DOUBLE"),
                    col("claims_paid_count", "BIGINT"), col("loss_ratio", "DOUBLE"),
                    col("cima_alert_threshold", "DOUBLE"), col("is_above_threshold", "BOOLEAN")],
        "metrics": [metric("loss_ratio", "SUM(claims_paid_eur) / NULLIF(SUM(premiums_eur), 0)", "Loss ratio", ".0%"),
                    metric("cima_threshold", "MAX(cima_alert_threshold)", "Seuil CIMA (70 %)", ".0%")],
        "description": "KPI gold : sinistres payés / primes acquises, par mois, pays et ligne produit.",
    },
    "aml_events": {
        "schema": "gold",
        # 30 derniers jours = fin du trimestre batch (silver) + événements détectés en temps réel (job 2)
        "sql": """
SELECT country_code, event_time, 'Temps réel' AS source, amount_eur
FROM gold.rt_aml_events
UNION ALL
SELECT country_code, "timestamp" AS event_time, 'Batch' AS source, amount_eur
FROM silver.bank_transactions
WHERE is_aml_candidate AND is_success""",
        "dttm": "event_time",
        "columns": [COUNTRY, col("event_time", "TIMESTAMP", True, "Date"), col("source", "VARCHAR", label="Source"),
                    col("amount_eur", "DOUBLE", label="Montant (EUR)")],
        "metrics": [metric("aml_count", "COUNT(*)", "Alertes AML")],
        "description": "Virements au-dessus du seuil déclaratif (BCEAO 1 M XOF, Ghana 5 000 GHS).",
    },
    "claims_processing_time": {
        "schema": "gold", "table": "claims_processing_time", "dttm": "period_month",
        "columns": [COUNTRY, col("period_month", "DATE", True, "Mois"), col("line_group", "VARCHAR", label="Branche"),
                    col("sla_days", "INTEGER"), col("claims_count", "BIGINT"), col("avg_processing_days", "DOUBLE"),
                    col("p50_processing_days", "INTEGER"), col("p90_processing_days", "INTEGER"),
                    col("within_sla_rate", "DOUBLE")],
        "metrics": [
            metric("avg_delay", "SUM(avg_processing_days * claims_count) / NULLIF(SUM(claims_count), 0)",
                   "Délai moyen (jours)", ".1f"),
            metric("sla", "MAX(sla_days)", "SLA (jours)", ".0f"),
            metric("within_sla", "SUM(within_sla_rate * claims_count) / NULLIF(SUM(claims_count), 0)",
                   "Traités dans le SLA", ".0%"),
        ],
        "description": "KPI gold : délai de traitement des sinistres (jours ouvrés) et respect du SLA.",
    },
    # --- D3 Mobile Money ------------------------------------------------------------------------------
    "mobile_money_payments": {
        "schema": "silver", "table": "mobile_money_payments", "dttm": "timestamp",
        "columns": [COUNTRY, col("timestamp", "TIMESTAMP", True, "Date"), col("payment_hour", "INTEGER", label="Heure"),
                    # libellé « 00h » .. « 23h » : la heatmap affiche 0 comme une valeur vide
                    {**col("hour_label", "VARCHAR", label="Heure"),
                     "expression": "LPAD(CAST(payment_hour AS VARCHAR), 2, '0') || 'h'"},
                    col("operator", "VARCHAR", label="Opérateur"), col("status", "VARCHAR", label="Statut"),
                    col("payment_type", "VARCHAR", label="Type"), col("amount_eur", "DOUBLE", label="Montant (EUR)")],
        "metrics": [metric("payments", "COUNT(*)", "Paiements", ",d"),
                    metric("failure_rate", "AVG(CASE WHEN status = 'FAILED' THEN 1.0 ELSE 0.0 END)",
                           "Taux d'échec", ".1%")],
        "description": "Paiements mobile money (silver) : heure, opérateur, statut. Aucune donnée personnelle exposée.",
    },
    "cross_border_transfers": {
        "schema": "gold", "table": "cross_border_transfers", "dttm": "week_start",
        "columns": [COUNTRY, col("week_start", "DATE", True, "Semaine"), col("corridor", "VARCHAR", label="Corridor"),
                    col("sender_country", "VARCHAR"), col("receiver_country", "VARCHAR"),
                    col("transfers_count", "BIGINT", label="Transferts"),
                    col("total_amount_eur", "DOUBLE", label="Montant (EUR)"), col("is_uemoa_corridor", "BOOLEAN")],
        "metrics": [metric("corridor_amount", "SUM(total_amount_eur)", "Montant transféré (EUR)", ",.0f")],
        "description": "KPI gold : transferts transfrontaliers réussis par semaine et corridor (pays émetteur = country_code).",
    },
}


def dataset_yaml(name: str, d: dict) -> dict:
    return {
        "table_name": d.get("table", name), "main_dttm_col": d.get("dttm"), "description": d["description"],
        "default_endpoint": None, "offset": 0, "cache_timeout": None, "schema": d["schema"],
        "sql": d.get("sql", "").strip() or None, "params": None, "template_params": None,
        "filter_select_enabled": True, "fetch_values_predicate": None, "extra": None,
        "normalize_columns": False, "always_filter_main_dttm": False,
        "uuid": uid("dataset", name), "metrics": d["metrics"], "columns": d["columns"],
        "version": VERSION, "database_uuid": uid("database", "trino"),
    }


# ============================================================================ graphiques
NO_FILTER = "No filter"
LAST_30_DAYS = 'DATEADD(DATETIME("now"), -30, day) : now'


def time_filter(column: str | None, comparator: str = NO_FILTER) -> list[dict]:
    if not column:
        return []
    return [{"clause": "WHERE", "subject": column, "operator": "TEMPORAL_RANGE", "comparator": comparator,
             "expressionType": "SIMPLE"}]


def sql_filter(expr: str) -> dict:
    return {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": expr, "subject": None,
            "operator": None, "comparator": None}


def bar(ds: str, x: str, metrics: list[str], groupby: list[str] | None = None, *, stack: bool = False,
        horizontal: bool = False, y_format: str = "SMART_NUMBER", filters: list[dict] | None = None,
        row_limit: int = 10000, sort_by_metric: bool = False, temporal_x: bool = False, show_value: bool = False):
    p = {
        "viz_type": "echarts_timeseries_bar", "x_axis": x, "metrics": metrics, "groupby": groupby or [],
        "adhoc_filters": time_filter(DATASETS[ds]["dttm"]) + (filters or []),
        "order_desc": True, "row_limit": row_limit, "truncate_metric": True, "show_empty_columns": True,
        "comparison_type": "values", "annotation_layers": [], "orientation": "horizontal" if horizontal else "vertical",
        "x_axis_title_margin": 15, "y_axis_title_margin": 15, "y_axis_title_position": "Left",
        "sort_series_type": "sum", "color_scheme": "supersetColors", "only_total": True, "show_legend": True,
        "legendType": "scroll", "legendOrientation": "top", "x_axis_time_format": "smart_date",
        "y_axis_format": y_format, "truncateXAxis": True, "y_axis_bounds": [None, None], "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date", "stack": "Stack" if stack else None, "show_value": show_value,
        "extra_form_data": {},
    }
    if temporal_x:
        p["time_grain_sqla"] = "P1D"
    if sort_by_metric:                      # axe catégoriel trié par valeur décroissante (top N)
        p.update({"x_axis_sort": metrics[0], "x_axis_sort_asc": False, "timeseries_limit_metric": metrics[0]})
    else:
        p.update({"x_axis_sort_asc": True})
    return p


def line(ds: str, x: str, metrics: list[str], groupby: list[str], grain: str, y_format: str):
    return {
        "viz_type": "echarts_timeseries_line", "x_axis": x, "time_grain_sqla": grain, "metrics": metrics,
        "groupby": groupby, "adhoc_filters": time_filter(DATASETS[ds]["dttm"]), "order_desc": True,
        "row_limit": 10000, "truncate_metric": True, "show_empty_columns": True, "comparison_type": "values",
        "annotation_layers": [], "x_axis_title_margin": 15, "y_axis_title_margin": 15,
        "y_axis_title_position": "Left", "sort_series_type": "sum", "color_scheme": "supersetColors",
        "seriesType": "line", "markerEnabled": True, "markerSize": 6, "show_legend": True, "legendType": "scroll",
        "legendOrientation": "top", "x_axis_time_format": "%b %Y", "y_axis_format": y_format,
        "truncateXAxis": True, "y_axis_bounds": [None, None], "rich_tooltip": True, "tooltipTimeFormat": "%b %Y",
        "extra_form_data": {},
    }


def mixed(ds: str, x: str, metrics_a: list[str], groupby_a: list[str], metrics_b: list[str],
          groupby_b: list[str], y_format: str, filters: list[dict] | None = None):
    """Barres (requête A) + ligne de référence (requête B : seuil, SLA) sur le même axe."""
    f = time_filter(DATASETS[ds]["dttm"]) + (filters or [])
    return {
        "viz_type": "mixed_timeseries", "x_axis": x, "metrics": metrics_a, "groupby": groupby_a,
        "adhoc_filters": f, "order_desc": True, "row_limit": 10000, "truncate_metric": True,
        "comparison_type": "values", "metrics_b": metrics_b, "groupby_b": groupby_b, "adhoc_filters_b": f,
        "order_desc_b": True, "row_limit_b": 10000, "truncate_metric_b": True, "comparison_type_b": "values",
        "annotation_layers": [], "x_axis_title_margin": 15, "y_axis_title_margin": 15,
        "y_axis_title_position": "Left", "color_scheme": "supersetColors",
        "seriesType": "bar", "opacity": 0.8, "markerSize": 6, "yAxisIndex": 0, "stack": None,
        "seriesTypeB": "line", "opacityB": 1, "markerEnabledB": True, "markerSizeB": 8, "yAxisIndexB": 0,
        "stackB": None, "show_legend": True, "legendType": "scroll", "legendOrientation": "top",
        "x_axis_time_format": "smart_date", "rich_tooltip": True, "tooltipTimeFormat": "smart_date",
        "truncateXAxis": True, "y_axis_bounds": [None, None], "y_axis_format": y_format,
        "y_axis_bounds_secondary": [None, None], "y_axis_format_secondary": y_format, "extra_form_data": {},
    }


CHARTS: dict[str, dict] = {
    # --- D1 -------------------------------------------------------------------------------------------
    "d1_revenue_country_line": {
        "title": "Revenu par pays et métier (EUR)", "ds": "revenue_by_business_line",
        "params": bar("revenue_by_business_line", "country_code", ["revenue"], ["business_line"], y_format=",.0f"),
    },
    "d1_arpc_monthly": {
        "title": "ARPC mensuel par pays (EUR / client actif)", "ds": "customer_arpu_monthly",
        "params": line("customer_arpu_monthly", "period_month", ["arpc"], ["country_code"], "P1M", ",.2f"),
    },
    "d1_revenue_map": {
        "title": "Carte du revenu - Afrique de l'Ouest", "ds": "revenue_by_business_line",
        "params": {
            "viz_type": "world_map", "entity": "country_code", "country_fieldtype": "cca2", "metric": "revenue",
            "secondary_metric": None, "adhoc_filters": time_filter("period_month"), "row_limit": 10000,
            "show_bubbles": False, "max_bubble_size": "25", "color_by": "metric",
            "linear_color_scheme": "superset_seq_1", "color_scheme": "supersetColors",
            "y_axis_format": ",.0f", "extra_form_data": {},
        },
    },
    "d1_top_products": {
        "title": "Top 10 produits par pays (montant des transactions)", "ds": "top_products_by_country",
        "params": {
            "viz_type": "table", "query_mode": "raw", "groupby": [], "metrics": [], "percent_metrics": [],
            "all_columns": ["country_code", "product_rank", "product_name", "amount_eur", "txn_count"],
            "order_by_cols": [json.dumps(["country_code", True]), json.dumps(["product_rank", True])],
            "adhoc_filters": [sql_filter("product_rank <= 10")], "row_limit": 1000, "server_page_length": 10,
            "order_desc": True, "table_timestamp_format": "smart_date", "show_cell_bars": True,
            "color_pn": True, "allow_render_html": False, "extra_form_data": {},
            "column_config": {"amount_eur": {"d3NumberFormat": ",.0f"}},
        },
    },
    # --- D2 -------------------------------------------------------------------------------------------
    "d2_npl_by_country": {
        "title": "Taux NPL par pays (dernier mois) - seuil BCEAO 5 %", "ds": "npl_latest_by_country",
        "params": bar("npl_latest_by_country", "country_code", ["npl"], ["npl_band"], stack=True, y_format=".1%",
                      show_value=True),
    },
    "d2_loss_ratio": {
        "title": "Loss ratio par produit et pays - seuil CIMA 70 %", "ds": "loss_ratio_by_product",
        "params": mixed("loss_ratio_by_product", "product_line", ["loss_ratio"], ["country_code"],
                        ["cima_threshold"], [], ".0%", [sql_filter("product_line <> 'ALL'")]),
    },
    "d2_aml_daily": {
        "title": "Alertes AML par jour et par pays (30 derniers jours)", "ds": "aml_events",
        "params": {**bar("aml_events", "event_time", ["aml_count"], ["country_code"], stack=True, temporal_x=True),
                   "adhoc_filters": time_filter("event_time", LAST_30_DAYS)},
    },
    "d2_claims_vs_sla": {
        "title": "Délai de traitement des sinistres vs SLA (jours ouvrés)", "ds": "claims_processing_time",
        "params": mixed("claims_processing_time", "country_code", ["avg_delay"], ["line_group"], ["sla"],
                        ["line_group"], ".1f"),
    },
    "d2_claims_within_sla": {
        "title": "Part des sinistres traités dans le SLA", "ds": "claims_processing_time",
        "params": bar("claims_processing_time", "country_code", ["within_sla"], ["line_group"], y_format=".0%"),
    },
    # --- D3 -------------------------------------------------------------------------------------------
    "d3_heatmap_hour": {
        "title": "Paiements mobile money par pays et heure", "ds": "mobile_money_payments",
        "params": {
            "viz_type": "heatmap_v2", "x_axis": "hour_label", "groupby": "country_code", "metric": "payments",
            "adhoc_filters": time_filter("timestamp"), "row_limit": 10000, "sort_x_axis": "alpha_asc",
            "sort_y_axis": "alpha_asc", "normalize_across": "heatmap", "legend_type": "continuous",
            "linear_color_scheme": "superset_seq_1", "xscale_interval": 1, "yscale_interval": 1,
            "left_margin": "auto", "bottom_margin": "auto", "value_bounds": [None, None],
            "y_axis_format": "SMART_NUMBER", "show_legend": True, "show_percentage": False, "show_values": True,
            "extra_form_data": {},
        },
    },
    "d3_top_corridors": {
        "title": "Top 5 corridors transfrontaliers (montant EUR)", "ds": "cross_border_transfers",
        "params": bar("cross_border_transfers", "corridor", ["corridor_amount"], [], horizontal=True,
                      y_format=",.0f", row_limit=5, sort_by_metric=True, show_value=True),
    },
    "d3_failure_rate": {
        "title": "Taux d'échec par opérateur et pays", "ds": "mobile_money_payments",
        "params": bar("mobile_money_payments", "country_code", ["failure_rate"], ["operator"], y_format=".1%"),
    },
}


def chart_yaml(name: str, c: dict) -> dict:
    params = {"datasource": "1__table", **c["params"], "dashboards": []}
    return {"slice_name": c["title"], "description": None, "certified_by": None, "certification_details": None,
            "viz_type": params["viz_type"], "params": params, "query_context": None, "cache_timeout": None,
            "uuid": uid("chart", name), "version": VERSION, "dataset_uuid": uid("dataset", c["ds"])}


# ============================================================================ dashboards
DASHBOARDS: dict[str, dict] = {
    "d1_performance_commerciale": {
        "title": "WABA - D1 Performance commerciale", "slug": "waba-performance",
        "intro": "**Performance commerciale** - revenu (commissions, intérêts, primes, frais mobile money) en EUR. "
                 "Filtre **Pays** en haut à gauche.",
        "rows": [[("d1_revenue_country_line", 7, 60), ("d1_revenue_map", 5, 60)],
                 [("d1_arpc_monthly", 6, 60), ("d1_top_products", 6, 60)]],
        "filter_ds": "revenue_by_business_line",
        "label_colors": {},
    },
    "d2_risque_conformite": {
        "title": "WABA - D2 Risque & Conformité", "slug": "waba-risque",
        "intro": "**Risque & conformité** - NPL : vert < 3 %, orange 3-5 %, rouge > 5 % (seuil BCEAO). "
                 "Loss ratio : ligne = seuil de vigilance CIMA (70 %). SLA sinistres : IARD 30 j, VIE 45 j.",
        "rows": [[("d2_npl_by_country", 5, 60), ("d2_loss_ratio", 7, 60)],
                 [("d2_aml_daily", 12, 55)],
                 [("d2_claims_vs_sla", 7, 55), ("d2_claims_within_sla", 5, 55)]],
        "filter_ds": "npl_latest_by_country",
        "label_colors": {"< 3 %": "#2E7D32", "3 - 5 %": "#F9A825", "> 5 % (seuil BCEAO)": "#C62828",
                         "Seuil CIMA (70 %)": "#C62828"},
    },
    "d3_mobile_money": {
        "title": "WABA - D3 Mobile Money", "slug": "waba-mobile-money",
        "intro": "**Mobile money** - activité par heure, corridors transfrontaliers (pays émetteur) "
                 "et taux d'échec par opérateur.",
        "rows": [[("d3_heatmap_hour", 12, 60)],
                 [("d3_top_corridors", 5, 55), ("d3_failure_rate", 7, 55)]],
        "filter_ds": "mobile_money_payments",
        "label_colors": {},
    },
}


def dashboard_yaml(name: str, d: dict) -> dict:
    pos: dict = {"DASHBOARD_VERSION_KEY": "v2",
                 "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
                 "HEADER_ID": {"id": "HEADER_ID", "meta": {"text": d["title"]}, "type": "HEADER"},
                 "GRID_ID": {"children": [], "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID"}}
    chart_id = 0

    def add_row(idx: int, children: list[tuple[str, dict]]) -> None:
        row_id = f"ROW-{name}-{idx}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"children": [c for c, _ in children], "id": row_id, "type": "ROW",
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}, "parents": ["ROOT_ID", "GRID_ID"]}
        for cid, node in children:
            pos[cid] = {**node, "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id]}

    add_row(0, [(f"MARKDOWN-{name}", {"type": "MARKDOWN", "meta": {"width": 12, "height": 12,
                                                                   "code": d["intro"]}})])
    for i, row in enumerate(d["rows"], start=1):
        nodes = []
        for chart, width, height in row:
            chart_id += 1
            nodes.append((f"CHART-{chart}", {"type": "CHART", "meta": {
                "chartId": chart_id, "width": width, "height": height, "uuid": uid("chart", chart),
                "sliceName": CHARTS[chart]["title"]}}))
        add_row(i, nodes)

    country_filter = {
        "id": f"NATIVE_FILTER-{name}-pays", "type": "NATIVE_FILTER", "name": "Pays", "filterType": "filter_select",
        "description": "Filtre tous les graphiques du dashboard sur un ou plusieurs pays",
        "targets": [{"column": {"name": "country_code"}, "datasetUuid": uid("dataset", d["filter_ds"])}],
        "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True,
                          "searchAllOptions": False, "inverseSelection": False, "sortAscending": True},
        "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
        "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "tabsInScope": [],
    }
    return {
        "dashboard_title": d["title"], "description": None, "css": "", "slug": d["slug"], "certified_by": "",
        "certification_details": "", "published": True, "uuid": uid("dashboard", name), "position": pos,
        "metadata": {
            "color_scheme": "supersetColors", "label_colors": d["label_colors"], "shared_label_colors": {},
            "color_scheme_domain": [], "refresh_frequency": 0, "expanded_slices": {},
            "timed_refresh_immune_slices": [], "cross_filters_enabled": True,
            "native_filter_configuration": [country_filter], "default_filters": "{}", "filter_scopes": {},
        },
        "version": VERSION,
    }


# ============================================================================ paquet
def build(path: str, db_uri: str) -> None:
    files = {
        "metadata.yaml": {"version": VERSION, "type": "assets",
                          "timestamp": datetime.now(timezone.utc).isoformat()},
        "databases/trino_waba.yaml": {
            "database_name": DB_NAME, "sqlalchemy_uri": db_uri, "cache_timeout": None, "expose_in_sqllab": True,
            "allow_run_async": False, "allow_ctas": False, "allow_cvas": False, "allow_dml": False,
            "allow_file_upload": False, "extra": {"allows_virtual_table_explore": True},
            "uuid": uid("database", "trino"), "version": VERSION,
        },
    }
    for name, d in DATASETS.items():
        files[f"datasets/trino_waba/{name}.yaml"] = dataset_yaml(name, d)
    for name, c in CHARTS.items():
        files[f"charts/{name}.yaml"] = chart_yaml(name, c)
    for name, d in DASHBOARDS.items():
        files[f"dashboards/{name}.yaml"] = dashboard_yaml(name, d)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for rel, content in files.items():
            z.writestr(f"{ROOT}/{rel}", json.dumps(content, ensure_ascii=False, indent=2))
    print(f"{path} : {len(DATASETS)} jeux de données, {len(CHARTS)} graphiques, {len(DASHBOARDS)} dashboards")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("output", help="ZIP à produire")
    p.add_argument("--db-uri", default=DEFAULT_DB_URI, help="URI SQLAlchemy de Trino")
    args = p.parse_args(argv)
    build(args.output, args.db_uri)
    return 0


if __name__ == "__main__":
    sys.exit(main())
