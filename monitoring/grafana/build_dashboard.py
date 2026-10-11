#!/usr/bin/env python3
"""Génère le dashboard Grafana « WABA — santé des pipelines » (niveau 4.4) : dashboards/pipeline-health.json.

Écrire le JSON à la main est illisible (des centaines de lignes de mise en page) : ce script ne décrit que
l'essentiel de chaque panneau (titre, requête, unité, seuil). Après modification :
    python3 monitoring/grafana/build_dashboard.py      (puis make k8s-install : Grafana recharge le JSON)

Les 4 vues exigées par l'énoncé : latence des jobs Spark par pays, retard des consommateurs Kafka (fraude),
débit NiFi, taux d'erreur des DAGs Airflow. Les identifiants de panneaux 2, 3 et 5 sont référencés par les
alertes (provisioning/alerting.yaml, annotation __panelId__) : ne pas les changer.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent / "dashboards" / "pipeline-health.json"
PROM = {"type": "prometheus", "uid": "prometheus"}
LOKI = {"type": "loki", "uid": "loki"}
PG = {"type": "grafana-postgresql-datasource", "uid": "airflow-db"}

REGULATORY_SQL = """WITH clock AS (SELECT (now() AT TIME ZONE 'UTC') AS now_utc),
runs AS (
  SELECT r.state FROM dag_run r, clock c
  WHERE r.dag_id = 'dag_regulatory_report'
    AND (r.data_interval_start AT TIME ZONE 'UTC')::date = c.now_utc::date - 1
)
SELECT now() AS time,
  CASE
    WHEN EXISTS (SELECT 1 FROM runs WHERE state = 'success') THEN 0
    WHEN (SELECT EXTRACT(HOUR FROM now_utc) FROM clock) >= 6 THEN 1
    WHEN EXISTS (SELECT 1 FROM runs WHERE state = 'failed') THEN 1
    ELSE 0
  END AS rapport_j1_manquant"""

LAST_RUNS_SQL = """SELECT dag_id AS "DAG", state AS "état", run_type AS "type",
       (data_interval_start AT TIME ZONE 'UTC')::date AS "données du",
       start_date AS "début", end_date AS "fin",
       round(extract(epoch FROM (coalesce(end_date, now()) - start_date)) / 60.0, 1) AS "durée (min)"
FROM dag_run
ORDER BY start_date DESC NULLS LAST
LIMIT 15"""

SPARK_APPS = 'app=~"stream-.*|spark-batch.*"'


# ----------------------------------------------------------------------------- briques
def prom(expr: str, legend: str = "", ref: str = "A", instant: bool = False) -> dict:
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend or "__auto",
            "range": not instant, "instant": instant, "editorMode": "code"}


def loki(expr: str, legend: str = "", ref: str = "A", kind: str = "range") -> dict:
    return {"refId": ref, "datasource": LOKI, "expr": expr, "legendFormat": legend, "queryType": kind,
            "editorMode": "code"}


def sql(raw: str, fmt: str = "table", ref: str = "A") -> dict:
    return {"refId": ref, "datasource": PG, "rawSql": raw, "rawQuery": True, "format": fmt, "editorMode": "code"}


def thresholds(*steps: tuple[str, float | None]) -> dict:
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}


GREEN_RED = thresholds(("green", None), ("red", 1))


def panel(pid: int, kind: str, title: str, targets: list[dict], pos: tuple[int, int, int, int], *,
          desc: str = "", unit: str = "short", ds: dict | None = None, th: dict | None = None,
          mappings: list | None = None, options: dict | None = None, custom: dict | None = None,
          decimals: int | None = None, maxv: float | None = None) -> dict:
    x, y, w, h = pos
    defaults = {"unit": unit, "thresholds": th or thresholds(("green", None)), "mappings": mappings or []}
    if decimals is not None:
        defaults["decimals"] = decimals
    if maxv is not None:
        defaults["max"] = maxv
    if custom:
        defaults["custom"] = custom
    return {"id": pid, "type": kind, "title": title, "description": desc, "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "datasource": ds or targets[0]["datasource"], "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": []}, "options": options or {}}


def row(pid: int, title: str, y: int) -> dict:
    return {"id": pid, "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
            "panels": []}


def stat(pid, title, target, pos, **kw) -> dict:
    opts = {"colorMode": "background", "graphMode": "none", "textMode": "value", "justifyMode": "center",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}
    return panel(pid, "stat", title, [target], pos, options=opts, **kw)


def timeseries(pid, title, targets, pos, *, threshold_line: float | None = None, **kw) -> dict:
    custom = {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10, "showPoints": "never", "spanNulls": True}
    if threshold_line is not None:
        custom["thresholdsStyle"] = {"mode": "line+area"}
        kw["th"] = thresholds(("transparent", None), ("red", threshold_line))
    opts = {"legend": {"displayMode": "table", "placement": "right", "calcs": ["lastNotNull", "max"]},
            "tooltip": {"mode": "multi", "sort": "desc"}}
    return panel(pid, "timeseries", title, targets, pos, custom=custom, options=opts, **kw)


UP_DOWN = [{"type": "value", "options": {"0": {"text": "ARRÊTÉ", "color": "red"},
                                          "1": {"text": "EN COURS", "color": "green"}}}]
REPORT_STATE = [{"type": "value", "options": {"0": {"text": "OK", "color": "green"},
                                               "1": {"text": "ABSENT", "color": "red"}}}]


# ----------------------------------------------------------------------------- panneaux
def build() -> dict:
    p: list[dict] = []
    # --- synthèse (ids 2, 3, 5 : cibles des alertes)
    p.append(row(1, "Synthèse — état des alertes", 0))
    p.append(stat(2, "Job Spark fraude / AML (driver)",
                  prom('sum(up{app="stream-fraud-aml", spark_role="driver"}) or vector(0)', instant=True),
                  (0, 1, 5, 4), mappings=UP_DOWN, th=thresholds(("red", None), ("green", 1)),
                  desc="Alerte 1 : driver absent ou en échec pendant plus de 5 min."))
    p.append(stat(3, "Retard du consommateur AML (messages)",
                  prom('max(kafka_consumergroup_lag_sum{consumergroup="waba-spark.fraud_aml_aml"}) or vector(0)',
                       instant=True),
                  (5, 1, 5, 4), th=thresholds(("green", None), ("orange", 1000), ("red", 5000)),
                  desc="Alerte 2 : plus de 5000 transactions de silver-bank-transactions non contrôlées."))
    p.append(stat(4, "Job Spark raw → silver (driver)",
                  prom('sum(up{app="stream-raw-to-silver", spark_role="driver"}) or vector(0)', instant=True),
                  (10, 1, 5, 4), mappings=UP_DOWN, th=thresholds(("red", None), ("green", 1))))
    p.append(stat(5, "Rapport réglementaire J-1 (SLA 06h00 UTC)", sql(REGULATORY_SQL, "time_series"),
                  (15, 1, 5, 4), mappings=REPORT_STATE, th=GREEN_RED,
                  desc="Alerte 3 : pas de run réussi de dag_regulatory_report pour la veille après 06h00 UTC "
                       "(ou run déjà en échec). Lu dans la base Airflow (lecture seule)."))
    p.append(stat(6, "Cibles Prometheus en échec",
                  prom("count(up == 0) or vector(0)", instant=True), (20, 1, 4, 4),
                  th=GREEN_RED, desc="Composants dont la collecte de métriques échoue (détail en bas de page)."))

    # --- vue 1 : latence Spark par pays
    p.append(row(10, "Spark — latence des jobs par pays", 5))
    p.append(timeseries(
        11, "Latence du job streaming 1 par pays (Kafka raw → table silver)",
        [loki('max by (country_code) (max_over_time({app="stream-raw-to-silver"} |= "latence par pays" '
              '| json | __error__="" | unwrap lag_s [1m]))', "{{country_code}}")],
        (0, 6, 14, 9), unit="s", ds=LOKI,
        desc="Pire cas par micro-lot : âge du message le plus ancien (dépôt dans Kafka) à la fin de l'écriture "
             "silver. Source : journaux JSON du driver (« latence par pays »), via Promtail et Loki."))
    p.append(timeseries(
        12, "Durée des micro-lots par requête de streaming",
        [loki('max by (query) (max_over_time({app=~"stream-.*"} |= "progression" '
              '| json | __error__="" | unwrap batch_duration_ms [1m]))', "{{query}}")],
        (14, 6, 10, 9), unit="ms", ds=LOKI,
        desc="Émis par StreamMonitor (StreamingQueryListener) à chaque micro-lot."))
    p.append(panel(
        13, "bargauge", "Jobs Spark batch : durée moyenne des tâches Airflow (période affichée)",
        [prom("sum by (dag_id, task_id) (increase(airflow_task_duration_seconds_sum[$__range])) / "
              "sum by (dag_id, task_id) (increase(airflow_task_duration_seconds_count[$__range])) > 0",
              "{{dag_id}} / {{task_id}}", instant=True)],
        (0, 15, 24, 7), unit="s",
        options={"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                 "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
        desc="Chaque tâche lance une SparkApplication (pays : paramètre « country » du DAG)."))

    # --- vue 2 : Kafka
    p.append(row(20, "Kafka — retard des consommateurs (fraude / AML)", 22))
    p.append(timeseries(
        21, "Retard des requêtes du job fraude / AML (messages)",
        [prom('sum by (consumergroup) (kafka_consumergroup_lag_sum{consumergroup=~"waba-spark\\\\.fraud_aml_.*"})',
              "{{consumergroup}}")],
        (0, 23, 14, 9), threshold_line=5000,
        desc="Groupes waba-spark.<requête> : offsets committés par StreamMonitor après chaque micro-lot. "
             "Ligne rouge : seuil de l'alerte AML (5000)."))
    p.append(timeseries(
        22, "Retard du job raw → silver (messages)",
        [prom('sum by (consumergroup) (kafka_consumergroup_lag_sum{consumergroup=~"waba-spark\\\\.raw_to_silver_.*"})',
              "{{consumergroup}}")],
        (14, 23, 10, 9)))

    # --- vue 3 : NiFi
    p.append(row(30, "NiFi — débit d'ingestion (MinIO → Kafka)", 32))
    p.append(timeseries(
        31, "NiFi : fichiers envoyés par processeur (FlowFiles/s, moyenne 5 min)",
        [prom('sum by (component_name) (nifi_amount_flowfiles_sent{component_type="Processor"}) / 300',
              "{{component_name}}")],
        (0, 33, 9, 8), unit="ops",
        desc="Compteurs NiFi « 5 dernières minutes » divisés par 300 s."))
    p.append(timeseries(
        32, "Messages publiés dans les topics raw-* (messages/s)",
        [prom('sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"raw-.*"}[$__rate_interval]))',
              "{{topic}}")],
        (9, 33, 9, 8), unit="ops", desc="Débit NiFi en lignes : 1 ligne de fichier = 1 message Kafka."))
    p.append(timeseries(
        33, "NiFi : FlowFiles en file d'attente",
        [prom('sum(nifi_amount_items_queued{component_type="Connection"})', "en attente")],
        (18, 33, 6, 8)))

    # --- vue 4 : Airflow
    p.append(row(40, "Airflow — taux d'erreur des DAGs", 41))
    p.append(timeseries(
        41, "Taux d'erreur des tâches par DAG (fenêtre glissante 1 h)",
        [prom('sum by (dag_id) (increase(airflow_ti_finish{state="failed"}[1h])) / '
              "sum by (dag_id) (increase(airflow_ti_finish[1h]))", "{{dag_id}}")],
        (0, 42, 12, 9), unit="percentunit", maxv=1,
        desc="Tâches en échec / tâches terminées (métriques StatsD d'Airflow via statsd-exporter)."))
    p.append(panel(
        42, "table", "Derniers DAG runs", [sql(LAST_RUNS_SQL)], (12, 42, 12, 9), ds=PG,
        options={"showHeader": True, "cellHeight": "sm"},
        custom={"cellOptions": {"type": "auto"}, "align": "auto"},
        desc="Lu dans la base Airflow (rôle grafana_reader, lecture seule)."))

    # --- journaux et plateforme
    p.append(row(50, "Journaux et plateforme", 51))
    p.append(panel(
        51, "logs", "Journaux Spark ERROR / WARNING",
        [loki('{' + SPARK_APPS + ', level=~"ERROR|WARNING"}')], (0, 52, 24, 10), ds=LOKI,
        options={"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending", "enableLogDetails": True},
        desc="Lignes JSON des jobs Spark, niveau extrait par Promtail (étiquette level)."))
    p.append(timeseries(
        52, "Trino : requêtes en cours / en file",
        [prom('{__name__=~"trino_execution_name_QueryManager_(RunningQueries|QueuedQueries)"}', "{{__name__}}")],
        (0, 62, 12, 8)))
    p.append(panel(
        53, "table", "Collecte Prometheus par composant (1 = OK)",
        [prom("min by (job, app) (up)", instant=True)], (12, 62, 12, 8),
        options={"showHeader": True}, th=thresholds(("red", None), ("green", 1))))

    return {
        "uid": "waba-pipeline-health",
        "title": "WABA — santé des pipelines",
        "tags": ["waba", "niveau-4", "observabilite"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": False,
        "graphTooltip": 1,
        "refresh": "30s",
        "time": {"from": "now-3h", "to": "now"},
        "annotations": {"list": []},
        "templating": {"list": []},
        "panels": p,
    }


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2, ensure_ascii=False) + "\n")
    print(f"{OUT} : {sum(1 for x in build()['panels'] if x['type'] != 'row')} panneaux")
