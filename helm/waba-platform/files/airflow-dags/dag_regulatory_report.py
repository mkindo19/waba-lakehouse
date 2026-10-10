"""dag_regulatory_report — agrégats réglementaires BCEAO / CIMA à J+1.

Planifié chaque jour à 00h30 UTC. Le run du jour J couvre la journée J-1 :
Airflow fournit {{ ds }} = début de l'intervalle de données = la veille.

Contrainte de service : le rapport doit être disponible avant 06h00 UTC (SLA de
5h30 après le lancement). Une alerte Grafana surveillera ce point au niveau 4.

    refresh_silver_bank ──► refresh_silver_insurance ──► refresh_gold_npl ──► build_regulatory_report
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG

from waba_common import COUNTRY_ARG, COUNTRY_PARAM, DEFAULT_ARGS, spark_task

with DAG(
    dag_id="dag_regulatory_report",
    description="Rapport réglementaire J-1 (BCEAO / CIMA) : table gold + export CSV dans MinIO",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2026, 9, 25, tz="UTC"),
    catchup=False,                     # backfill explicite : airflow dags backfill -s ... -e ...
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 3},
    params=COUNTRY_PARAM,
    tags=["waba", "niveau-2", "gold", "reglementaire"],
) as dag:
    # Garantit que la veille est bien consolidée avant de produire le rapport
    refresh_silver = spark_task("refresh_silver_bank", "bronze_to_silver.py",
                                ["--domain", "bank_transactions", "--country", COUNTRY_ARG])
    refresh_silver_ins = spark_task("refresh_silver_insurance", "bronze_to_silver.py",
                                    ["--domain", "insurance_operations", "--country", COUNTRY_ARG])
    refresh_gold = spark_task("refresh_gold_npl", "silver_to_gold.py", ["--kpi", "banking", "--country", COUNTRY_ARG])

    report = spark_task(
        "build_regulatory_report", "regulatory_report.py",
        ["--report-date", "{{ ds }}", "--country", COUNTRY_ARG],
        sla=timedelta(hours=5, minutes=30),
    )

    refresh_silver >> refresh_silver_ins >> refresh_gold >> report
