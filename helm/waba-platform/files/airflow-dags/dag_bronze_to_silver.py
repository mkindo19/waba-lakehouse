"""dag_bronze_to_silver — nettoyage, enrichissement et consolidation multi-pays.

Déclenché par dag_ingest_raw (ou manuellement avec {"country_code": "SN"} pour un backfill).

    silver_referentials ──► silver_bank_transactions ─┐
                        ├─► silver_insurance_operations ├─► trigger_silver_to_gold
                        ├─► silver_mobile_money ────────┤
                        └─► silver_loan_repayments ─────┘
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from waba_common import COUNTRY_ARG, COUNTRY_PARAM, DEFAULT_ARGS, spark_task

# Taux GHS -> EUR : Airflow Variable (modifiable sans toucher au code)
GHS_PER_EUR = "{{ var.value.get('waba_ghs_per_eur', '14.6') }}"

with DAG(
    dag_id="dag_bronze_to_silver",
    description="bronze.* -> silver.* : dédoublonnage, jointures référentiels, conversion EUR",
    schedule=None,                       # déclenché par dag_ingest_raw
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=COUNTRY_PARAM,
    tags=["waba", "niveau-2", "silver"],
) as dag:

    def silver(domain: str):
        return spark_task(f"silver_{domain}", "bronze_to_silver.py",
                          ["--domain", domain, "--country", COUNTRY_ARG, "--ghs-per-eur", GHS_PER_EUR])

    referentials = silver("referentials")
    flows = [silver(d) for d in ("bank_transactions", "insurance_operations",
                                 "mobile_money_payments", "loan_repayments")]

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="dag_silver_to_gold",
        conf={"country_code": COUNTRY_ARG},
        wait_for_completion=False,
    )

    referentials >> flows >> trigger_gold
