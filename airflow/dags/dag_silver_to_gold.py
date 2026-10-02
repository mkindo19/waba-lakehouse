"""dag_silver_to_gold — calcul des 7 KPIs financiers et réglementaires.

Déclenché par dag_bronze_to_silver (ou manuellement, paramétrable par pays).

    gold_banking       : daily_transaction_volume, npl_ratio_by_country, customer_arpu_monthly
    gold_insurance     : loss_ratio_by_product, claims_processing_time
    gold_mobile_money  : mobile_money_daily_flow, cross_border_transfers
"""

from __future__ import annotations

import pendulum
from airflow import DAG

from waba_common import COUNTRY_ARG, COUNTRY_PARAM, DEFAULT_ARGS, spark_task

with DAG(
    dag_id="dag_silver_to_gold",
    description="silver.* -> gold.* : KPIs bancaires, assurance et mobile money",
    schedule=None,                       # déclenché par dag_bronze_to_silver
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=COUNTRY_PARAM,
    tags=["waba", "niveau-2", "gold"],
) as dag:
    gold_banking = spark_task("gold_banking", "silver_to_gold.py", ["--kpi", "banking", "--country", COUNTRY_ARG])
    gold_insurance = spark_task("gold_insurance", "silver_to_gold.py",
                                ["--kpi", "insurance", "--country", COUNTRY_ARG])
    gold_mobile_money = spark_task("gold_mobile_money", "silver_to_gold.py",
                                   ["--kpi", "mobile_money", "--country", COUNTRY_ARG])

    # Indépendants entre eux : le pool spark_pool les exécute l'un après l'autre
    [gold_banking, gold_insurance, gold_mobile_money]  # noqa: B018
