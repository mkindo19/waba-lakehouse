"""dag_ingest_raw — détection de nouveaux fichiers dans MinIO puis chargement jusqu'à Bronze.

Planifié toutes les 10 minutes. Si aucun fichier n'attend dans raw-landing, le run
s'arrête immédiatement (ShortCircuit) : pas de job Spark lancé pour rien.

    check_new_files ──► landing_to_raw ──► raw_to_bronze ──► trigger_bronze_to_silver
"""

from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from waba_common import COUNTRY_ARG, COUNTRY_PARAM, DEFAULT_ARGS, spark_task

RAW_FOLDERS = ["referentials", "bank_transactions", "insurance_operations",
               "mobile_money_payments", "loan_repayments"]


def has_new_files(country_code: str = "ALL") -> bool:
    """True si au moins un CSV attend dans raw-landing (pour le pays demandé)."""
    import boto3  # import local : évite de charger boto3 à chaque parsing du DAG

    s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                      region_name=os.environ.get("AWS_REGION", "us-east-1"))
    bucket = os.environ.get("RAW_BUCKET", "raw-landing")
    for folder in RAW_FOLDERS:
        prefix = f"{folder}/" if folder == "referentials" or country_code == "ALL" else f"{folder}/{country_code}/"
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) > 0:
            print(f"Nouveaux fichiers détectés sous {bucket}/{prefix}")
            return True
    print("Aucun fichier en attente : run court-circuité")
    return False


with DAG(
    dag_id="dag_ingest_raw",
    description="raw-landing (MinIO) -> raw.* -> bronze.* pour tous les pays",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=COUNTRY_PARAM,
    tags=["waba", "niveau-2", "bronze"],
) as dag:
    check_new_files = ShortCircuitOperator(
        task_id="check_new_files",
        python_callable=has_new_files,
        op_kwargs={"country_code": COUNTRY_ARG},
    )

    landing_to_raw = spark_task("landing_to_raw", "ingest_raw.py", ["--dataset", "all", "--country", COUNTRY_ARG])

    raw_to_bronze = spark_task("raw_to_bronze", "raw_to_bronze.py", ["--dataset", "all", "--country", COUNTRY_ARG])

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="dag_bronze_to_silver",
        conf={"country_code": COUNTRY_ARG},
        wait_for_completion=False,
    )

    check_new_files >> landing_to_raw >> raw_to_bronze >> trigger_silver
