"""Briques communes aux DAGs WABA : arguments par défaut, alertes, tâches Spark, paramètres.

Bonnes pratiques appliquées :
- aucun identifiant dans le code : le cluster Spark est une *Connection* Airflow
  (`spark_default`, définie par variable d'environnement), les paramètres métier
  sont des *Variables* Airflow (`waba_*`) ;
- retries avec backoff + callback d'alerte structuré (JSON) en cas d'échec ;
- toutes les tâches Spark partagent le pool `spark_pool` (1 slot) : un seul job à la
  fois sur le worker, pour ne pas saturer la mémoire (cf. piège exit code 52) ;
- chaque DAG est paramétrable par pays (`country_code`) pour un backfill sélectif ;
- même DAGs en Docker Compose et sur Kubernetes : seule la variable WABA_SPARK_MODE change la façon
  de lancer Spark (spark-submit vers le cluster standalone, ou SparkApplication via Spark Operator).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta

from airflow.models.baseoperator import BaseOperator
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
SPARK_JOBS_DIR = os.environ.get("WABA_SPARK_JOBS_DIR", "/opt/airflow/spark-jobs")
SPARK_DRIVER_HOST = os.environ.get("WABA_SPARK_DRIVER_HOST", "airflow-scheduler")
SPARK_POOL = "spark_pool"
SPARK_MODE = os.environ.get("WABA_SPARK_MODE", "standalone")    # standalone (Compose) | k8s (niveau 4)

log = logging.getLogger("waba.alerts")


def alert_on_failure(context: dict) -> None:
    """Alerte d'échec : ligne JSON exploitable par Loki/Grafana (niveau 4).

    En production, on brancherait ici Slack / Teams / e-mail (via une Connection).
    """
    ti = context["task_instance"]
    log.error(json.dumps({
        "alert": "AIRFLOW_TASK_FAILED",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "country_code": (context.get("params") or {}).get("country_code"),
        "log_url": ti.log_url,
        "exception": str(context.get("exception"))[:500],
    }, ensure_ascii=False))


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": alert_on_failure,
}

COUNTRY_PARAM = {
    "country_code": Param(
        "ALL",
        type="string",
        enum=["ALL", *COUNTRIES],
        description="Pays à traiter (ALL = les 8 pays). Permet un backfill sélectif.",
    )
}

COUNTRY_ARG = "{{ params.country_code }}"


def spark_task(task_id: str, script: str, args: list[str] | None = None, **kwargs) -> BaseOperator:
    """Tâche Spark exécutant spark/jobs/<script>.

    - k8s        : SparkApplication créée dans le namespace processing (Spark Operator), cf. waba_sparkapp ;
    - standalone : spark-submit vers le cluster Spark de Docker Compose (driver dans le container Airflow).
    """
    if SPARK_MODE == "k8s":
        from waba_sparkapp import SparkApplicationOperator

        return SparkApplicationOperator(task_id=task_id, script=script, arguments=args or [], pool=SPARK_POOL,
                                        **kwargs)
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SPARK_JOBS_DIR}/{script}",
        application_args=args or [],
        conn_id="spark_default",
        name=f"airflow_{task_id}",
        pool=SPARK_POOL,
        conf={
            # Les executors (container spark-worker) rappellent le driver par ce nom DNS
            "spark.driver.host": SPARK_DRIVER_HOST,
            "spark.driver.bindAddress": "0.0.0.0",
        },
        # `common.*` (modules partagés des jobs) doit être importable par le driver
        env_vars={"PYTHONPATH": SPARK_JOBS_DIR},
        verbose=False,
        **kwargs,
    )
