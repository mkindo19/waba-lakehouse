"""Opérateur Airflow -> Spark Operator (niveau 4, Kubernetes).

Une tâche Spark = une ressource SparkApplication créée dans le namespace processing :

    Airflow (scheduler)  --crée-->  SparkApplication  --Spark Operator-->  pod driver + pods executors
                         <--lit---  status.applicationState (SUBMITTED, RUNNING, COMPLETED / FAILED)

- Le modèle de SparkApplication (image, catalogue Iceberg, accès S3, ressources, secrets) est rendu par Helm
  et monté dans le pod Airflow (WABA_SPARK_APP_TEMPLATE) : le code des DAGs ne contient aucune
  configuration d'infrastructure, et le batch partage la configuration du streaming (helpers du chart).
- L'opérateur n'ajoute que le script, ses arguments et des étiquettes de traçabilité (DAG, tâche, pays),
  attend la fin, recopie la fin du journal du driver dans le journal Airflow, puis échoue si Spark a échoué.
- Nettoyage : la SparkApplication est supprimée par Spark Operator une heure après la fin (timeToLiveSeconds
  dans le modèle) : on garde le temps d'examiner un échec ; elle est supprimée tout de suite si la tâche est
  interrompue (timeout, arrêt manuel).

Pourquoi pas SparkKubernetesOperator (provider cncf.kubernetes) : il suit le pod driver grâce à des
étiquettes qu'il n'ajoute pas lui-même au modèle ; sans elles, une tâche peut être marquée réussie alors que
le job Spark a échoué (cf. D28). Cet opérateur lit directement l'état calculé par Spark Operator.
"""

from __future__ import annotations

import os
import re
import time
import uuid
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

GROUP, VERSION, PLURAL = "sparkoperator.k8s.io", "v1beta2", "sparkapplications"
TEMPLATE = os.environ.get("WABA_SPARK_APP_TEMPLATE", "/opt/airflow/spark-k8s/sparkapplication.yaml")
NAMESPACE = os.environ.get("WABA_SPARK_NAMESPACE", "processing")
JOBS_URI = os.environ.get("WABA_SPARK_JOBS_URI", "local:///opt/spark-apps")   # code des jobs dans l'image Spark
SUCCEEDED, FAILED = {"COMPLETED"}, {"FAILED", "SUBMISSION_FAILED"}
LOG_TAIL = 120


def _label(value: Any) -> str:
    """Valeur d'étiquette Kubernetes valide (63 caractères, [A-Za-z0-9_.-])."""
    return re.sub(r"[^A-Za-z0-9_.-]", "-", str(value))[:63].strip("-_.") or "none"


class SparkApplicationOperator(BaseOperator):
    """Lance spark/jobs/<script> en SparkApplication et attend sa fin."""

    template_fields = ("arguments",)
    ui_color = "#fde2c4"

    def __init__(self, *, script: str, arguments: list[str] | None = None, poke_interval: int = 10,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.script = script
        self.arguments = list(arguments or [])
        self.poke_interval = poke_interval
        self._app: str | None = None

    # ------------------------------------------------------------------ construction de la ressource
    def app_name(self) -> str:
        """Nom unique et court : le pod driver et ses services en dérivent (63 caractères max)."""
        base = re.sub(r"[^a-z0-9-]+", "-", f"{self.dag_id.removeprefix('dag_')}-{self.task_id}".lower())
        return f"{base[:40].strip('-')}-{uuid.uuid4().hex[:6]}"

    def build_body(self, name: str, context: dict) -> dict:
        import yaml  # import local : le parsing des DAGs reste léger

        with open(TEMPLATE, encoding="utf-8") as f:
            body = yaml.safe_load(f)
        spec = body["spec"]
        spec["mainApplicationFile"] = f"{JOBS_URI}/{self.script}"
        spec["arguments"] = [str(a) for a in self.arguments]
        country = (context.get("params") or {}).get("country_code", "ALL")
        labels = {"waba/dag-id": _label(self.dag_id), "waba/task-id": _label(self.task_id),
                  "waba/country": _label(country)}
        run = {"waba/run-id": str(context.get("run_id", "")), "waba/try-number": str(context["ti"].try_number)}
        for role in ("driver", "executor"):              # étiquettes reprises par les pods (Prometheus, Loki)
            spec[role]["labels"] = {**spec[role].get("labels", {}), **labels}
            spec[role]["annotations"] = {**spec[role].get("annotations", {}), **run}
        body["metadata"] = {"name": name, "namespace": NAMESPACE, "labels": labels, "annotations": run}
        return body

    # ------------------------------------------------------------------ exécution
    def execute(self, context: dict) -> str:
        from kubernetes import client, config

        config.load_incluster_config()                   # compte de service « airflow » du pod
        api, core = client.CustomObjectsApi(), client.CoreV1Api()
        name = self._app = self.app_name()
        api.create_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PLURAL, self.build_body(name, context))
        self.log.info("SparkApplication %s/%s créée : %s %s", NAMESPACE, name, self.script, " ".join(self.arguments))
        try:
            status = self.wait(api, name)
        except BaseException:                            # timeout Airflow, arrêt manuel : on libère le cluster
            self.delete(api, name)
            raise
        self.show_driver_log(core, status, name)
        state = status.get("applicationState", {})
        if state.get("state") in FAILED:
            raise AirflowException(f"Job Spark {name} en échec ({state.get('state')}) : "
                                   f"{(state.get('errorMessage') or 'voir le journal du driver ci-dessus')[:1000]}")
        self.log.info("Job Spark %s terminé (%s)", name, state.get("state"))
        return name

    def wait(self, api, name: str) -> dict:
        last = None
        while True:
            app = api.get_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PLURAL, name)
            status = app.get("status") or {}
            state = (status.get("applicationState") or {}).get("state", "") or "PENDING"
            if state != last:
                self.log.info("SparkApplication %s : %s", name, state)
                last = state
            if state in SUCCEEDED | FAILED:
                return status
            time.sleep(self.poke_interval)

    def show_driver_log(self, core, status: dict, name: str) -> None:
        pod = (status.get("driverInfo") or {}).get("podName") or f"{name}-driver"
        try:
            text = core.read_namespaced_pod_log(pod, NAMESPACE, container="spark-kubernetes-driver",
                                                tail_lines=LOG_TAIL)
        except Exception as exc:                         # pod déjà supprimé, journal indisponible
            self.log.warning("Journal du driver %s indisponible : %s", pod, exc)
            return
        self.log.info("----- fin du journal du driver %s (%d dernières lignes) -----\n%s", pod, LOG_TAIL, text)

    def delete(self, api, name: str) -> None:
        try:
            api.delete_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PLURAL, name)
            self.log.info("SparkApplication %s supprimée", name)
        except Exception as exc:
            self.log.warning("Suppression de %s impossible : %s", name, exc)

    def on_kill(self) -> None:
        if self._app:
            from kubernetes import client, config

            config.load_incluster_config()
            self.delete(client.CustomObjectsApi(), self._app)
