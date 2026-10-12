#!/usr/bin/env python3
"""Synchronisation OpenMetadata de la plateforme WABA (niveau 4.3, gouvernance).

Exécuté par un Job Kubernetes à chaque `make k8s-install` (et à la demande : `make k8s-om-sync`). Idempotent.

1. Attend le serveur OpenMetadata, se connecte en administrateur et remplace le mot de passe par défaut
   (« admin ») par celui de .env / waba-secrets au premier passage.
2. Découverte automatique (connecteurs officiels OpenMetadata) :
   - Trino : toutes les tables Iceberg des schémas raw, bronze, silver, gold (colonnes, types, partitions) ;
   - Kafka : les topics raw-*, silver-*, gold-*, dlq-*.
3. Gouvernance « as code » (governance/catalog.yaml) : équipes propriétaires, classifications (BCEAO, CIMA,
   données personnelles), glossaire financier, documentation des tables gold, marquage PII des colonnes sur
   toutes les couches, pipelines (DAGs Airflow, jobs streaming) et lignage raw -> bronze -> silver -> gold.

Variables : OM_URL, OPENMETADATA_ADMIN_PASSWORD, TRINO_HOST_PORT, KAFKA_BOOTSTRAP, CATALOG_FILE, SKIP_INGESTION.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests
import yaml
from metadata.generated.schema.api.classification.createClassification import CreateClassificationRequest
from metadata.generated.schema.api.classification.createTag import CreateTagRequest
from metadata.generated.schema.api.data.createGlossary import CreateGlossaryRequest
from metadata.generated.schema.api.data.createGlossaryTerm import CreateGlossaryTermRequest
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
from metadata.generated.schema.api.teams.createTeam import CreateTeamRequest
from metadata.generated.schema.entity.data.pipeline import Pipeline, Task
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.data.topic import Topic
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.connections.pipeline.airflowConnection import AirflowConnection
from metadata.generated.schema.entity.services.connections.pipeline.backendConnection import BackendConnection
from metadata.generated.schema.entity.services.pipelineService import PipelineConnection, PipelineServiceType
from metadata.generated.schema.entity.teams.team import Team, TeamType
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityLineage import Source as LineageSource
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
from metadata.generated.schema.type.tagLabel import LabelType, State, TagLabel, TagSource
from metadata.ingestion.models.table_metadata import ColumnDescription, ColumnTag
from metadata.ingestion.ometa.ometa_api import OpenMetadata

log = logging.getLogger("om-sync")          # handler propre : la bibliothèque OpenMetadata reconfigure le logging
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(_handler)
log.setLevel(logging.INFO)
log.propagate = False
for noisy in ("metadata", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

OM_URL = os.environ.get("OM_URL", "http://openmetadata.governance.svc.cluster.local:8585").rstrip("/")
API = f"{OM_URL}/api"
ADMIN_EMAIL = "admin@open-metadata.org"
DEFAULT_ADMIN_PASSWORD = "admin"                       # mot de passe livré par OpenMetadata, remplacé ici
PIPELINE_SERVICE = "waba_airflow"
AIRFLOW_UI = os.environ.get("AIRFLOW_UI_URL", "http://airflow.localtest.me:8090")


# ----------------------------------------------------------------------------- connexion
def wait_for_server(timeout_s: int = 1200) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{API}/v1/system/version", timeout=5)
            if r.ok:
                log.info("OpenMetadata %s joignable", r.json().get("version"))
                return
        except requests.RequestException:
            pass
        log.info("attente du serveur OpenMetadata...")
        time.sleep(10)
    sys.exit("OpenMetadata injoignable")


def login(password: str) -> str | None:
    encoded = base64.b64encode(password.encode()).decode()       # l'API attend le mot de passe en base64
    r = requests.post(f"{API}/v1/users/login", json={"email": ADMIN_EMAIL, "password": encoded}, timeout=30)
    return r.json().get("accessToken") if r.ok else None


def admin_token() -> str:
    """Jeton JWT administrateur ; au premier passage, remplace le mot de passe par défaut par le secret."""
    password = os.environ["OPENMETADATA_ADMIN_PASSWORD"]
    token = login(password)
    if token:
        return token
    token = login(DEFAULT_ADMIN_PASSWORD)
    if not token:
        sys.exit("connexion administrateur impossible (ni le secret ni le mot de passe par défaut)")
    r = requests.put(f"{API}/v1/users/changePassword", headers={"Authorization": f"Bearer {token}"}, timeout=30,
                     json={"username": "admin", "oldPassword": DEFAULT_ADMIN_PASSWORD, "newPassword": password,
                           "confirmPassword": password, "requestType": "SELF"})
    if not r.ok:
        sys.exit(f"changement du mot de passe administrateur refusé : {r.status_code} {r.text[:300]}")
    log.info("mot de passe administrateur par défaut remplacé")
    return login(password) or sys.exit("connexion impossible après changement du mot de passe")


def server_config(token: str) -> dict:
    return {"hostPort": API, "authProvider": "openmetadata", "securityConfig": {"jwtToken": token}}


# ----------------------------------------------------------------------------- découverte automatique
def run_ingestion(token: str, catalog: dict) -> None:
    from metadata.workflow.metadata import MetadataWorkflow  # import lourd : seulement si l'on ingère

    workflows = {
        "Trino (tables Iceberg)": {
            "source": {
                "type": "trino",
                "serviceName": catalog["service"],
                "serviceConnection": {"config": {
                    "type": "Trino",
                    "hostPort": os.environ.get("TRINO_HOST_PORT", "trino.serving.svc.cluster.local:8080"),
                    "username": "openmetadata",
                    "catalog": catalog["database"],
                }},
                "sourceConfig": {"config": {
                    "type": "DatabaseMetadata",
                    "schemaFilterPattern": {"includes": [f"^({'|'.join(catalog['schemas'])})$"]},
                    "includeViews": False,
                    "includeTags": False,
                    "markDeletedTables": True,
                }},
            },
        },
        "Kafka (topics)": {
            "source": {
                "type": "kafka",
                "serviceName": catalog["messaging_service"],
                "serviceConnection": {"config": {
                    "type": "Kafka",
                    "bootstrapServers": os.environ.get("KAFKA_BOOTSTRAP", "kafka.ingestion.svc.cluster.local:9092"),
                }},
                "sourceConfig": {"config": {
                    "type": "MessagingMetadata",
                    "topicFilterPattern": {"includes": ["^(raw|silver|gold|dlq)-"]},
                    "markDeletedTopics": True,
                }},
            },
        },
    }
    for label, cfg in workflows.items():
        cfg["sink"] = {"type": "metadata-rest", "config": {}}
        cfg["workflowConfig"] = {"loggerLevel": "WARN", "openMetadataServerConfig": server_config(token)}
        log.info("découverte %s...", label)
        workflow = MetadataWorkflow.create(cfg)
        workflow.execute()
        workflow.print_status()
        workflow.raise_from_status()
        workflow.stop()


# ----------------------------------------------------------------------------- gouvernance as code
class Governance:
    def __init__(self, om: OpenMetadata, catalog: dict):
        self.om, self.c = om, catalog
        self.teams: dict[str, Team] = {}
        self.pipelines: dict[str, Pipeline] = {}
        self.stats = {"tables_gold_documentees": 0, "colonnes_marquees": 0, "aretes_lignage": 0, "erreurs_lignage": 0,
                      "manquants": []}

    # -- références
    def table_fqn(self, ref: str) -> str:
        return f"{self.c['service']}.{self.c['database']}.{ref}"

    def table(self, ref: str) -> Table | None:
        return self.om.get_by_name(Table, self.table_fqn(ref), fields=["tags", "owners", "columns"])

    def node(self, ref: str) -> EntityReference | None:
        if ref.startswith("topic:"):
            entity, kind = self.om.get_by_name(Topic, f"{self.c['messaging_service']}.{ref[6:]}"), "topic"
        else:
            entity, kind = self.om.get_by_name(Table, self.table_fqn(ref)), "table"
        if entity is None:
            self.stats["manquants"].append(ref)
            return None
        return EntityReference(id=entity.id, type=kind)

    @staticmethod
    def label(fqn: str, glossary: bool = False) -> TagLabel:
        return TagLabel(tagFQN=fqn, source=TagSource.Glossary if glossary else TagSource.Classification,
                        labelType=LabelType.Manual, state=State.Confirmed)

    def owner_ref(self, team: str) -> EntityReferenceList:
        return EntityReferenceList(root=[EntityReference(id=self.teams[team].id, type="team")])

    # -- étapes
    def apply_teams(self) -> None:
        for t in self.c["teams"]:
            self.teams[t["name"]] = self.om.create_or_update(CreateTeamRequest(
                name=t["name"], displayName=t["display"], description=t["description"], teamType=TeamType.Group))
        log.info("équipes : %s", ", ".join(self.teams))

    def apply_classifications(self) -> None:
        for cl in self.c["classifications"]:
            self.om.create_or_update(CreateClassificationRequest(name=cl["name"], description=cl["description"]))
            for tag in cl["tags"]:
                self.om.create_or_update(CreateTagRequest(classification=cl["name"], name=tag["name"],
                                                          description=tag["description"]))
        log.info("classifications : %s", ", ".join(cl["name"] for cl in self.c["classifications"]))

    def apply_glossary(self) -> None:
        g = self.c["glossary"]
        self.om.create_or_update(CreateGlossaryRequest(
            name=g["name"], displayName=g["display"], description=g["description"],
            owners=self.owner_ref("waba_conformite")))
        for term in g["terms"]:
            self.om.create_or_update(CreateGlossaryTermRequest(
                glossary=g["name"], name=term["name"], displayName=term["display"], description=term["description"]))
        log.info("glossaire %s : %d termes", g["name"], len(g["terms"]))

    def apply_gold_documentation(self) -> None:
        glossary = self.c["glossary"]["name"]
        for name, doc in self.c["gold_tables"].items():
            table = self.table(f"gold.{name}")
            if table is None:
                self.stats["manquants"].append(f"gold.{name}")
                continue
            self.om.patch_description(Table, table, doc["description"], force=True)
            self.om.patch_owner(Table, table, self.owner_ref(doc["owner"]), force=True)
            labels = [self.label(t) for t in doc.get("tags", [])]
            labels += [self.label(f"{glossary}.{t}", glossary=True) for t in doc.get("glossary", [])]
            if labels:
                self.om.patch_tags(Table, self.table(f"gold.{name}"), labels)
            self.stats["tables_gold_documentees"] += 1

    def all_tables(self) -> list[Table]:
        return list(self.om.list_all_entities(
            entity=Table, fields=["tags", "owners", "columns"],
            params={"database": f"{self.c['service']}.{self.c['database']}"}))

    def apply_default_owners_and_pii(self) -> None:
        rules = [(re.compile(r["pattern"]), [re.compile(t) for t in r.get("tables", [])], r) for r in self.c["column_tags"]]
        default_owner = self.owner_ref(self.c["default_owner"])
        for table in self.all_tables():
            schema_table = ".".join(table.fullyQualifiedName.root.split(".")[-2:])
            if not table.owners or not table.owners.root:
                self.om.patch_owner(Table, table, default_owner)
            column_tags, descriptions = [], {}
            for col in table.columns:
                col_name = col.name.root
                for pattern, table_filters, rule in rules:
                    if not pattern.match(col_name) or (table_filters and not any(f.match(schema_table) for f in table_filters)):
                        continue
                    col_fqn = f"{table.fullyQualifiedName.root}.{col_name}"
                    column_tags += [ColumnTag(column_fqn=col_fqn, tag_label=self.label(t)) for t in rule["tags"]]
                    if not col.description and rule.get("description"):
                        descriptions[col_fqn] = rule["description"]
            if column_tags:
                self.om.patch_column_tags(self.table(schema_table), column_tags)
                self.stats["colonnes_marquees"] += len({ct.column_fqn for ct in column_tags})
            if descriptions:
                self.om.patch_column_descriptions(self.table(schema_table), [
                    ColumnDescription(column_fqn=fqn, description=text) for fqn, text in descriptions.items()])

    def apply_pipelines(self) -> None:
        self.om.create_or_update(CreatePipelineServiceRequest(
            name=PIPELINE_SERVICE, serviceType=PipelineServiceType.Airflow,
            description="Orchestration WABA : DAGs Airflow (batch) et jobs Spark Structured Streaming (Spark Operator).",
            connection=PipelineConnection(config=AirflowConnection(hostPort=AIRFLOW_UI, connection=BackendConnection()))))
        for p in self.c["pipelines"]:
            url = f"{AIRFLOW_UI}/dags/{p['name']}/grid" if p["name"].startswith("dag_") else None
            self.pipelines[p["name"]] = self.om.create_or_update(CreatePipelineRequest(
                name=p["name"], service=PIPELINE_SERVICE, description=p["description"], sourceUrl=url,
                tasks=[Task(name=t) for t in p["tasks"]]))
        log.info("pipelines : %s", ", ".join(self.pipelines))

    def apply_lineage(self) -> None:
        for pipeline_name, edges in self.c["lineage"].items():
            pipeline = self.pipelines[pipeline_name]
            for source, targets in edges.items():
                src = self.node(source)
                for target in targets:
                    dst = self.node(target)
                    if src is None or dst is None:
                        continue
                    result = self.om.add_lineage(AddLineageRequest(edge=EntitiesEdge(
                        fromEntity=src, toEntity=dst,
                        lineageDetails=LineageDetails(
                            pipeline=EntityReference(id=pipeline.id, type="pipeline"),
                            description=f"{source} -> {target} ({pipeline_name})",
                            source=LineageSource.PipelineLineage))))
                    if isinstance(result, dict) and "error" in result:
                        self.stats["erreurs_lignage"] += 1
                    else:
                        self.stats["aretes_lignage"] += 1

    def run(self) -> dict:
        self.apply_teams()
        self.apply_classifications()
        self.apply_glossary()
        self.apply_pipelines()
        self.apply_gold_documentation()
        self.apply_default_owners_and_pii()
        self.apply_lineage()
        self.stats["manquants"] = sorted(set(self.stats["manquants"]))
        return self.stats


def main() -> int:
    catalog = yaml.safe_load(Path(os.environ.get("CATALOG_FILE", Path(__file__).with_name("catalog.yaml"))).read_text())
    wait_for_server()
    token = admin_token()
    if os.environ.get("SKIP_INGESTION", "false").lower() != "true":
        run_ingestion(token, catalog)
    om = OpenMetadata(OpenMetadataConnection(hostPort=API, authProvider=AuthProvider.openmetadata,
                                             securityConfig=OpenMetadataJWTClientConfig(jwtToken=token)))
    stats = Governance(om, catalog).run()
    log.info("synthèse : %s", stats)
    if stats["tables_gold_documentees"] < 5:
        log.error("moins de 5 tables gold documentées (tables absentes de Trino ?)")
        return 1
    if stats["erreurs_lignage"]:
        log.error("%d arêtes de lignage refusées par le serveur", stats["erreurs_lignage"])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
