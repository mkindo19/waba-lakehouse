#!/usr/bin/env python3
"""Déploie le flux NiFi « raw-landing (MinIO) -> topics Kafka raw-* » via l'API REST de NiFi.

Pourquoi un script plutôt que des clics dans l'interface : le flux est ainsi VERSIONNÉ dans Git,
relu en revue de code et recréé à l'identique sur n'importe quel poste (infrastructure as code).
L'interface NiFi (http://localhost:8089/nifi) sert ensuite à l'observer.

Flux créé dans le process group « WABA - raw-landing vers Kafka » :

    ListS3 ──► UpdateAttribute ──► RouteOnAttribute ──► FetchS3Object ──► UpdateRecord ──► PublishKafkaRecord
    (liste      (kafka.topic,       (garde les 4 flux     (télécharge      (CSV -> JSON,      (1 ligne = 1 message,
     MinIO)      source_file)        financiers)            le CSV)          + 2 champs)        clé = country_code)
                                          │ failure                             │ failure
                                          └──────────► PublishKafka ──► dlq-financial-events ◄┘

Idempotent : si le process group existe déjà, rien n'est modifié (option --recreate pour le reconstruire).
Aucun secret en dur : identifiants MinIO lus dans l'environnement, stockés chiffrés par NiFi
(propriétés « sensitive », clé NIFI_SENSITIVE_PROPS_KEY).

Bibliothèque standard uniquement (urllib) : le script tourne dans une image Python nue.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

NIFI = os.environ.get("NIFI_API", "http://nifi:8080/nifi-api")
PG_NAME = "WABA - raw-landing vers Kafka"
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
BUCKET = os.environ.get("RAW_BUCKET", "raw-landing")
S3 = {
    "Bucket": BUCKET,
    "Region": os.environ.get("AWS_REGION", "us-east-1"),
    "Access Key ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "Secret Access Key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    # Avec un « Endpoint Override », NiFi 1.x active automatiquement l'accès « path-style »
    # (bucket dans le chemin, indispensable pour MinIO, cf. chapitre 1) : aucune propriété à ajouter.
    "Endpoint Override URL": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
}
FLOWS = "bank_transactions|insurance_operations|mobile_money_payments|loan_repayments"

# Back-pressure (exigence 3.1) : plafond de chaque file d'attente. Quand il est atteint, NiFi
# cesse d'alimenter l'étape précédente au lieu de saturer Kafka.
BP_DEFAULT = {"backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "500 MB"}
BP_TO_KAFKA = {"backPressureObjectThreshold": 2000, "backPressureDataSizeThreshold": "100 MB"}


# ----------------------------------------------------------------------------- client REST minimal
def call(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{NIFI}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code} : {exc.read().decode()[:800]}") from exc


def wait_for_nifi(timeout_s: int = 600) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            call("GET", "/flow/about")
            print("NiFi est prêt")
            return
        except Exception:
            print("attente du démarrage de NiFi...")
            time.sleep(10)
    sys.exit("NiFi ne répond pas")


def rev(entity: dict) -> dict:
    return {"version": entity["revision"]["version"]}


# ----------------------------------------------------------------------------- composants
def _resolve(descriptors: dict, props: dict) -> dict:
    """Accepte des noms AFFICHÉS (ceux de l'interface) ou des clés internes : plus lisible et robuste."""
    by_display = {d["displayName"]: k for k, d in descriptors.items()}
    out = {}
    for name, value in props.items():
        out[by_display.get(name, name)] = value   # inconnu -> propriété dynamique (UpdateAttribute...)
    return out


def controller_service(pg: str, ctype: str, name: str, props: dict) -> str:
    cs = call("POST", f"/process-groups/{pg}/controller-services",
              {"revision": {"version": 0}, "component": {"type": ctype, "name": name}})
    cs = call("PUT", f"/controller-services/{cs['id']}", {
        "revision": rev(cs),
        "component": {"id": cs["id"], "properties": _resolve(cs["component"]["descriptors"], props)},
    })
    print(f"  service    {name}")
    return cs["id"]


def processor(pg: str, ptype: str, name: str, x: int, y: int, props: dict | None = None,
              schedule: str = "0 sec", auto_terminate: list[str] | None = None) -> str:
    p = call("POST", f"/process-groups/{pg}/processors", {
        "revision": {"version": 0},
        "component": {"type": ptype, "name": name, "position": {"x": x, "y": y}},
    })
    p = call("PUT", f"/processors/{p['id']}", {
        "revision": rev(p),
        "component": {
            "id": p["id"],
            "config": {
                "properties": _resolve(p["component"]["config"]["descriptors"], props or {}),
                "schedulingPeriod": schedule,
                "autoTerminatedRelationships": auto_terminate or [],
                "penaltyDuration": "30 sec",       # une erreur ne boucle pas en continu
            },
        },
    })
    print(f"  processor  {name}")
    return p["id"]


def connect(pg: str, src: str, dst: str, rels: list[str], bp: dict = BP_DEFAULT) -> None:
    call("POST", f"/process-groups/{pg}/connections", {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src, "groupId": pg, "type": "PROCESSOR"},
            "destination": {"id": dst, "groupId": pg, "type": "PROCESSOR"},
            "selectedRelationships": rels,
            **bp,
        },
    })


# ----------------------------------------------------------------------------- (re)création
def find_pg(root: str) -> dict | None:
    for pg in call("GET", f"/process-groups/{root}/process-groups")["processGroups"]:
        if pg["component"]["name"] == PG_NAME:
            return pg
    return None


def delete_pg(pg: dict) -> None:
    """Arrêt, vidage des files, désactivation des services puis suppression."""
    pid = pg["id"]
    call("PUT", f"/flow/process-groups/{pid}", {"id": pid, "state": "STOPPED"})
    call("PUT", f"/flow/process-groups/{pid}/controller-services", {"id": pid, "state": "DISABLED"})
    for c in call("GET", f"/process-groups/{pid}/connections")["connections"]:
        call("POST", f"/flowfile-queues/{c['id']}/drop-requests")
    time.sleep(10)
    pg = call("GET", f"/process-groups/{pid}")
    call("DELETE", f"/process-groups/{pid}?version={pg['revision']['version']}")
    print(f"ancien process group supprimé ({pid})")


def build(root: str) -> str:
    pg = call("POST", f"/process-groups/{root}/process-groups", {
        "revision": {"version": 0}, "component": {"name": PG_NAME, "position": {"x": 0, "y": 0}},
    })["id"]
    print(f"process group créé : {PG_NAME} ({pg})")

    # --- services : lecture CSV (toutes colonnes en texte), écriture / lecture JSON
    csv_reader = controller_service(pg, "org.apache.nifi.csv.CSVReader", "CSV (en-tête, colonnes texte)", {
        "Schema Access Strategy": "csv-header-derived",     # schéma = noms de l'en-tête, types texte
    })
    json_array = controller_service(pg, "org.apache.nifi.json.JsonRecordSetWriter", "JSON (tableau)", {
        "Schema Write Strategy": "no-schema",
    })
    json_reader = controller_service(pg, "org.apache.nifi.json.JsonTreeReader", "JSON (lecture)", {})
    json_line = controller_service(pg, "org.apache.nifi.json.JsonRecordSetWriter", "JSON (1 objet par message)", {
        "Schema Write Strategy": "no-schema",
        "Output Grouping": "output-oneline",                 # pas de [...] autour de chaque message
    })

    # --- processeurs
    list_s3 = processor(pg, "org.apache.nifi.processors.aws.s3.ListS3", "1. Lister raw-landing (MinIO)",
                        0, 0, {**S3, "Minimum Object Age": "0 sec"}, schedule="10 sec")
    tag = processor(pg, "org.apache.nifi.processors.attributes.UpdateAttribute", "2. Topic cible + source_file",
                    0, 200, {
                        # bank_transactions/CI/x.csv -> raw-bank-transactions
                        "kafka.topic": "${filename:substringBefore('/'):replaceAll('_', '-'):prepend('raw-')}",
                        "source_file": f"s3://{BUCKET}/${{filename}}",
                    })
    route = processor(pg, "org.apache.nifi.processors.standard.RouteOnAttribute",
                      "3. Garder les 4 flux financiers", 0, 400, {
                          "Routing Strategy": "Route to Property name",
                          "flux_financier": f"${{filename:matches('({FLOWS})/[A-Z]{{2}}/.*[.]csv')}}",
                      }, auto_terminate=["unmatched"])        # référentiels : traités par le batch seulement
    fetch = processor(pg, "org.apache.nifi.processors.aws.s3.FetchS3Object", "4. Télécharger le CSV",
                      0, 600, {**S3, "Object Key": "${filename}"})
    enrich = processor(pg, "org.apache.nifi.processors.standard.UpdateRecord",
                       "5. CSV -> JSON + ingestion_timestamp + source_file", 0, 800, {
                           "Record Reader": csv_reader,
                           "Record Writer": json_array,
                           "Replacement Value Strategy": "literal-value",
                           "/ingestion_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", 'GMT')}",
                           "/source_file": "${source_file}",
                       })
    publish = processor(pg, "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
                        "6. Publier dans Kafka (1 ligne = 1 message)", 0, 1000, {
                            "Kafka Brokers": KAFKA,
                            "Topic Name": "${kafka.topic}",
                            "Record Reader": json_reader,
                            "Record Writer": json_line,
                            "Use Transactions": "false",
                            "Delivery Guarantee": "all",            # acks=all : accusé du broker
                            "Message Key Field": "country_code",     # même pays -> même partition
                        }, auto_terminate=["success"])
    dlq = processor(pg, "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
                    "DLQ : fichier illisible -> dlq-financial-events", 500, 800, {
                        "Kafka Brokers": KAFKA,
                        "Topic Name": "dlq-financial-events",
                        "Use Transactions": "false",
                        "Delivery Guarantee": "all",
                        "Attributes to Send as Headers (Regex)": "filename|source_file|kafka.topic",
                    }, auto_terminate=["success"])

    # --- liaisons (les « failure » bouclent sur elles-mêmes : nouvelle tentative après pénalité)
    connect(pg, list_s3, tag, ["success"])
    connect(pg, tag, route, ["success"])
    connect(pg, route, fetch, ["flux_financier"])
    connect(pg, fetch, enrich, ["success"])
    connect(pg, fetch, dlq, ["failure"])        # fichier disparu / illisible : tracé en DLQ, pas de boucle infinie
    connect(pg, enrich, publish, ["success"], BP_TO_KAFKA)
    connect(pg, enrich, dlq, ["failure"])
    connect(pg, publish, publish, ["failure"], BP_TO_KAFKA)
    connect(pg, dlq, dlq, ["failure"])
    return pg


def start(pg: str) -> None:
    call("PUT", f"/flow/process-groups/{pg}/controller-services", {"id": pg, "state": "ENABLED"})
    time.sleep(5)
    call("PUT", f"/flow/process-groups/{pg}", {"id": pg, "state": "RUNNING"})
    time.sleep(5)
    invalid = [p["component"]["name"] + " : " + "; ".join(p["component"].get("validationErrors", []))
               for p in call("GET", f"/process-groups/{pg}/processors")["processors"]
               if p["component"].get("validationStatus") == "INVALID"]
    if invalid:
        sys.exit("Processeurs invalides :\n  " + "\n  ".join(invalid))
    print("flux démarré : tous les processeurs sont valides")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--recreate", action="store_true", help="supprime puis recrée le process group")
    args = parser.parse_args()

    if not S3["Access Key ID"] or not S3["Secret Access Key"]:
        sys.exit("AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY absents de l'environnement")
    wait_for_nifi()
    root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
    existing = find_pg(root)
    if existing and not args.recreate:
        print(f"« {PG_NAME} » existe déjà : rien à faire (option --recreate pour le reconstruire)")
        return
    if existing:
        delete_pg(existing)
    start(build(root))


if __name__ == "__main__":
    main()
