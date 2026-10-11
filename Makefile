# Raccourcis (à lancer depuis WSL2 / Linux / macOS)
SHELL := /bin/bash
COMPOSE := docker compose
SPARK_SUBMIT := $(COMPOSE) exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
DATASET ?= all
COUNTRY ?=

.PHONY: help env up down clean ps logs referentials generate ingest replay trino test lint bronze silver gold report test-spark airflow-ui up-streaming topics kafka-smoke nifi-deploy nifi-redeploy kafka-peek stream-logs dlq-test stream-reset stream2-logs alert-test gold-peek stream2-reset lambda check3 k8s-files k8s-secrets k8s-install k8s-status k8s-expose k8s-uninstall k8s-lint k8s-images k8s-referentials k8s-stream k8s-trino k8s-deps k8s-crds k8s-spark k8s-spark-logs k8s-generate k8s-dag k8s-dag-runs k8s-dags-on k8s-alert-test k8s-gold-peek k8s-deps-update k8s-start

help:                ## Liste des commandes
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

env:                 ## Crée / complète .env depuis .env.example (secrets aléatoires, existant conservé)
	@python3 scripts/env_sync.py

up: env              ## Démarre toute la stack niveau 1
	$(COMPOSE) up -d --build

down:                ## Arrête la stack (données conservées)
	$(COMPOSE) down

clean:               ## Arrête la stack ET supprime les volumes (données perdues)
	@read -p "Supprimer toutes les données (volumes) ? [y/N] " ok && [ "$$ok" = "y" ] && $(COMPOSE) down -v || echo "annulé"

ps:                  ## État des services
	$(COMPOSE) ps

logs:                ## Logs de tous les services
	$(COMPOSE) logs -f --tail=100

referentials:        ## Génère les référentiels (CLI, sans passer par Streamlit)
	$(COMPOSE) exec generator python -m waba_gen referentials

generate:            ## Génère un trimestre de transactions (volumes par défaut)
	$(COMPOSE) exec generator python -m waba_gen batch

ingest:              ## Ingestion Spark -> raw.* (DATASET=all|bank_transactions..., COUNTRY=CI)
	$(SPARK_SUBMIT) /opt/spark-apps/ingest_raw.py --dataset $(DATASET) $(if $(COUNTRY),--country $(COUNTRY),)

replay:              ## Remet les fichiers archivés dans raw-landing (test d'idempotence)
	$(COMPOSE) run --rm --entrypoint /bin/sh minio-init -c 'mc alias set l http://minio:9000 $$MINIO_ROOT_USER $$MINIO_ROOT_PASSWORD >/dev/null && mc cp --recursive l/archive/ l/raw-landing/'

trino:               ## Ouvre le client SQL Trino
	$(COMPOSE) exec trino trino --catalog iceberg --schema raw

bronze:              ## [N2] raw.* -> bronze.* (COUNTRY=CI optionnel)
	$(SPARK_SUBMIT) /opt/spark-apps/raw_to_bronze.py --country $(or $(COUNTRY),ALL)

silver:              ## [N2] bronze.* -> silver.* (COUNTRY=CI optionnel)
	$(SPARK_SUBMIT) /opt/spark-apps/bronze_to_silver.py --domain all --country $(or $(COUNTRY),ALL)

gold:                ## [N2] silver.* -> gold.* : les 7 KPIs (COUNTRY=CI optionnel)
	$(SPARK_SUBMIT) /opt/spark-apps/silver_to_gold.py --kpi all --country $(or $(COUNTRY),ALL)

report:              ## [N2] Rapport réglementaire (DATE=AAAA-MM-JJ, défaut : hier)
	$(SPARK_SUBMIT) /opt/spark-apps/regulatory_report.py $(if $(DATE),--report-date $(DATE),)

airflow-ui:          ## [N2] Adresse de l'interface Airflow
	@echo "http://localhost:$${AIRFLOW_PORT:-8082}  (identifiants : AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD dans .env)"

test:                ## Tests unitaires du générateur
	cd generator && python -m pytest -q tests

test-spark:          ## Tests des transformations Spark (nécessite Java + pyspark en local)
	cd spark && python -m pytest -q tests

lint:                ## Vérification PEP 8 (ruff)
	ruff check generator spark airflow superset

# ----------------------------------------------------------------------------- niveau 3 : streaming
KAFKA_EXEC := $(COMPOSE) exec -T kafka /opt/kafka/bin

up-streaming: env    ## [N3] Démarre la stack + Kafka, Kafka UI (profil streaming)
	$(COMPOSE) --profile streaming up -d --build

topics:              ## [N3] Liste des topics Kafka avec partitions et rétention
	@$(KAFKA_EXEC)/kafka-topics.sh --bootstrap-server kafka:9092 --describe | grep -E "^Topic:" | awk '{print $$2, $$4, $$6, $$10}' | column -t

kafka-smoke:         ## [N3] Test aller-retour : produire puis relire 1 message (topic jetable)
	@$(KAFKA_EXEC)/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic smoke-test --partitions 1 --replication-factor 1 >/dev/null
	@echo '{"test":"waba","ts":"'$$(date -u +%FT%TZ)'"}' | $(KAFKA_EXEC)/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic smoke-test
	@$(KAFKA_EXEC)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic smoke-test --from-beginning --max-messages 1 --timeout-ms 15000
	@$(KAFKA_EXEC)/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic smoke-test && echo "OK : message produit puis relu (topic smoke-test supprimé)"

nifi-deploy:         ## [N3] (Re)lance le déploiement du flux NiFi (idempotent)
	$(COMPOSE) --profile streaming run --rm nifi-init

nifi-redeploy:       ## [N3] Supprime puis recrée le flux NiFi (après modification de deploy_flow.py)
	$(COMPOSE) --profile streaming run --rm nifi-init python -u /scripts/deploy_flow.py --recreate

TOPIC ?= raw-bank-transactions
N ?= 3
kafka-peek:          ## [N3] Affiche N messages d'un topic (TOPIC=raw-bank-transactions N=3), avec clé
	@$(KAFKA_EXEC)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $(TOPIC) --from-beginning \
	  --max-messages $(N) --timeout-ms 15000 --property print.key=true --property key.separator=" | "

stream-logs:         ## [N3] Journaux du job streaming 1 (raw -> silver), micro-lot par micro-lot
	$(COMPOSE) --profile streaming logs -f --tail=50 stream-raw-to-silver

dlq-test:            ## [N3] Publie un message volontairement invalide (montant "12a") dans raw-bank-transactions
	@echo 'SN|{"transaction_id":"DLQ-TEST-1","timestamp":"2026-10-05T12:00:00","account_id":"X","beneficiary_account":"","branch_id":"X","country_code":"SN","transaction_type":"PAYMENT","amount":"12a","currency":"XOF","channel":"ATM","transaction_status":"SUCCESS","fee_amount":"0","entity_type":"BANK","source_file":"test-manuel","ingestion_timestamp":"2026-10-06T00:00:00.000Z"}' \
	  | $(KAFKA_EXEC)/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic raw-bank-transactions \
	    --property parse.key=true --property key.separator='|' && echo "message invalide publié : il doit apparaître dans dlq-financial-events"

stream-reset:        ## [N3] Repart de zéro côté streaming : vide silver-* et la DLQ, efface les checkpoints du job 1
	@read -p "Vider les topics silver-* / dlq et rejouer raw-* depuis le début ? [y/N] " ok && [ "$$ok" = "y" ] || { echo "annulé"; exit 1; }
	$(COMPOSE) --profile streaming stop stream-raw-to-silver
	$(KAFKA_EXEC)/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic 'silver-.*|dlq-financial-events'
	sleep 10
	$(COMPOSE) --profile streaming run --rm kafka-init
	$(COMPOSE) --profile streaming run --rm --no-deps --entrypoint rm stream-raw-to-silver -rf /opt/spark-checkpoints/raw_to_silver
	$(COMPOSE) --profile streaming up -d stream-raw-to-silver

stream2-logs:        ## [N3] Journaux du job streaming 2 (silver -> gold : fraude, AML, liquidité)
	$(COMPOSE) --profile streaming logs -f --tail=50 stream-fraud-aml

alert-test:          ## [N3] Publie 5 transactions de test (rafale, AML, liquidité) horodatées maintenant (COUNTRY=SN)
	@bash scripts/alert_test.sh $(or $(COUNTRY),SN)

gold-peek:           ## [N3] Nombre de messages dans les topics gold-* (contenu : make kafka-peek TOPIC=gold-fraud-alerts)
	@for t in gold-fraud-alerts gold-aml-events gold-liquidity-alerts; do \
	  n=$$($(KAFKA_EXEC)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $$t | awk -F: '{s+=$$3} END {print s+0}'); \
	  echo "== $$t : $$n message(s)"; \
	done

stream2-reset:       ## [N3] Repart de zéro côté job 2 : vide gold-*, efface ses checkpoints et les tables gold.rt_*
	@read -p "Vider les topics gold-*, les tables gold.rt_* et rejouer silver-* depuis le début ? [y/N] " ok && [ "$$ok" = "y" ] || { echo "annulé"; exit 1; }
	$(COMPOSE) --profile streaming stop stream-fraud-aml
	$(KAFKA_EXEC)/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic 'gold-.*'
	for t in rt_fraud_alerts rt_aml_events rt_liquidity_alerts; do \
	  $(COMPOSE) exec -T trino trino --execute "DROP TABLE IF EXISTS iceberg.gold.$$t"; done
	sleep 10
	$(COMPOSE) --profile streaming run --rm kafka-init
	$(COMPOSE) --profile streaming run --rm --no-deps --entrypoint rm stream-fraud-aml -rf /opt/spark-checkpoints/fraud_aml
	$(COMPOSE) --profile streaming up -d stream-fraud-aml

lambda:              ## [N3] Requête Lambda unifiée : gold Iceberg (batch) + Kafka (temps réel) dans Trino
	$(COMPOSE) exec -T trino trino --catalog iceberg < sql/level3_lambda.sql

check3:              ## [N3] Contrôles du niveau 3 (latence, double sink, DLQ, alertes, AML, liquidité)
	$(COMPOSE) exec -T trino trino --catalog iceberg < sql/level3_checks.sql

# ----------------------------------------------------------------------------- niveau 4 : Kubernetes
K8S_RELEASE := waba
K8S_NS := processing
K8S_HTTP_PORT ?= 8090
HELM_CHART := helm/waba-platform

k8s-files:           ## [N4] Copie les scripts de infra/ dans le chart (Helm ne lit que les fichiers du chart)
	@mkdir -p $(HELM_CHART)/files/minio $(HELM_CHART)/files/kafka $(HELM_CHART)/files/nifi $(HELM_CHART)/files/trino-kafka $(HELM_CHART)/files/airflow-dags $(HELM_CHART)/files/superset
	@cp infra/minio/init.sh $(HELM_CHART)/files/minio/init.sh
	@cp infra/kafka/create-topics.sh $(HELM_CHART)/files/kafka/create-topics.sh
	@cp infra/nifi/deploy_flow.py $(HELM_CHART)/files/nifi/deploy_flow.py
	@cp trino/kafka/*.json $(HELM_CHART)/files/trino-kafka/
	@rm -f $(HELM_CHART)/files/airflow-dags/*.py && cp airflow/dags/*.py $(HELM_CHART)/files/airflow-dags/
	@cp superset/superset_config.py superset/dashboards/build_assets.py superset/dashboards/import_assets.py $(HELM_CHART)/files/superset/

k8s-secrets: env     ## [N4] Génère helm/values-secrets.yaml depuis .env (ignoré par git)
	@python3 scripts/k8s_values_secrets.py

k8s-lint: k8s-files k8s-secrets k8s-crds   ## [N4] Vérifie le chart (syntaxe + rendu complet) sans rien installer
	helm lint $(HELM_CHART) -f helm/values-secrets.yaml
	helm template $(K8S_RELEASE) $(HELM_CHART) -n $(K8S_NS) -f helm/values-secrets.yaml | kubectl create --dry-run=client -o name -f - >/dev/null && echo "rendu OK : manifestes valides"

# Téléchargé une seule fois : une installation ne dépend pas d'Internet si l'archive est déjà là (vécu : coupure DNS
# sous WSL -> make k8s-install bloqué). Forcer la mise à jour : make k8s-deps-update.
k8s-deps:            ## [N4] Dépendances du chart (Spark Operator) dans helm/waba-platform/charts/ (si absentes)
	@if ls $(HELM_CHART)/charts/spark-operator-*.tgz >/dev/null 2>&1; then \
	  echo "Spark Operator déjà présent : $$(ls $(HELM_CHART)/charts/)"; \
	else $(MAKE) --no-print-directory k8s-deps-update; fi

k8s-deps-update:     ## [N4] Retélécharge les dépendances du chart (Internet requis)
	@helm repo add spark-operator https://kubeflow.github.io/spark-operator >/dev/null 2>&1 || true
	helm dependency build $(HELM_CHART) || helm dependency update $(HELM_CHART)

# Helm n'installe les CRD d'un sous-chart qu'au PREMIER install : on les applique explicitement (idempotent).
k8s-crds: k8s-deps   ## [N4] Installe / met à jour les CRD de Spark Operator (SparkApplication...)
	tar -xzOf $$(ls $(HELM_CHART)/charts/spark-operator-*.tgz | tail -1) --wildcards 'spark-operator/crds/*.yaml' \
	  | kubectl apply --server-side --force-conflicts -f - >/dev/null && echo "CRD Spark Operator à jour"

k8s-install: k8s-files k8s-secrets k8s-crds   ## [N4] Installe / met à jour TOUTE la plateforme : un seul helm upgrade --install
	helm upgrade --install $(K8S_RELEASE) $(HELM_CHART) -n $(K8S_NS) --create-namespace \
	  -f helm/values-secrets.yaml --wait --timeout 20m

LOCAL_IMAGES := waba/generator:$(or $(WABA_VERSION),dev) waba/spark:$(or $(WABA_VERSION),dev) waba/airflow:$(or $(WABA_VERSION),dev) waba/superset:$(or $(WABA_VERSION),dev)
k8s-images:          ## [N4] Construit les images maison (docker compose) et les charge dans minikube (~5 min)
	$(COMPOSE) build generator spark-master airflow-scheduler
	docker build -t waba/superset:$(or $(WABA_VERSION),dev) superset
	@for img in $(LOCAL_IMAGES); do echo "-> minikube image load $$img"; minikube image load $$img || exit 1; done
	@minikube image ls | grep waba/

k8s-referentials:    ## [N4] Génère les référentiels dans MinIO (pod generator)
	kubectl exec -n ingestion deploy/generator -- python -m waba_gen referentials

k8s-generate:        ## [N4] Génère un trimestre de fichiers de transactions dans MinIO (raw-landing)
	kubectl exec -n ingestion deploy/generator -- python -m waba_gen batch

DAG ?= dag_ingest_raw
k8s-dag:             ## [N4] Active et déclenche un DAG Airflow (DAG=dag_ingest_raw)
	kubectl exec -n processing deploy/airflow-scheduler -c scheduler -- airflow dags unpause $(DAG)
	kubectl exec -n processing deploy/airflow-scheduler -c scheduler -- airflow dags trigger $(DAG)

k8s-dags-on:         ## [N4] Active les DAGs de la chaîne batch (créés en pause) ; le rapport réglementaire reste à part
	@for d in dag_ingest_raw dag_bronze_to_silver dag_silver_to_gold; do \
	  kubectl exec -n processing deploy/airflow-scheduler -c scheduler -- airflow dags unpause $$d | grep -E "^dag_"; done

k8s-dag-runs:        ## [N4] État des derniers runs des 4 DAGs et des SparkApplication
	@for d in dag_ingest_raw dag_bronze_to_silver dag_silver_to_gold dag_regulatory_report; do \
	  kubectl exec -n processing deploy/airflow-scheduler -c scheduler -- airflow dags list-runs -d $$d -o plain 2>/dev/null | head -4; done
	@echo; kubectl get sparkapplications -n processing

K8S_KAFKA := kubectl exec -i -n ingestion kafka-0 --
k8s-alert-test:      ## [N4] Transactions de test (rafale, AML, liquidité) publiées dans Kafka sur K8s (COUNTRY=SN)
	@KAFKA_EXEC="$(K8S_KAFKA)" KAFKA_BOOTSTRAP=localhost:9092 bash scripts/alert_test.sh $(or $(COUNTRY),SN)

k8s-gold-peek:       ## [N4] Nombre de messages dans les topics gold-* (job 2 sur K8s)
	@for t in gold-fraud-alerts gold-aml-events gold-liquidity-alerts; do \
	  n=$$($(K8S_KAFKA) /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $$t | awk -F: '{s+=$$3} END {print s+0}'); \
	  echo "== $$t : $$n message(s)"; \
	done

ROWS ?= 2000
DURATION ?= 90
k8s-stream:          ## [N4] Flux continu de transactions (ROWS par lot, DURATION secondes)
	kubectl exec -n ingestion deploy/generator -- python -m waba_gen stream --rows-per-tick $(ROWS) --min-interval 20 --max-interval 30 --duration $(DURATION)

k8s-trino:           ## [N4] Client SQL Trino dans le cluster (catalogue iceberg)
	kubectl exec -it -n serving deploy/trino -- trino --catalog iceberg

k8s-spark:           ## [N4] Applications Spark (SparkApplication) et leurs pods
	@kubectl get sparkapplications -n processing
	@echo; kubectl get pods -n processing -l spark-role --show-labels=false

APP ?= stream-raw-to-silver
k8s-spark-logs:      ## [N4] Journaux du driver d'une application Spark (APP=stream-raw-to-silver)
	kubectl logs -n processing -f --tail=40 $$(kubectl get pods -n processing -l spark-role=driver,app.kubernetes.io/name=$(APP) -o name | tail -1)

k8s-start:           ## [N4] Redémarre le cluster minikube existant (après arrêt / redémarrage de WSL)
	minikube start --force        # --force : pilote docker utilisé en root (cf. D26)
	kubectl get nodes

k8s-status:          ## [N4] Pods, volumes et URL de la plateforme
	@kubectl get pods -A -l app.kubernetes.io/part-of=waba-platform -o wide
	@echo; kubectl get pvc -A
	@echo; kubectl get ingress -A -o custom-columns=NAMESPACE:.metadata.namespace,URL:.spec.rules[0].host

k8s-expose:          ## [N4] Rend les interfaces accessibles depuis Windows : http://<ui>.localtest.me (laisser tourner)
	@echo "Interfaces (port $(K8S_HTTP_PORT)) : kafka-ui, minio, generator, nifi/nifi, trino, airflow, superset  ->  http://<nom>.localtest.me:$(K8S_HTTP_PORT)   (Ctrl+C pour arrêter)"
	kubectl port-forward -n ingress-nginx svc/ingress-nginx-controller $(K8S_HTTP_PORT):80 --address 127.0.0.1

k8s-uninstall:       ## [N4] Désinstalle la plateforme (les volumes de données sont conservés)
	@read -p "Désinstaller la release $(K8S_RELEASE) (volumes conservés) ? [y/N] " ok && [ "$$ok" = "y" ] || { echo "annulé"; exit 1; }
	helm uninstall $(K8S_RELEASE) -n $(K8S_NS)
