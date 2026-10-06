# Raccourcis (à lancer depuis WSL2 / Linux / macOS)
SHELL := /bin/bash
COMPOSE := docker compose
SPARK_SUBMIT := $(COMPOSE) exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
DATASET ?= all
COUNTRY ?=

.PHONY: help env up down clean ps logs referentials generate ingest replay trino test lint bronze silver gold report test-spark airflow-ui up-streaming topics kafka-smoke nifi-deploy nifi-redeploy kafka-peek stream-logs dlq-test stream-reset stream2-logs alert-test gold-peek stream2-reset

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
	ruff check generator spark airflow

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
