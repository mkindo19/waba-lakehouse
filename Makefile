# Raccourcis (à lancer depuis WSL2 / Linux / macOS)
SHELL := /bin/bash
COMPOSE := docker compose
SPARK_SUBMIT := $(COMPOSE) exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
DATASET ?= all
COUNTRY ?=

.PHONY: help env up down clean ps logs referentials generate ingest replay trino test lint bronze silver gold report test-spark airflow-ui

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
