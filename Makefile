# Raccourcis (à lancer depuis WSL2 / Linux / macOS)
SHELL := /bin/bash
COMPOSE := docker compose
SPARK_SUBMIT := $(COMPOSE) exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077
DATASET ?= all
COUNTRY ?=

.PHONY: help env up down clean ps logs referentials generate ingest replay trino test lint

help:                ## Liste des commandes
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

env:                 ## Crée .env à partir de .env.example (secrets aléatoires)
	@test -f .env && echo ".env existe déjà" || ( \
	  sed -e "s/change-me-admin-password/$$(openssl rand -hex 16)/" \
	      -e "s/change-me-app-secret-min-8-chars/$$(openssl rand -hex 20)/" \
	      -e "s/change-me-generate-with-openssl-rand-hex-32/$$(openssl rand -hex 32)/" \
	      .env.example > .env && echo ".env créé")

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

test:                ## Tests unitaires du générateur
	cd generator && python -m pytest -q tests

lint:                ## Vérification PEP 8 (ruff)
	ruff check generator spark
