# WABA Group - Plateforme Analytique Financière Multi-Pays

Challenge Data Engineer Artefact : un Data Lakehouse (batch + streaming) pour un groupe
fictif de banque-assurance présent dans 8 pays d'Afrique de l'Ouest.

> **Avancement** : Niveau 1 ✅ validé · Niveau 2 ✅ validé (Airflow + médaillon, backfill par pays) · Niveau 3 ✅ streaming (NiFi, Kafka, Spark Streaming, Lambda) · Niveau 4 : à venir
> (voir [docs/PLAN_7_JOURS.md](docs/PLAN_7_JOURS.md)).

## Architecture — Niveau 1

```
 Streamlit (générateur) ──CSV──► MinIO raw-landing ──► Spark (validation, MERGE) ──► Iceberg lakehouse.raw.*
                                        │                         │                          │
                                        └──── archive ◄───────────┘                     Trino (SQL)
                                                          catalogue Iceberg REST (partagé Spark/Trino)
```

| Service | Rôle | URL locale |
|---|---|---|
| `generator` | Streamlit : génération des données simulées (one-time / continu) | http://localhost:8501 |
| `minio` | Stockage objet S3 : `raw-landing`, `lakehouse`, `archive` | http://localhost:9001 (console) |
| `iceberg-rest` | Catalogue Iceberg (REST) partagé par Spark et Trino | http://localhost:8181 |
| `spark-master` / `spark-worker` | Traitement PySpark | http://localhost:8090 |
| `trino` | Requêtes SQL sur les tables Iceberg | http://localhost:8088 (`TRINO_PORT`) |

## Niveau 2 — Orchestration et architecture médaillon

```
raw-landing ─► raw.* ─► bronze.* ─► silver.* ─► gold.* (7 KPIs) ─► gold.regulatory_daily_report (J+1)
   └──────────── dag_ingest_raw ───┘   dag_bronze_to_silver   dag_silver_to_gold   dag_regulatory_report
```

| Couche | Contenu | Partitionnement |
|---|---|---|
| `bronze.*` | copie validée de raw, sans transformation métier | pays + date d'ingestion |
| `silver.*` | dédoublonné, joint aux référentiels, montants en EUR, nulls normalisés, aberrants signalés | pays + mois |
| `gold.*` | 7 KPIs + rapport réglementaire | pays (+ date de rapport) |

| DAG | Déclenchement | Rôle |
|---|---|---|
| `dag_ingest_raw` | toutes les 10 min (si nouveaux fichiers) | landing → raw → bronze |
| `dag_bronze_to_silver` | après `dag_ingest_raw` | référentiels puis 4 flux en Silver |
| `dag_silver_to_gold` | après `dag_bronze_to_silver` | 7 tables Gold |
| `dag_regulatory_report` | chaque jour 00h30 UTC | agrégats BCEAO/CIMA de J-1 + export CSV |

Interface Airflow : http://localhost:8082 (identifiants `AIRFLOW_ADMIN_*` dans `.env`).
Backfill d'un pays : *Trigger DAG w/ config* → `{"country_code": "SN"}`.
Exécution manuelle sans Airflow : `make bronze`, `make silver`, `make gold`, `make report DATE=2026-06-30`.
Contrôles : `docker compose exec -T trino trino --catalog iceberg < sql/level2_checks.sql`
(backfill : `sql/level2_backfill_check.sql`, à lancer avant et après).

## Niveau 3 — Streaming temps réel et architecture Lambda

```
MinIO raw-landing ─► NiFi ─► raw-* ─► Job 1 (Spark) ─┬─► silver-* (Kafka) ─► Job 2 (Spark) ─┬─► gold-fraud-alerts
  (ListS3 10 s)   1 ligne CSV           validation,   │   + silver.rt_* (Iceberg)  fenêtres    ├─► gold-aml-events
                  = 1 message JSON      dédoublonnage └─► dlq-financial-events     5 min/1 min └─► gold-liquidity-alerts
                                        10 min, EUR                                 (+ gold.rt_* Iceberg)
Trino : iceberg.gold.* (batch) ⨝ kafka.default."silver-bank-transactions" (temps réel) = requête Lambda
```

| Élément | Fichiers | Points clés |
|---|---|---|
| Kafka 3.9 (KRaft) + Kafka UI | `infra/kafka/create-topics.sh` | 11 topics créés par script, 3 partitions, clé = pays |
| NiFi 1.28 | `infra/nifi/deploy_flow.py` | flux « as code » via l'API REST, back-pressure, erreurs -> DLQ |
| Job 1 raw -> silver | `spark/jobs/stream_raw_to_silver.py`, `common/streaming.py` | règles qualité du batch, DLQ, dédoublonnage 10 min, double sink |
| Job 2 silver -> gold | `spark/jobs/stream_fraud_aml.py`, `common/fraud.py` | 3 règles de fraude, AML, liquidité ; une rafale = une alerte |
| Requête Lambda | `trino/catalog/kafka.properties`, `trino/kafka/*.json`, `sql/level3_lambda.sql` | énoncé + version sans double comptage |

Démarrage et tests :

```bash
make up-streaming          # stack + Kafka, Kafka UI (http://localhost:8085), NiFi (http://localhost:8089/nifi), jobs streaming
make nifi-deploy           # déploie le flux NiFi
docker compose exec generator python -m waba_gen stream --rows-per-tick 2000 --duration 90   # flux continu
make dlq-test              # message invalide -> dlq-financial-events
make alert-test            # rafale + AML + liquidité horodatées maintenant
make gold-peek             # nombre de messages gold-*
make lambda                # requête Lambda unifiée (Trino)
make check3                # latence NiFi -> Kafka, double sink, DLQ, alertes, vérité terrain
```

Journaux : `make stream-logs` (job 1), `make stream2-logs` (job 2). Décisions : D18 à D24.

## Prérequis

| Outil | Version testée |
|---|---|
| Docker Engine / Docker Desktop | ≥ 24 (Compose v2) |
| RAM allouée à Docker | ≥ 12 Go pour le niveau 1 (32 Go conseillés pour l'ensemble) |
| make, openssl, git | versions récentes |

**Windows (WSL2)** — deux réglages importants :

1. Cloner le dépôt **dans WSL** (`cd ~ && git clone ...`), pas dans `/mnt/c/...` : les volumes
   montés depuis Windows sont très lents.
2. Donner assez de mémoire à WSL : fichier `%UserProfile%\.wslconfig` puis `wsl --shutdown` :
   ```ini
   [wsl2]
   memory=24GB
   processors=8
   ```

## Démarrage (from scratch)

```bash
git clone <url-du-depot> waba-lakehouse && cd waba-lakehouse

make env          # crée .env depuis .env.example avec des secrets aléatoires
make up           # build + démarrage de toute la stack (≈ 5 min au premier lancement)
make ps           # tous les services doivent être "healthy" (minio-init : "exited (0)")
```

Sans `make` : `cp .env.example .env`, remplacer les valeurs `change-me`, puis
`docker compose up -d --build`.

### 1. Générer les données

Ouvrir http://localhost:8501 :

1. onglet **Référentiels** → *Générer les référentiels* (500 000 clients, 800 000 comptes,
   200 agences, 50 produits ; ≈ 30 s) ;
2. onglet **Génération one-time** → choisir pays / lignes métier / période → *Générer* ;
3. onglet **Génération continue** → *Démarrer* : un lot toutes les 10 à 60 s.

Équivalent en ligne de commande : `make referentials` puis `make generate`.

### 2. Ingérer dans Iceberg

```bash
make ingest                                     # tous les jeux de données, tous les pays
make ingest DATASET=bank_transactions COUNTRY=CI   # un seul flux, un seul pays
```

### 3. Interroger avec Trino

```bash
make trino
trino:raw> SHOW TABLES;
```

Les requêtes de contrôle (soldes par pays, volumes, comptages, intégrité, idempotence, PII)
sont dans [sql/level1_checks.sql](sql/level1_checks.sql).

### 4. Vérifier l'idempotence

```bash
make replay       # remet les fichiers archivés dans raw-landing
make ingest       # ré-ingestion
# puis dans Trino : la requête n°5 de sql/level1_checks.sql doit renvoyer 0 ligne
```

## Structure du dépôt

```
generator/          Application Streamlit + package waba_gen (génération, dépôt MinIO) + tests
spark/              Image Spark (Iceberg, S3) et jobs PySpark
  jobs/ingest_raw.py      Ingestion raw-landing -> lakehouse.raw.*
  jobs/common/            Schémas, règles qualité, pseudonymisation, archivage, logs JSON
trino/catalog/      Catalogue Iceberg de Trino
infra/minio/        Initialisation des buckets et du compte applicatif
sql/                Requêtes de vérification
docs/               Plan, journal des décisions, write-up
```

## Données générées

| Fichier | Volume par défaut | Chemin dans `raw-landing` |
|---|---|---|
| customers / accounts / branches / products | 500 k / 800 k / 200 / 50 | `referentials/<nom>/<nom>.csv` |
| Transactions bancaires | 10 000 | `bank_transactions/<CC>/bank_txn_<CC>_<AAAAMMJJ>_<NN>.csv` |
| Opérations d'assurance | 5 000 | `insurance_operations/<CC>/insurance_ops_...csv` |
| Paiements mobile money | 20 000 | `mobile_money_payments/<CC>/mobile_money_...csv` |
| Remboursements de crédit | 5 000 | `loan_repayments/<CC>/loan_repayments_...csv` |

Garanties : clés étrangères toujours valides (tirées dans les référentiels du même pays), XOF en
zone UEMOA et GHS au Ghana, statuts ~92/5/3 %, NPL 3-8 % et loss ratio 50-85 % selon les pays,
quelques anomalies injectées pour tester la détection de fraude.

## Qualité et sécurité

- **Aucun secret dans le code** : tout passe par `.env` (non versionné) ; voir `.env.example`.
- **Compte MinIO dédié** aux applications (le compte root ne sert qu'à l'administration).
- **Données personnelles** : IBAN et numéros de compte pseudonymisés (SHA-256 salé) et masqués
  dès l'écriture dans Iceberg.
- **Rejets tracés** : chaque ligne invalide est conservée dans `raw.rejected_records` avec son
  motif (format, devise, clé orpheline, montant...).
- **Logs JSON structurés** dans tous les composants Python.
- Tests : `make test` · style PEP 8 : `make lint` (ruff).

## Décisions et limites connues

Voir [docs/DECISIONS.md](docs/DECISIONS.md).
