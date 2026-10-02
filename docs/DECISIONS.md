# Journal des décisions (matière première du write-up)

Chaque choix non trivial est noté ici au fil de l'eau : contexte, décision, alternative écartée.

## D1 — Guinée (GN) et devise XOF
- **Constat** : dans la réalité, la Guinée n'est pas membre de l'UEMOA et utilise le franc guinéen (GNF).
- **Décision** : on suit l'énoncé (GN en XOF) pour respecter les critères d'évaluation ; la table
  des devises est centralisée (`CURRENCY_MAP`) et passer GN en GNF ne toucherait qu'une ligne.

## D2 — Colonnes ajoutées aux schémas de l'annexe
- `accounts` : `entity_type` (exigé sur toutes les tables), `product_id` (nécessaire au « Top 10
  produits » du dashboard Superset), `account_number` et `iban` (l'énoncé impose leur masquage
  et leur tag PII dans OpenMetadata : il faut donc qu'ils existent).
- `mobile_money_payments` : `country_code` (= pays émetteur), exigé sur toutes les tables.
- Pas d'IBAN au Ghana (le Ghana n'utilise pas l'IBAN) → `NULL`.

## D3 — Pseudonymisation dès la couche raw
- Les valeurs en clair n'existent que dans `raw-landing` (zone d'atterrissage). Dès l'écriture
  Iceberg, `iban` / `account_number` sont remplacés par `<col>_hash` (SHA-256 salé, sel secret
  en variable d'environnement) et `<col>_masked`.
- Le hash permet les jointures et le comptage de valeurs distinctes sans exposer la donnée.

## D4 — Idempotence
- `MERGE INTO ... ON key AND country_code` : insertion seule pour les événements financiers
  (immuables), upsert (SCD1) pour les référentiels.
- Dédoublonnage intra-lot avant MERGE (même clé dans deux fichiers).
- Les rejets sont eux aussi idempotents (MERGE sur un hash de la ligne).

## D5 — Archivage non destructif
- Copie vers `archive` → vérification de la taille → suppression de la source.
  Un échec laisse le fichier en place : il sera re-traité sans doublon (cf. D4).
- Versioning activé sur le bucket `archive`.

## D6 — Catalogue Iceberg REST (tabulario/iceberg-rest, backend SQLite sur volume)
- Choix simple et partagé par Spark et Trino (même catalogue → mêmes tables).
- **Limite assumée** : SQLite = un seul nœud. En production : Apache Polaris / Lakekeeper
  avec PostgreSQL et authentification OAuth2.

## D7 — Calibrage des données simulées
- NPL : chaque compte prêt a un profil de risque stable (hash CRC32 de l'`account_id`), en
  défaut si sous le taux cible du pays → NPL cohérent d'un fichier à l'autre, 3,4 % à 7,5 %.
- Loss ratio : recalage par (pays, mois) vers une cible de 55 % à 81 % (certains pays au-dessus
  du seuil CIMA de 70 %, pour que le dashboard de conformité ait des alertes à montrer).
- Anomalies injectées (~0,5 %) : rafales de retraits > 500 000 XOF, clients mobile money
  émettant depuis un pays inhabituel, sinistres > 3x la prime annuelle.

## D8 — Montants
- XOF sans décimale (le franc CFA n'a pas de centimes), GHS à 2 décimales.

## D9 — Disparition des images MinIO (Docker Hub)
- **Constat** : MinIO a retiré ses images communautaires de Docker Hub (`pull access denied`).
- **Décision** : image `coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` (reconstruction de la
  dernière release communautaire, contient `minio` et `mc`), paramétrable dans `.env`.
- **Limite** : plus de correctifs de sécurité. En production : migrer vers un stockage S3
  maintenu (RustFS, Garage, SeaweedFS, Ceph RGW) ou un S3 managé. Le code n'utilise que
  l'API S3 standard (boto3, S3FileIO, s3a) : la migration ne touche que la configuration.

## D10 — MERGE Iceberg et source non déterministe
- **Constat** : Iceberg refuse un MERGE dont la source contient `input_file_name()`, `current_timestamp()` ou
  une fenêtre `row_number()` (`INVALID_NON_DETERMINISTIC_EXPRESSIONS`).
- **Décision** : horodatage calculé une fois côté driver (littéral) + source figée par un checkpoint **fiable**
  écrit dans MinIO (`s3a://lakehouse/_spark_checkpoints`). Un `localCheckpoint` a été écarté : il vit en mémoire
  des executors et disparaît si un executor meurt (vécu : exit code 52 = OutOfMemory avec 1 Go par défaut).
- **Ressources** : executor 4 Go / driver 2 Go fixés dans `spark/conf/spark-defaults.conf`.

## D11 — raw.* (niveau 1) et bronze.* (niveau 2)
- `raw.*` = contrat du niveau 1 : miroir validé de raw-landing, partitionné par date **métier**.
- `bronze.*` = journal des chargements, partitionné par **date d'ingestion** (`ingestion_date`) : permet de
  rejouer/auditer un chargement donné. Chargement incrémental par watermark `_ingested_at` **par pays**
  (un backfill d'un seul pays ne fait pas avancer le watermark des autres) + MERGE idempotent.

## D12 — Silver et Gold : recalcul + écrasement de partitions
- Silver et Gold sont **recalculés** pour les pays demandés puis écrits avec `overwritePartitions()` :
  rejouer un job remplace les partitions concernées, sans doublon, et permet un backfill par pays.
- Un DataFrame vide n'écrase rien (protection contre l'effacement accidentel).
- Les transformations sont des **fonctions pures** (`common/transforms_*.py`), testées en local (`spark/tests`).

## D13 — Airflow
- Airflow 2.10.5 (LocalExecutor + PostgreSQL). Airflow 3 est sorti mais 2.10 reste la version la plus
  éprouvée avec le provider Spark ; migration identifiée comme perspective.
- Jobs lancés par `SparkSubmitOperator` : le **driver** tourne dans le container Airflow (Spark copié depuis
  l'image officielle, pas de `pip install pyspark`), les **executors** sur le worker Spark.
- Cluster Spark = **Connection** `spark_default` (variable d'environnement JSON) ; paramètres métier =
  **Variables** (`waba_ghs_per_eur`) : aucun identifiant ni paramètre en dur dans les DAGs.
- **Pool `spark_pool` à 1 slot** : un seul job Spark à la fois sur le worker (6 Go), pour éviter la saturation
  mémoire. En production : Spark sur Kubernetes (niveau 4) avec allocation dynamique.
- Chaînage `dag_ingest_raw → dag_bronze_to_silver → dag_silver_to_gold` par `TriggerDagRunOperator` qui
  transmet le paramètre `country_code` (backfill sélectif). Détection de nouveaux fichiers par
  `ShortCircuitOperator` (listing MinIO) toutes les 10 minutes plutôt qu'un sensor bloquant un slot.

## D14 — Conversion EUR
- XOF : parité fixe 1 EUR = 655,957 XOF. GHS : taux de référence **simulé** (14,6 GHS/EUR, cohérent avec le
  générateur), paramétrable ; en production, table de taux quotidiens. Les taux utilisés sont tracés dans
  `silver.fx_rates`.

## D15 — Définitions des KPIs (hypothèses documentées)
- **NPL** : un prêt est douteux sur le mois s'il a une échéance en défaut ou > 90 jours de retard ; ratio
  pondéré par l'encours (solde restant dû du compte prêt).
- **ARPC** : commissions (frais bancaires + mobile money réussis) + intérêts (encours × taux annuel / 12 par
  échéance honorée), divisés par les clients distincts actifs du mois sur ces produits.
- **Délai sinistres** : SLA contractuel supposé de 30 jours ouvrés (IARD) et 45 (Vie / prévoyance).
- **Régulateurs** : BCEAO / CIMA pour la zone UEMOA (Guinée incluse, cf. D1) ; au Ghana, Bank of Ghana (BOG)
  et National Insurance Commission (NIC) — le Ghana n'est ni membre de l'UEMOA ni de la CIMA.

## D16 — Montages de dossiers sous WSL2 / Docker et recréation des containers
- **Constat** : après une resynchronisation des fichiers ou un redémarrage de WSL, un container peut garder une
  vue **vide ou périmée** d'un dossier monté (`./trino/catalog`, `./airflow/dags`). Symptômes vécus : Trino sans
  catalogue `iceberg`, DAGs marqués inactifs par le scheduler.
- **Décision** : en cas de doute, **recréer** le service (`docker compose up -d --force-recreate <service>`) — un
  simple redémarrage ne suffit pas. Vérification : `docker compose exec <service> ls <dossier monté>`.
- En production (Kubernetes, niveau 4) : configuration en ConfigMap et DAGs synchronisés par git-sync.

## D17 — Backfill sélectif par pays
- Les DAGs exposent un paramètre `country_code` (`ALL` ou un code pays, liste fermée) transmis aux jobs Spark et
  propagé d'un DAG à l'autre par `TriggerDagRunOperator`.
- Silver/Gold réécrivent uniquement les partitions du pays (`overwritePartitions`, partitionnement par pays) :
  un backfill SN ne touche ni les données ni les snapshots des autres pays. Contrôle :
  `sql/level2_backfill_check.sql` (volumes identiques avant/après, partitions réécrites = SN uniquement).
