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

## D18 — Kafka (niveau 3) : KRaft, topics explicites, profil "streaming"
- **Kafka 3.9 en mode KRaft** (un nœud broker + contrôleur, sans Zookeeper) : l'énoncé autorise les deux ;
  Zookeeper est déprécié et supprimé dans Kafka 4. Facteur de réplication 1 (un seul broker en local) ;
  en production : 3 brokers minimum, réplication 3, `min.insync.replicas=2`.
- **Topics créés explicitement** par `kafka-init` (`infra/kafka/create-topics.sh`, idempotent) et
  `auto.create.topics.enable=false` : une faute de frappe dans un nom de topic produit une erreur au lieu d'un
  topic fantôme.
- **3 partitions par topic, clé = `country_code`** : ordre garanti par pays, parallélisme de lecture.
- **Rétention** : 7 jours pour `raw-*` / `silver-*` (rejouables), 30 jours pour les alertes `gold-*` et la DLQ
  (temps d'investigation).
- **Profil Docker Compose `streaming`** : Kafka, Kafka UI (et NiFi ensuite) ne démarrent qu'avec
  `make up-streaming`, pour ne pas consommer de mémoire quand on ne travaille que sur le batch.
- **Kafka UI sans authentification** : acceptable en local uniquement (port publié sur le poste) ; au niveau 4,
  accès derrière Keycloak.

## D19 — NiFi : flux « as code » (raw-landing -> Kafka)
- **NiFi 1.28** (dernière 1.x) plutôt que 2.x : HTTP possible en local sans gestion de certificats, et
  processeurs Kafka 2.6 / S3 inclus dans l'image. Migration 2.x (HTTPS + authentification) au niveau 4.
- **Le flux est un script versionné** (`infra/nifi/deploy_flow.py`, API REST, bibliothèque standard) exécuté
  par le container `nifi-init` : rejouable, relu en revue de code, identique sur tous les postes. L'interface
  NiFi sert à observer (files, provenance), pas à construire.
- **Chaîne** : ListS3 (toutes les 10 s, état mémorisé : chaque fichier émis une fois) -> UpdateAttribute
  (topic cible déduit du dossier : `bank_transactions/` -> `raw-bank-transactions`) -> RouteOnAttribute
  (seuls les 4 flux financiers ; les référentiels restent au batch) -> FetchS3Object -> UpdateRecord
  (CSV -> JSON + `ingestion_timestamp` + `source_file`) -> PublishKafkaRecord (1 ligne = 1 message).
- **Colonnes lues en texte** (« Use String Fields From Header ») : NiFi ne devine aucun type ; le typage et la
  validation sont faits par Spark Streaming (job 1), comme en batch. Un montant `12a` traverse NiFi et sera
  rejeté vers la DLQ par Spark.
- **Clé Kafka = `country_code`**, `acks=all` (accusé de réception du broker avant de considérer l'envoi réussi).
- **Back-pressure** : 10 000 FlowFiles / 500 Mo par file, 2 000 / 100 Mo avant Kafka. Si Kafka ralentit, la
  file se remplit puis NiFi arrête d'alimenter l'étape précédente (les fichiers restent dans MinIO).
- **Erreurs** : fichier introuvable ou CSV illisible -> topic `dlq-financial-events` (avec le nom du fichier en
  en-tête) ; échec d'envoi Kafka -> nouvelle tentative après pénalité de 30 s.

## D20 — Lecture concurrente de raw-landing (NiFi + batch)
- NiFi (temps réel) et Spark batch (toutes les 10 min) lisent le même bucket ; le batch **archive** ensuite.
- **Risque** : archiver un fichier que NiFi n'a pas encore téléchargé. **Parade** : le batch n'archive qu'un
  fichier déposé depuis plus de `ARCHIVE_MIN_AGE_SECONDS` (120 s par défaut, `common/landing.py`) ; les plus
  récents sont archivés au passage suivant (déjà ingérés : le MERGE évite tout doublon).
- En production : notifications d'événements S3 (bucket notification -> Kafka) plutôt qu'un listing périodique.

## D21 — Job streaming 1 (raw -> silver)
- **Tables Iceberg séparées** `silver.rt_*` : la couche batch réécrit ses partitions (`overwritePartitions`) et
  effacerait les lignes du streaming ; deux écrivains sur une table = conflits de commit. Principe Lambda : vue
  temps réel distincte, remplacée par la vue batch quand celle-ci passe.
- **Mêmes règles de qualité qu'en batch** (`common/quality.py`), hors contrôles de clés étrangères (jointure
  avec 1,3 M de lignes de référentiels à chaque micro-lot : trop coûteux pour un gain faible, le batch les vérifie).
  Rejets -> `dlq-financial-events` avec motif et message d'origine intact.
- **Dédoublonnage 10 min** sur l'identifiant (`dropDuplicatesWithinWatermark`), watermark sur l'**heure d'arrivée
  Kafka** et non sur l'heure métier : le générateur simule des journées passées, toutes « en retard » au sens
  d'un watermark métier, qui seraient donc jetées.
- **Double sink** (`foreachBatch`) : Kafka `silver-*` (au moins une fois : un micro-lot rejoué après panne peut
  republier ; le job 2 dédoublonne ses alertes) et Iceberg par **MERGE** sur l'identifiant (idempotent).
- **Checkpoints** sur un volume Docker partagé driver / executors (offsets + état du dédoublonnage) ;
  en production : stockage objet ou HDFS. Petits fichiers Iceberg (un commit par micro-lot) : compaction
  périodique `rewrite_data_files` à planifier (perspective).
- **Ressources** : chaque job streaming réserve 1 cœur / 1 Go (`spark.cores.max=1`) en permanence ; le worker
  passe à 8 cœurs / 10 Go pour que les jobs batch (4 cœurs) gardent leur place.

## D22 — Catalogue SQLite et écritures concurrentes ; publication Kafka « effectivement une fois »
- **Constat (vécu)** : les 4 requêtes du job 1 committent en parallèle ; le catalogue REST, adossé à SQLite
  (D6), refuse les écritures simultanées (`ServiceFailureException: UncheckedSQLException`). La requête échoue,
  le container redémarre et rejoue le micro-lot : les topics `silver-*` ont reçu 17 fois les mêmes messages.
- **Parades** : (1) verrou dans le driver, un commit Iceberg à la fois, et nouvelles tentatives avec attente
  croissante (autres écrivains : jobs batch) ; (2) **registre des micro-lots terminés** (`_sinks_last_batch`,
  à côté du checkpoint) : un micro-lot rejoué avec un `batch_id` déjà écrit n'est pas republié ; ordre
  d'écriture Iceberg (idempotent) -> Kafka -> registre. Fenêtre résiduelle de doublon : panne entre l'envoi
  Kafka et l'écriture du registre (le job 2 dédoublonne ses alertes).
- **Cause exacte (logs du serveur `iceberg-rest`)** : `SQLITE_BUSY - database is locked`. Une fois une
  connexion bloquée, même le batch seul échouait (`CommitStateUnknownException`) jusqu'au redémarrage du
  catalogue. **Réglage** : SQLite en mode **WAL** (lectures et écriture simultanées) et **`busy_timeout=30000`**
  (un écrivain attend la libération du verrou au lieu d'échouer), via l'URI JDBC du catalogue.
- **En production** : catalogue sur PostgreSQL (Polaris / Lakekeeper) et producteur Kafka transactionnel.

## D23 — Job streaming 2 (silver -> gold : fraude, AML, liquidité)
- **5 requêtes** dans une application (1 cœur / 1 Go) : `bursts` et `liquidity` (agrégats sur fenêtres glissantes
  5 min / pas 1 min, avec état), `aml`, `unusual_country`, `claims` (sans état, événement par événement).
  Code des règles dans `common/fraud.py` (fonctions pures, testées), le job ne fait qu'assembler.
- **Heure métier** (`event_time`) pour les fenêtres, watermark **2 jours** (`STREAM_EVENT_WATERMARK`) : une rafale
  s'apprécie sur l'heure des opérations, pas sur l'heure d'arrivée. Le générateur horodate ses lots sur toute la
  journée en cours ; un lot rejoué de plus de 2 jours est ignoré par ces deux règles (le batch le couvre).
- **Une rafale = une alerte** : une rafale tombe dans jusqu'à 5 fenêtres glissantes. Les fenêtres d'un même compte
  qui se recoupent sont fusionnées (« îlots »), puis rattachées à l'alerte déjà émise si les périodes se
  chevauchent : l'alerte est **mise à jour** (`alert_status = UPDATED`, plus de transactions), pas dupliquée.
- **Seuils** : rafale = au moins 2 transactions réussies > 500 000 XOF (équivalent EUR pour le Ghana) ; sinistre >
  3 x prime annuelle, prime annuelle = 12 x solde du compte `INSURANCE_POLICY` (prime mensuelle dans le
  générateur) ; AML = virement réussi > 1 000 000 XOF / 5 000 GHS (indicateur posé par le job 1).
- **Liquidité** (l'énoncé laisse la mesure ouverte) : dépôts de référence par pays = somme des soldes des comptes
  courants, épargne et wallets actifs (`silver.accounts`) ; taux de couverture = (dépôts - sorties nettes de la
  fenêtre) / dépôts ; alerte sous **98 %** (`LIQUIDITY_MIN_COVERAGE`), critique sous 90 %. Sorties nettes =
  retraits, virements, paiements - dépôts, opérations réussies. Hypothèse de démonstration, à calibrer avec la
  trésorerie (ratio de liquidité BCEAO sur données réelles).
- **Référentiels** (clients, comptes) lus dans Iceberg `silver.*` et gardés en cache 10 min (jointure
  stream-static) : un nouveau client est pris en compte au plus tard 10 min après le batch silver.
- **Publication** : seules les alertes nouvelles ou aggravées sont publiées (comparaison avec `gold.rt_*`). Ordre :
  **Kafka puis MERGE Iceberg** : une panne entre les deux republie la même alerte (même `alert_id`, clé du message)
  au lieu de la perdre. Au moins une fois ; les consommateurs dédoublonnent sur la clé.
- **Test reproductible** : `make alert-test` publie 5 transactions horodatées maintenant (rafale + AML + liquidité),
  marquées `ALERT-TEST-*` ; R2 et R3 sont vérifiées sur les anomalies injectées par le générateur (0,5 %).
