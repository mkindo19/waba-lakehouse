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

## D24 — Trino : catalogue Kafka et requête Lambda unifiée
- **Catalogue `kafka`** (`trino/catalog/kafka.properties`) : les topics deviennent des tables en lecture seule ;
  le schéma JSON de chaque topic est décrit dans `trino/kafka/<topic>.json` (champ -> colonne typée, dates au format
  ISO 8601). Colonnes techniques visibles (`_timestamp`, `_partition_offset`...) : elles servent à mesurer la latence.
- **Requête de l'énoncé exécutée telle quelle** (`sql/level3_lambda.sql`, partie 1) : elle tourne sans erreur, mais
  joint des lignes de grains différents (une transaction côté Kafka, une ligne entité x type côté gold) et additionne
  batch et temps réel sur les mêmes jours : montants comptés plusieurs fois.
- **Version corrigée** (partie 2) : agrégation par pays et jour des deux côtés, puis le temps réel ne couvre que les
  jours postérieurs à la dernière date gold du pays. C'est le principe Lambda : la vue batch, exacte, remplace la vue
  temps réel dès qu'elle passe ; pas de double comptage. Transactions de test (`ALERT-TEST-*`) exclues.
- **Latence NiFi -> Kafka** mesurée en SQL : `_timestamp` (écriture dans Kafka) - `ingestion_timestamp` (horodatage
  NiFi), plus au plus 10 s de détection du fichier par ListS3 (`sql/level3_checks.sql`).
- **Limite** : le connecteur Kafka relit tout le topic à chaque requête (pas d'index) ; acceptable avec une rétention
  de 7 jours. En production, la vue temps réel interrogée serait plutôt `silver.rt_*` (Iceberg) ou un moteur OLAP
  temps réel (Pinot, Druid).

## D25 — Conflit de commit Iceberg entre requêtes d'une même application (vécu)
- **Constat** : `gold.rt_fraud_alerts` est alimentée par 3 requêtes du job 2 (rafales, pays inhabituel, sinistres).
  Malgré le verrou (un commit à la fois), le job a échoué 7 fois au démarrage : `ValidationException: Found
  conflicting files`. Le MERGE était planifié sur un **snapshot en cache** (cache de tables de Spark) alors qu'une
  autre requête venait de commiter : Iceberg (isolation sérialisable) refuse ce commit.
- **Conséquence** : publication Kafka avant Iceberg (D23) -> chaque échec a republié ses alertes au redémarrage :
  109 messages pour 49 alertes « pays inhabituel ». Pas de perte, mais des doublons (même `alert_id`).
- **Correction** : `REFRESH TABLE` juste avant chaque MERGE (sous le verrou) et `ValidationException` ajoutée aux
  erreurs rejouables. Topics gold-* et tables gold.rt_* reconstruits (`make stream2-reset`).
- **Leçon** : les consommateurs de `gold-*` doivent dédoublonner sur la clé du message ; en production, producteur
  Kafka transactionnel ou topic compacté sur `alert_id`.

## D26 — Kubernetes : un chart Helm « maison », minikube, secrets hors dépôt
- **Cluster** : minikube (pilote Docker, 8 cœurs / 18 Go, addons ingress et metrics-server), accepté par l'énoncé.
  Docker Compose reste l'environnement des niveaux 1-3 ; on ne fait pas tourner les deux en même temps (mémoire).
- **Un seul chart parapluie `helm/waba-platform`** : `make k8s-install` = un seul `helm upgrade --install`.
  Les charts publics (Airflow, Superset, Trino, OpenMetadata...) installent tout dans le namespace de la release
  et ne savent pas, pour la plupart, cibler un autre namespace : incompatible avec « un namespace par domaine ».
  Le chart déclare donc ses propres manifestes, chacun dans son namespace (lisibles, probes et ressources
  maîtrisées). Exception prévue : Spark Operator (CRD + webhook), installé comme dépendance dans `processing`.
- **Namespaces** : ingestion (NiFi, Kafka), processing (MinIO, Iceberg, Spark, Airflow ; namespace de la release),
  serving (Trino, Superset), governance (Keycloak, OpenMetadata), monitoring (Prometheus, Loki, Grafana).
- **Secrets** : aucun dans le chart. `helm/values-secrets.yaml` est généré depuis `.env`
  (`scripts/k8s_values_secrets.py`, droits 600, ignoré par git) ; le chart en fait un Secret `waba-secrets` par
  namespace, limité aux clés utiles (moindre privilège), lu par `secretKeyRef`. En production : External Secrets
  Operator + Vault (ou Sealed Secrets) pour ne jamais faire transiter de secret par le poste.
- **Données** : PVC pour MinIO, catalogue Iceberg, Kafka (puis bases Airflow, OpenMetadata...), annotés
  `helm.sh/resource-policy: keep` : un `helm uninstall` ne détruit pas les données.
- **Accès aux interfaces** : Ingress NGINX + noms `*.localtest.me` (DNS public qui répond 127.0.0.1 : rien à
  modifier dans le fichier hosts). Depuis Windows : `make k8s-expose` (port-forward du contrôleur Ingress).
- **Initialisations** (buckets MinIO, topics Kafka) : Jobs Helm `post-install/post-upgrade`, idempotents, mêmes
  scripts qu'en Docker Compose (copiés dans le chart par `make k8s-files`).

## D27 — Spark sur Kubernetes : Spark Operator et SparkApplication
- **Spark Operator** (Kubeflow, chart `~2.1.0`, sous-chart dans `processing`) remplace le cluster Spark standalone :
  chaque job est une ressource `SparkApplication` ; l'opérateur crée le pod driver, qui crée ses executors.
  Pas de master ni de worker permanents : la mémoire n'est consommée que par les jobs qui tournent.
- **Jobs streaming** : `restartPolicy: Always` (relance après 30 s en cas d'échec), checkpoints sur un PVC monté
  par le driver et les executors (`/opt/spark-checkpoints`), même code et même image qu'en Docker Compose.
- **Configuration** : en mode Kubernetes le driver est lancé avec `--properties-file`, le `spark-defaults.conf`
  de l'image n'est pas lu : catalogue Iceberg et accès S3 sont repris dans `sparkConf` (helper `waba.sparkConf`).
- **Sondes du driver** (énoncé 4.1) via le modèle de pod (`driver.template`) : readiness = port RPC du driver,
  liveness = UI Spark. **Métriques** : servlet Prometheus natif de Spark
  (`/metrics/prometheus/`, annotations `prometheus.io/*`) pour la supervision 4.4.
- **CRD** : Helm n'installe les CRD d'un sous-chart qu'à la première installation ; `make k8s-crds` les applique
  (idempotent) avant chaque `helm upgrade`.
- **Webhook `failurePolicy: Ignore`** (vécu) : à l'installation, la SparkApplication est créée avant que le webhook
  de l'opérateur réponde (`connection refused`) et Helm échoue. Avec `Ignore`, la création passe ; un pod Spark
  créé pendant cette fenêtre (sans volumes ni secrets) échoue puis est relancé automatiquement.
- **Readiness du driver = port RPC 7078** (vécu) : une première sonde HTTP sur l'UI (délai 30 s) rendait le driver
  « non prêt » ; or le service DNS headless du driver ne publie que les pods prêts : les executors, créés 3 s après,
  échouaient (`UnknownHostException ...-driver-svc`) et Spark abandonnait après 3 échecs. La sonde vérifie
  maintenant ce dont les executors ont besoin (port RPC ouvert) et passe en 1-2 s ; liveness = UI Spark.
- **Job 2 (fraude/AML)** activé après Airflow (4b.3) : il a besoin des référentiels `silver.customers/accounts`.

## D28 — Airflow sur Kubernetes : DAGs inchangés, jobs batch en SparkApplication
- **Mêmes DAGs** qu'en Docker Compose : seule `spark_task()` (waba_common) change de comportement selon
  `WABA_SPARK_MODE` : `standalone` -> `SparkSubmitOperator` (Compose), `k8s` -> `SparkApplicationOperator`.
- **Opérateur maison** (`airflow/dags/waba_sparkapp.py`, ~100 lignes) : crée la `SparkApplication`, suit
  `status.applicationState` jusqu'à `COMPLETED` / `FAILED`, recopie la fin du journal du driver dans Airflow, et la
  supprime si la tâche est interrompue (timeout, arrêt manuel). Écarté : `SparkKubernetesOperator` (provider
  cncf.kubernetes 10.1) retrouve le pod driver grâce à des étiquettes qu'il n'ajoute pas au modèle ; sans elles il
  ne trouve pas le pod et la tâche peut être marquée réussie alors que Spark a échoué (lu dans son code source).
- **Modèle de SparkApplication rendu par Helm** (ConfigMap `airflow-spark-template`, helper `waba.sparkBatchApp`) :
  image, catalogue Iceberg, S3, secrets et ressources sont décrits une seule fois dans le chart, partagés avec le
  streaming ; le DAG n'apporte que le script, ses arguments et des étiquettes (`waba/dag-id`, `waba/task-id`,
  `waba/country`) reprises par les pods (métriques et journaux par pays, niveau 4.4).
- **Nettoyage** : `timeToLiveSeconds: 3600` -> Spark Operator supprime l'application (et ses pods) 1 h après la
  fin : le temps d'examiner un échec. Les nouvelles tentatives sont gérées par Airflow (`restartPolicy: Never`).
- **Droits minimaux** : compte de service `airflow` + `Role` limité au namespace `processing`
  (sparkapplications : create/get/list/watch/delete ; pods et pods/log : lecture).
- **Démarrage sans hook Helm** : avec `--wait`, Helm attend que les pods soient prêts avant les hooks
  post-install, or le webserver a besoin d'une base migrée pour être prêt. La migration (+ compte admin + pool)
  se fait donc dans un initContainer du scheduler (idempotent) ; le webserver attend
  (`airflow db check-migrations`).
- **DAGs** : ConfigMap généré depuis `airflow/dags` (`make k8s-files`), copié au démarrage dans un dossier ordinaire
  (un ConfigMap monté contient des liens `..data` que le scanner de DAGs lirait en double). Une annotation
  `checksum/dags` redémarre le scheduler quand un DAG change. En production : git-sync ou image versionnée.
- **Journaux des tâches** : écrits par le scheduler (LocalExecutor) et lus par le webserver sur le port 8793 ;
  le nom d'hôte d'un pod n'étant pas résolu par le DNS, Airflow publie l'IP du pod (`hostname_callable`).
  Volume `emptyDir` : l'historique durable des journaux passera par Loki (4.4).
- **Base de métadonnées** : PostgreSQL 16 sur PVC `airflow-db-data` (`keep`), mot de passe injecté par
  `secretKeyRef` puis substitué dans l'URL SQLAlchemy par Kubernetes (`$(AIRFLOW_DB_PASSWORD)`).
- **Mémoire** : un seul job batch à la fois (pool `spark_pool` = 1 slot) ; driver 1g, executor 2g de tas Java
  + `memoryOverhead` 1g. Vécu : avec un overhead de 0.2 x 2g (410 Mo), l'executor a été tué par Kubernetes
  (`OOMKilled`, code 137) pendant `ingest_raw` : la limite du pod (= tas + overhead) doit aussi contenir les
  workers Python (un par cœur) et la mémoire hors tas de la JVM. En Docker Compose, le worker n'avait pas de
  limite de conteneur : le problème n'apparaissait pas. Après 3 executors perdus, Spark arrête l'application ;
  Spark Operator la passe en `FAILED`, l'opérateur Airflow échoue et Airflow relance la tâche (retries).
- **DAGs créés en pause** (`DAGS_ARE_PAUSED_AT_CREATION`, comme en Compose) (vécu) : un `TriggerDagRunOperator` crée
  bien le run du DAG suivant, mais il reste `queued` tant que ce DAG est en pause. `make k8s-dags-on` active la chaîne
  ingest -> silver -> gold ; `dag_regulatory_report` est activé séparément (alerte J+1 06:00, 4.4).
- **Job 2 (fraude/AML)** activé une fois `silver.customers` / `silver.accounts` produits par les DAGs ; avant, ses
  référentiels n'existent pas et la SparkApplication échouerait en boucle (`restartPolicy: Always`).

## D29 — Superset : 3 dashboards « as code » sur Trino
- **Déploiement** (namespace `serving`) : image `waba/superset` = image officielle 4.1.1 + pilote `trino[sqlalchemy]`
  + Authlib (SSO Keycloak, 4.3) ; base de métadonnées PostgreSQL sur PVC (`keep`) ; secrets (clé de session, mots de
  passe) dans `waba-secrets`. Connexion SQLAlchemy : `trino://superset@trino.serving.svc.cluster.local:8080/iceberg`.
- **Dashboards as code** : `superset/dashboards/build_assets.py` décrit en Python les jeux de données, les 12
  graphiques et les 3 dashboards, et produit le ZIP au format d'import v1 de Superset. Identifiants stables (uuid5) :
  un ré-import met à jour au lieu de dupliquer. Le script est monté par ConfigMap et exécuté au démarrage du pod :
  modifier un graphique = modifier le fichier puis `make k8s-install` (pas de clic dans l'interface, rien à perdre).
- **Import en écrasement** (vécu, en test local) : `superset import-dashboards` n'écrase que les dashboards ; les
  graphiques et jeux de données déjà présents ne sont jamais mis à jour. `import_assets.py` utilise la commande de
  l'API `/api/v1/assets/import` (`ImportAssetsCommand`), qui écrase tout.
- **Jeux de données** : tables gold quand le KPI existe (ARPC, loss ratio, délais sinistres, corridors) ; requêtes SQL
  (jeux « virtuels ») pour les vues qui combinent plusieurs tables : revenu par métier (commissions + intérêts +
  primes + frais mobile money), top 10 produits par pays (`ROW_NUMBER`), dernier NPL par pays et sa classe de risque,
  alertes AML (batch silver + temps réel `gold.rt_aml_events`). Toutes ont une colonne `country_code` : le filtre
  natif « Pays » de chaque dashboard s'applique à tous ses graphiques (vérifié avec plusieurs jeux de données).
- **Seuils réglementaires visibles** : NPL coloré par classe (vert < 3 %, orange 3-5 %, rouge > 5 % BCEAO, couleurs
  fixées dans le dashboard) ; loss ratio en barres + ligne du seuil CIMA 70 % (graphique mixte) ; délai des
  sinistres en barres + ligne du SLA par branche (IARD 30 j, VIE 45 j).
- **Carte** : Superset ne fournit pas de fond « Afrique de l'Ouest » (cartes par pays seulement) ; la World Map
  (codes ISO-2) colore les 8 pays. Alternative écartée : deck.gl + GeoJSON maison (fond de carte Mapbox requis).
- **Validation avant livraison** : Superset 4.1.1 lancé localement sur DuckDB (mêmes schémas `silver`/`gold`, données
  factices), import du paquet puis rendu des 3 dashboards dans Chromium (Playwright) : aucun graphique en erreur,
  filtre Pays appliqué partout. Correctifs issus de ce test : heure 0 affichée « <NULL> » par la heatmap (libellé
  « 00h ».. « 23h »), mode d'import en écrasement.
- **Pilote PostgreSQL** (vécu, au premier déploiement) : l'image officielle 4.1 « lean » ne contient aucun pilote de
  base, pas même `psycopg2` pour sa propre base de métadonnées (`ModuleNotFoundError`, initContainer en boucle). Le
  test local utilisait SQLite et ne pouvait pas le révéler : `psycopg2-binary` est ajouté à l'image `waba/superset`.
- **Liens de service Kubernetes** (vécu) : pour chaque Service du namespace, Kubernetes injecte des variables
  `<NOM>_PORT=tcp://IP:port`. Le Service s'appelant `superset`, le pod recevait `SUPERSET_PORT=tcp://…:8088`, que le
  script de démarrage de l'image utilise comme port d'écoute -> crash (« 'tcp' is not a valid port number »).
  Correctif : `enableServiceLinks: false` (la découverte passe par le DNS) et `SUPERSET_PORT=8088` explicite.
- **Limite connue de la World Map** (plugin « legacy » de Superset 4.1) : l'infobulle n'apparaît que sur les pays qui
  ont des données, et la couleur de survol peut rester affichée après le passage de la souris (rafraîchir avec F5).
  Comportement du plugin, sans impact sur les données ; alternative si gênant : graphique en barres par pays.

## D30 — Observabilité : Prometheus, Loki, Grafana (« santé des pipelines » + 3 alertes)
- **Composants** (namespace `monitoring`, images épinglées) : Prometheus 3.1 (PVC, 7 jours), Loki 3.3 (PVC, 7 jours),
  Promtail (DaemonSet : un agent par nœud), Grafana 11.3, `kafka-exporter`, `statsd-exporter`. Pas de chart
  kube-prometheus-stack : une dizaine de composants et d'opérateurs de plus que nécessaire sur un minikube de 24 Go,
  et des configurations moins lisibles. Configurations dans `monitoring/` (validées localement : `promtool`,
  `loki -verify-config`, `promtail -dry-run` sur des journaux réels aux formats docker et CRI).
- **Une source de métrique par composant** : Spark = servlet Prometheus du driver (port 4040, pods annotés
  `prometheus.io/*`, étiquettes `app`, `spark_role`, `dag_id`, `country_code`) ; Kafka = `kafka-exporter` ; NiFi =
  point natif `/nifi-api/flow/metrics/prometheus` ; Airflow = StatsD -> `statsd-exporter` (règles de nommage
  `monitoring/statsd/mapping.yml` : `airflow_ti_finish{dag_id, task_id, state}`) ; Trino = `/metrics` (OpenMetrics,
  en-tête `X-Trino-User`).
- **Retard (lag) des jobs Spark** : Spark ne committe jamais ses offsets dans Kafka (il les garde dans ses
  checkpoints), donc aucun outil Kafka ne voit son retard. `StreamMonitor` (StreamingQueryListener,
  `spark/jobs/common/stream_monitor.py`) committe après chaque micro-lot les offsets traités dans un groupe
  `waba-spark.<requête>` (AdminClient Kafka de la JVM, via py4j : aucune dépendance ajoutée). Ce groupe ne sert qu'à
  la supervision ; la reprise reste fondée sur les checkpoints. Atout décisif pour l'alerte : si le job tombe, les
  offsets se figent et le retard continue de grandir (une métrique exposée par le job disparaîtrait avec lui). Le
  commit est protégé (try/except) : un échec de supervision ne peut pas arrêter le traitement.
- **Latence par pays** : le job 1 écrit pour chaque micro-lot une ligne JSON « latence par pays » (âge du message le
  plus ancien du pays entre son dépôt dans Kafka et la fin de l'écriture silver). Grafana la lit dans Loki
  (`| json | unwrap lag_s`) : pas d'étiquette Prometheus par pays à maintenir dans le code, et l'historique est gardé
  avec les journaux. Promtail extrait `level` et `logger` des lignes JSON des jobs Spark (filtre ERROR / WARNING).
- **Formats de journaux** : minikube `--driver=docker` utilise le runtime docker (fichiers JSON, liens symboliques de
  `/var/log/pods` vers `/var/lib/docker/containers`) ; containerd / CRI-O écrivent au format CRI. Promtail enchaîne
  les étapes `docker` puis `cri` (chacune ignore les lignes qui ne sont pas à son format) et monte les deux dossiers :
  la même configuration fonctionne sur les deux runtimes (vérifié en local).
- **Les 3 alertes** (Grafana unified alerting, `monitoring/grafana/provisioning/alerting.yaml`, évaluées chaque minute) :
  1. *Job fraude en erreur > 5 min* : `sum(up{app="stream-fraud-aml", spark_role="driver"}) or vector(0) < 1` pendant
     5 min. Le driver n'est scrapé que s'il est « Running » : échec, redémarrages en boucle ou suppression -> 0.
  2. *Retard AML > 5000* : `kafka_consumergroup_lag_sum{consumergroup="waba-spark.fraud_aml_aml"} > 5000` pendant 1 min.
  3. *Rapport réglementaire absent à J+1 06h00 UTC* : requête SQL sur la base Airflow — aucun run réussi de
     `dag_regulatory_report` pour la veille ET (06h00 UTC passées OU run déjà en échec). Écarté : les compteurs StatsD
     (un compteur Prometheus ne voit pas sa première incrémentation, ni un DAG qui n'a pas tourné du tout — or
     l'absence de run est justement le cas à détecter). Moindre privilège : Grafana se connecte avec un rôle
     PostgreSQL `grafana_reader` limité à `SELECT` sur `dag_run`, créé par l'init du scheduler (mot de passe dans
     `.env` / `waba-secrets`, jamais en clair).
  Requête SQL testée sur PostgreSQL 16 avec 7 cas (pas de run avant / après 06h, échec, en cours, succès, succès
  d'un autre jour, fuseau de session non UTC).
- **Dashboard « as code »** : `monitoring/grafana/build_dashboard.py` génère `dashboards/pipeline-health.json`
  (18 panneaux, uid `waba-pipeline-health`, page d'accueil de Grafana). Les 4 vues de l'énoncé : latence Spark par
  pays, retard des consommateurs du job fraude (ligne de seuil 5000), débit NiFi (FlowFiles/s par processeur +
  messages/s dans les topics raw-*), taux d'erreur des DAGs ; plus une ligne de synthèse (état de chaque alerte),
  les derniers DAG runs, les journaux Spark ERROR/WARNING et l'état de la collecte. Les alertes sont rattachées à
  leurs panneaux (`__dashboardUid__`/`__panelId__`) : leur état s'affiche sur le dashboard.
- **Notification** : aucun canal configuré (pas de SMTP / Teams en local) ; l'état et l'historique des alertes sont
  visibles dans Grafana (Alerting > Alert rules). Production : point de contact Teams / PagerDuty / e-mail, routage
  par l'étiquette `domaine` (fraude, aml, reglementaire).
- **Démonstration** (alertes réellement déclenchées, pas simulées) : job fraude cassé volontairement
  (`make k8s-demo-break` : fichier principal inexistant) + flux de transactions (`make k8s-stream`) -> alerte 2
  (retard) puis alerte 1 (5 min) ; `make k8s-install` rétablit le job, le retard est rattrapé et les alertes se
  résolvent. Alerte 3 : le DAG réglementaire étant en pause, elle se déclenche après 06h00 UTC ; `make k8s-dag
  DAG=dag_regulatory_report` produit le rapport et la résout.
- **Limite constatée en démo** : le groupe `waba-spark.<requête>` n'existe qu'après le premier micro-lot traité par
  la requête (Spark ne signale une progression que s'il y a des données). Un job cassé avant d'avoir traité un seul
  lot n'a donc pas encore de groupe : retard invisible (0), seule l'alerte 1 se déclenche. En régime normal, le
  groupe existe dès la première donnée et Kafka conserve ses offsets 7 jours sans commit (`offsets.retention.minutes`) ;
  au-delà, l'alerte 1 (job arrêté) est de toute façon active.
- **Réservations CPU** (vécu) : Spark réserve par défaut autant de CPU que de cœurs (`cores`). Avec la supervision
  (+0,4 CPU réservé), le nœud atteignait 90 % de CPU réservé pour 13 % réellement consommé : l'executor du job fraude
  et celui d'un job batch restaient `Pending`. Correctif : `coreRequest` (réservation) distinct de `cores`
  (parallélisme) — drivers 250m, executors streaming 500m, executor batch 1 CPU.

## D31 — Catalogue Iceberg sur PostgreSQL (Kubernetes)
- **Constat (vécu, démo 4.4)** : le batch `dag_ingest_raw` (déclenché par les fichiers de la démo) et le job
  streaming 1 committaient en même temps ; le catalogue REST sur SQLite répondait 500
  (`SQLITE_BUSY_SNAPSHOT : database is locked`). En mode WAL, ce cas (une transaction de lecture qui veut écrire
  alors qu'un autre écrivain est passé entre-temps) n'est pas couvert par `busy_timeout` (D22) : échec immédiat.
  Conséquences visibles dans Grafana : runs `dag_ingest_raw` en échec (taux d'erreur 27 %), requête du job 1
  arrêtée, plus aucune donnée silver -> le retard AML ne pouvait plus monter. La supervision a fait son travail.
- **Correctif** : base du catalogue JDBC sur **PostgreSQL** (`iceberg-catalog-db`, PVC `keep`, mot de passe dans
  `waba-secrets`) : écritures concurrentes gérées par verrous de ligne, c'est la configuration de production
  annoncée en D22. Le serveur REST et les clients (Spark, Trino) sont inchangés.
- **Migration sans perte** : un initContainer (`infra/iceberg/migrate_catalog.py`) crée les tables du catalogue
  JDBC (schéma V1 d'Iceberg 1.6) et y recopie les lignes de l'ancien SQLite. Une table Iceberg n'est dans le
  catalogue qu'un pointeur vers son fichier de métadonnées dans MinIO : rien n'est réécrit ni réenregistré.
  Idempotent ; le fichier SQLite est conservé (retour arrière). Testé sur PostgreSQL 16 (2 passages ; un bug de
  lecture des colonnes trouvé et corrigé grâce au test).
- **Niveaux 1-3 (docker compose)** : SQLite conservé (un seul poste, charge faible).

## D32 — OpenMetadata : catalogue, lignage et PII « as code »
- **Déploiement** (namespace `governance`) : serveur OpenMetadata 1.10.14, PostgreSQL et Elasticsearch 8.11 sur
  PVC (`keep`). Pas de chart officiel : il installe aussi un Airflow d'ingestion (~2 Go de mémoire) alors que la
  plateforme en a déjà un et que la découverte tient dans un Job. Le serveur attend Elasticsearch avant de
  démarrer : sans index, OpenMetadata démarre quand même mais l'API de lignage répond 500 (vu en test local).
- **Découverte automatique** : un Job (`openmetadata-sync`, image `waba/om-sync` = connecteurs officiels Trino et
  Kafka, même version que le serveur) lit les tables Iceberg des schémas raw, bronze, silver, gold via Trino et les
  topics Kafka. Lancé à chaque `make k8s-install` et chaque jour à 05h40 UTC (CronJob : après la chaîne batch de
  la nuit) ; à la demande : `make k8s-om-sync`.
- **Gouvernance as code** (`governance/catalog.yaml`, appliqué par le même Job, idempotent) :
  - 5 équipes propriétaires (Bank, Insurance, Mobile Money, Conformité, Data Platform) ;
  - classifications `Reglementaire` (BCEAO, CIMA) et `ProtectionDonnees` (DonneePersonnelle, Confidentiel) ;
  - glossaire financier (10 termes : NPL, loss ratio, ARPC, AML...) rattaché aux tables gold ;
  - les 11 tables gold documentées (description métier, propriétaire, tags réglementaires, termes du glossaire) ;
  - marquage PII par règles sur le nom des colonnes, sur toutes les couches : `customer_id`, `sender_id`,
    `receiver_id`, `account_number*`, `iban*` -> `PII.Sensitive` + `ProtectionDonnees.DonneePersonnelle` ;
    identifiants de compte -> `PII.NonSensitive` ; montants individuels de sinistres -> `Confidentiel` (servira au
    masquage du rôle viewer, 4f).
- **Lignage** : les transformations sont faites par Spark (pas par Trino), donc aucun journal de requêtes Trino ne
  peut le déduire. Le lignage table -> table (53 arêtes, dont le temps réel à travers les topics Kafka) est déclaré
  à côté du code dans `catalog.yaml` et poussé automatiquement par le Job, chaque arête portant le pipeline qui la
  réalise (entités Pipeline : les 4 DAGs Airflow avec lien vers leur page, les 2 jobs streaming). Évolution
  production : agent OpenLineage dans Spark (lignage au niveau colonne, capté à l'exécution).
- **Sécurité** : le mot de passe par défaut du compte `admin` est remplacé au premier passage par
  `OPENMETADATA_ADMIN_PASSWORD` (.env -> waba-secrets) ; clé Fernet propre ; inscription libre désactivée ;
  Elasticsearch sans Ingress. Reste à faire en production : clés RSA de signature JWT propres (celles de l'image
  sont publiques) et SSO Keycloak (4f).
- **Validation avant livraison** : serveur OpenMetadata 1.10.14 lancé localement sur PostgreSQL 16 (sans
  Elasticsearch) ; tables simulées à partir des schémas réels (`spark/jobs/common/datasets.py`) ; synchronisation
  jouée deux fois (idempotence) : équipes, classifications, glossaire, 11 tables gold documentées, 62 colonnes
  marquées, pipelines vérifiés via l'API. Non testable localement : lignage (Elasticsearch requis) et connexion à
  Trino / Kafka (configuration validée par le SDK). Trouvé en test : dépendance `cachetools` manquante du paquet
  `openmetadata-ingestion` 1.10.14 (ajoutée à l'image), politique de mot de passe d'OpenMetadata (générateur adapté).

## D33 — Keycloak : SSO OIDC et 4 rôles (Superset ; Trino en 4f-2)
- **Déploiement** (namespace `governance`) : Keycloak 26.4.7 (image officielle `quay.io/keycloak/keycloak`) et sa
  base PostgreSQL sur PVC (`keep`). URL publique `http://keycloak.localtest.me:8090` (console : `/admin`, compte
  `admin` / `KEYCLOAK_ADMIN_PASSWORD`). HTTP uniquement parce que la plateforme est locale ; en production :
  HTTPS de bout en bout (`sslRequired: external`, certificats sur l'Ingress).
- **Realm « waba » as code** : `keycloak/gen_realm.py` génère `keycloak/waba-realm.json` (rôles, groupes, clients,
  utilisateurs de démonstration). Un initContainer le ré-importe **en écrasement** à chaque démarrage du pod : le
  dépôt est la source de vérité (une modification faite dans la console est perdue au redémarrage, comme pour les
  dashboards Superset). Aucun secret dans le fichier : placeholders `${SUPERSET_OIDC_CLIENT_SECRET}`... remplacés
  par Keycloak depuis les variables d'environnement (Secret `waba-secrets`). Fichier nommé `*-realm.json` : c'est
  le nom attendu par `kc.sh import --dir` (sinon import « réussi » mais vide, vu en test local).
- **4 rôles de realm** attribués par des **groupes fonctionnels** (`administrateurs` -> `group_admin`,
  `analystes_pays` -> `country_analyst`, `conformite` -> `compliance_officer`, `lecteurs` -> `viewer`) ; le **pays**
  d'un analyste est un groupe `pays_XX` (et non un attribut utilisateur : depuis Keycloak 24, le profil utilisateur
  refuse les attributs non déclarés). Les jetons portent deux claims : `roles` (rôles effectifs) et `groups`
  (groupes, dont `pays_XX`). Utilisateurs de démonstration (mot de passe `KEYCLOAK_DEMO_PASSWORD`) :
  `admin.groupe`, `analyste.ci`, `analyste.sn`, `conformite`, `lecteur`.
- **URL publique / URL interne** : le navigateur parle à `keycloak.localtest.me:8090` (Ingress), les applications
  à `keycloak.governance.svc.cluster.local:8080`. Avec `KC_HOSTNAME` (URL publique) et
  `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`, la découverte OIDC lue en interne annonce l'URL publique pour la page de
  connexion et l'URL interne pour jetons / userinfo / clés ; l'émetteur (`iss`) reste toujours l'URL publique.
  Aucune résolution de `*.localtest.me` n'est donc nécessaire dans les pods.
- **Superset** : `AUTH_OAUTH` (Authlib, PKCE) ; rôles Superset déduits à **chaque connexion** des claims
  (`AUTH_ROLES_SYNC_AT_LOGIN`) : retirer un rôle dans Keycloak suffit. Correspondance et droits
  (`superset/dashboards/apply_security.py`, rejoué à chaque démarrage, idempotent) :

  | Keycloak | Superset | Dashboards | Données |
  |---|---|---|---|
  | group_admin | Admin | les 3 + SQL Lab | tout |
  | country_analyst + pays_XX | WABA_Analyste + WABA_Pays_XX | les 3, exploration | lignes du pays XX (RLS) |
  | compliance_officer | WABA_Conformite | D2 Risque & Conformité | jeux de données gold réglementaires, tous pays |
  | viewer | WABA_Lecteur | D1, D3 | agrégats uniquement : ni exploration, ni « drill to detail », ni export ; pas de D2 (événements AML individuels) |

  Filtres pays : un filtre RLS `country_code = 'XX'` par rôle `WABA_Pays_XX`, plus un filtre `1 = 0` pour le rôle
  analyste, dans le même groupe (combinés par OR) : un analyste sans groupe pays ne voit **aucune** ligne
  (sécurité par défaut). Le filtre natif « Pays » des dashboards ne propose alors que son pays. Aucun rôle WABA n'a
  SQL Lab : les filtres RLS ne s'appliquent pas au SQL libre. Données sensibles (IBAN, numéros de compte, montants
  individuels de sinistres) : aucun jeu de données Superset ne les expose (agrégats gold, identifiants hachés ou
  masqués en silver, cf. D3) ; l'accès direct aux tables passe par Trino (4f-2).
- **Pièges évités** (vérifiés en test local) : derrière l'Ingress, `X-Forwarded-Port` vaut 80 (port interne du
  contrôleur NGINX) alors que le navigateur est sur 8090 : `PROXY_FIX_CONFIG` ignore ce port (`x_port: 0`), sinon
  l'URL de retour OIDC devient `:80` et Keycloak la refuse. Déconnexion : Superset renvoie vers la déconnexion
  Keycloak (sinon la session SSO reconnecte aussitôt le même compte). Tampon d'en-têtes NGINX agrandi (16k) pour
  les cookies et jetons OIDC.
- **Validation avant livraison** : Keycloak 26.4.7 local (PostgreSQL 16) + Superset 4.1.1 (FAB 4.5.0, versions de
  l'image) derrière un simulateur d'Ingress (mêmes en-têtes X-Forwarded-*) : connexion SSO des 5 utilisateurs avec
  un navigateur (Playwright), rôles obtenus, dashboards visibles, pays renvoyés par l'API de données
  (`analyste.ci` -> CI seulement, `analyste.sn` -> SN, `conformite` -> D2 seul, `lecteur` -> D1/D3 sans
  exploration), ré-import du realm et `apply_security.py` rejoués (idempotence).
