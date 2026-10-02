# Guide technique — Niveau 1, techno par techno

Pour chaque brique : **où est sa configuration**, **comment elle a été mise en place**,
**comment elle fonctionne concrètement**, **quelles commandes on tape et ce qu'elles font**.

> Toutes les commandes se lancent dans Ubuntu (WSL), depuis `~/waba-lakehouse`.

---

## 0. Le socle commun : Docker Compose et `.env`

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | décrit les 7 services, leurs images, ports, volumes, variables, ordre de démarrage |
| `.env` (non versionné) | secrets + versions d'images ; lu **automatiquement** par Docker Compose |
| `.env.example` | modèle de `.env`, sans secrets réels (versionné) |
| `Makefile` | raccourcis : chaque `make xxx` = une commande `docker compose ...` |

**Mécanique de `.env`** : dans `docker-compose.yml`, `${WABA_S3_ACCESS_KEY}` est remplacé par la
valeur écrite dans `.env`. Le secret n'apparaît donc jamais dans un fichier versionné.

**Le bloc `x-s3-env`** (haut de `docker-compose.yml`) : un « modèle » de variables S3 réutilisé par
tous les services qui parlent à MinIO (`<<: *s3-env`). On écrit l'adresse et les identifiants une
seule fois :

```yaml
x-s3-env: &s3-env
  S3_ENDPOINT: http://minio:9000          # "minio" = nom du service sur le réseau Docker
  AWS_ACCESS_KEY_ID: ${WABA_S3_ACCESS_KEY} # compte applicatif (pas root)
  AWS_SECRET_ACCESS_KEY: ${WABA_S3_SECRET_KEY}
```

**Ce qui se passe derrière `make up`** (= `docker compose up -d --build`) :
1. lit `docker-compose.yml` + `.env` ;
2. **construit** les images maison (`generator/Dockerfile`, `spark/Dockerfile`) si besoin ;
3. crée le **réseau** `waba` et les **volumes** (`minio-data`, `iceberg-catalog`) s'ils n'existent pas ;
4. démarre les containers **dans l'ordre des `depends_on`**, en attendant les healthchecks :
   `minio` (healthy) → `minio-init` (terminé OK) → `iceberg-rest` (healthy) → `spark-master`, `trino`, `generator` → `spark-worker` ;
5. `-d` : rend la main (les containers tournent en arrière-plan).

| Commande | Ce qu'elle fait |
|---|---|
| `make ps` | état des containers (Up / healthy / Exited) |
| `docker compose logs <service> --tail 50` | logs d'un service |
| `make down` | arrête et supprime les containers, **garde les volumes** (données) |
| `make clean` | idem + supprime les volumes → **toutes les données perdues** (demande confirmation) |
| `docker compose up -d --force-recreate <service>` | recrée un seul service |

---

## 1. MinIO — le stockage objet

### Où est la configuration
| Fichier | Contenu |
|---|---|
| `docker-compose.yml` → services `minio` et `minio-init` | image, commande, ports, volume |
| `.env` | `MINIO_IMAGE`, `MINIO_ROOT_USER/PASSWORD` (admin), `WABA_S3_ACCESS_KEY/SECRET_KEY` (applis) |
| `infra/minio/init.sh` | création des buckets et du compte applicatif |

### Comment il a été mis en place
```yaml
minio:
  image: ${MINIO_IMAGE}                         # coollabsio/minio (images officielles retirées)
  command: server /data --console-address ":9001"   # sert le dossier /data ; console web sur 9001
  ports: ["9000:9000", "9001:9001"]             # 9000 = API S3, 9001 = console
  volumes: [minio-data:/data]                   # les objets survivent aux redémarrages
  healthcheck: mc ready local                   # "healthy" quand MinIO répond
```
`minio-init` est un container **éphémère** (image avec l'outil `mc`) qui exécute `infra/minio/init.sh` :
```sh
mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD  # se connecter en admin
mc mb --ignore-existing local/raw-landing        # créer les buckets (idempotent)
mc mb --ignore-existing local/lakehouse
mc mb --ignore-existing local/archive
mc version enable local/archive                  # versioning : rien n'est perdu définitivement
mc admin user add local $WABA_S3_ACCESS_KEY $WABA_S3_SECRET_KEY   # compte des applis
mc admin policy attach local readwrite --user $WABA_S3_ACCESS_KEY # droits lecture/écriture
```

### Comment les fichiers sont stockés
- MinIO parle le **protocole S3** : on y dépose des **objets** (fichier + clé) dans des **buckets**.
  Les « dossiers » n'existent pas vraiment : `bank_transactions/CI/bank_txn_CI_20260401_01.csv`
  est juste une **clé** contenant des `/`.
- Sur disque, MinIO range les objets dans le volume `minio-data` dans **son propre format**
  (`xl.meta` + morceaux) : on ne lit **jamais** ces fichiers directement, on passe par l'API S3.
- Trois clients écrivent/lisent dans MinIO, chacun avec sa bibliothèque S3 :

| Qui | Bibliothèque | Adresse utilisée |
|---|---|---|
| Générateur (Python) | `boto3` → `generator/waba_gen/storage.py` (`put_object`) | `s3://raw-landing/...` |
| Spark (lecture des CSV) | `hadoop-aws` (connecteur `s3a`) | `s3a://raw-landing/...` |
| Iceberg (Spark et Trino) | `S3FileIO` / S3 natif Trino | `s3://lakehouse/...` |

> `s3://` et `s3a://` désignent la même chose (un objet S3) : seul le *connecteur* change.

### L'utiliser au quotidien
- **Console web** : http://localhost:9001 (identifiants : `grep MINIO_ROOT .env`).
- **En ligne de commande** avec `mc` (client MinIO), via un container temporaire :
```bash
alias mc='docker compose run --rm --entrypoint /bin/sh minio-init -c'
mc 'mc alias set l http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc ls l/'
mc 'mc alias set l http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc ls --recursive l/raw-landing | head'
mc 'mc alias set l http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc du l/lakehouse'
```
- `make replay` utilise exactement ce mécanisme : `mc cp --recursive l/archive/ l/raw-landing/`.

---

## 2. Catalogue Iceberg REST — le registre des tables

### Où est la configuration
`docker-compose.yml` → service `iceberg-rest` (aucun fichier séparé : tout passe par variables).

```yaml
iceberg-rest:
  image: ${ICEBERG_REST_IMAGE}                       # tabulario/iceberg-rest:1.6.0
  environment:
    CATALOG_WAREHOUSE: s3://lakehouse/               # où créer les nouvelles tables
    CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO   # comment écrire dans S3
    CATALOG_S3_ENDPOINT: http://minio:9000           # S3 = notre MinIO
    CATALOG_S3_PATH__STYLE__ACCESS: "true"           # URL du type http://minio:9000/bucket/...
    CATALOG_URI: jdbc:sqlite:file:/catalog/iceberg_catalog.db   # le registre lui-même (SQLite)
  volumes: [iceberg-catalog:/catalog]                # le registre survit aux redémarrages
```
Convention de l'image : `CATALOG_XXX_YYY` devient la propriété `xxx.yyy` (`__` = `-`).

### Ce qu'il stocke
**Uniquement** une liste : *nom de table → emplacement de son fichier de métadonnées courant*.
Exemple : `raw.bank_transactions → s3://lakehouse/raw/bank_transactions/metadata/00012-….metadata.json`.
Les **données** ne passent jamais par lui.

### Tester
```bash
curl -s localhost:8181/v1/namespaces                       # -> raw
curl -s localhost:8181/v1/namespaces/raw/tables            # -> les 9 tables
```

---

## 3. Iceberg — le format de table

Iceberg n'est **pas un service** : c'est un **format** (une spécification) + des **bibliothèques**
chargées par Spark (jars) et par Trino (connecteur intégré).

### Où est la configuration
| Côté | Fichier | Clés importantes |
|---|---|---|
| Spark | `spark/conf/spark-defaults.conf` | `spark.sql.catalog.lakehouse.*` (type rest, uri, warehouse, io-impl, endpoint) |
| Spark | `spark/Dockerfile` | téléchargement des jars `iceberg-spark-runtime` et `iceberg-aws-bundle` |
| Trino | `trino/catalog/iceberg.properties` | `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri`, `s3.*` |
| Tables | `spark/jobs/common/datasets.py` + `ingest_raw.py::ensure_tables` | schéma, partitionnement, propriétés |

### Comment une table est créée (dans `ingest_raw.py`, fonction `ensure_tables`)
```sql
CREATE TABLE IF NOT EXISTS lakehouse.raw.bank_transactions ( ...colonnes... )
USING iceberg
PARTITIONED BY (country_code, days(`timestamp`))       -- partitionnement "caché"
TBLPROPERTIES ('format-version'='2')                    -- v2 : nécessaire pour MERGE/UPDATE/DELETE
```

### Où et comment les données sont stockées
Dans MinIO, bucket `lakehouse`, une arborescence par table :
```
lakehouse/raw/bank_transactions/
├── data/                                             ← LES DONNÉES (fichiers Parquet, colonnes compressées)
│   ├── country_code=CI/timestamp_day=2026-04-01/00000-12-ab34….parquet
│   ├── country_code=CI/timestamp_day=2026-04-02/…
│   └── country_code=SN/…
└── metadata/                                         ← LA "TABLE" (ce qui fait d'un tas de fichiers une table)
    ├── 00000-….metadata.json   schéma, partitionnement, liste des snapshots
    ├── 00001-….metadata.json   nouvelle version après chaque écriture
    ├── snap-….avro             "manifest list" : liste des manifests d'un snapshot
    └── ….-m0.avro              "manifest" : liste des fichiers Parquet + statistiques (min/max…)
```
**Lecture d'une table** : catalogue → metadata.json courant → snapshot courant → manifests → fichiers
Parquet utiles seulement (élagage par partition et statistiques).

**Écriture (ex. MERGE)** : écrire de nouveaux Parquet → écrire de nouveaux manifests → nouveau
metadata.json → **le catalogue bascule atomiquement** vers ce nouveau fichier. Si quelque chose
échoue avant la bascule, rien n'est visible : c'est la garantie **ACID**.

### L'explorer (dans Trino)
```sql
SELECT * FROM "bank_transactions$snapshots";      -- historique des écritures (1 ligne par MERGE)
SELECT * FROM "bank_transactions$partitions";     -- partitions + nb de lignes / fichiers
SELECT file_path, record_count FROM "bank_transactions$files" LIMIT 5;   -- fichiers Parquet réels
SELECT count(*) FROM bank_transactions FOR VERSION AS OF <snapshot_id>;  -- time travel
```

---

## 4. Spark — le moteur de traitement

### Où est la configuration
| Fichier | Rôle |
|---|---|
| `spark/Dockerfile` | image : Spark 3.5.3 + jars Iceberg/S3 + boto3 |
| `spark/requirements.txt` | dépendances Python des jobs (boto3) |
| `spark/conf/spark-defaults.conf` | catalogue Iceberg, accès S3, mémoire, fuseau UTC |
| `docker-compose.yml` → `spark-master`, `spark-worker` | rôles, ressources (`SPARK_WORKER_CORES/MEMORY`), montages |
| `spark/jobs/` | **les scripts** (montés dans les containers en `/opt/spark-apps`) |

### Comment il a été mis en place
`spark/Dockerfile` :
```dockerfile
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu   # Spark officiel
RUN curl ... iceberg-spark-runtime-3.5_2.12-1.6.1.jar      # Spark sait lire/écrire Iceberg
    curl ... iceberg-aws-bundle-1.6.1.jar                  # Iceberg sait parler S3
    curl ... hadoop-aws-3.3.4.jar + aws-java-sdk-bundle    # Spark sait lire s3a:// (les CSV)
RUN pip install -r requirements.txt                        # boto3 (lister / archiver)
ENV PYTHONPATH=/opt/spark-apps                             # "from common.xxx import" fonctionne
```
Une seule image, deux rôles (dans `docker-compose.yml`) :
- `spark-master` : `spark-class org.apache.spark.deploy.master.Master` → distribue le travail (port 7077, UI http://localhost:8090) ;
- `spark-worker` : `spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077` → exécute (4 cœurs, 6 Go).

Montages importants :
```yaml
volumes:
  - ./spark/jobs:/opt/spark-apps          # ton code, modifiable sans rebuild
  - ./spark/conf:/opt/waba/spark-conf:ro  # ta config ; SPARK_CONF_DIR pointe ici
```

### Ce qui se passe derrière `make ingest`
```
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 /opt/spark-apps/ingest_raw.py --dataset all
```
1. `docker compose exec` : exécute une commande **dans** le container `spark-master` déjà démarré ;
2. `spark-submit` démarre le **driver** (le chef d'orchestre : ton script Python + une JVM) dans ce container ;
3. le driver contacte le **master** (`spark://spark-master:7077`) et demande des ressources ;
4. le master demande au **worker** de lancer un **executor** (4 Go, 4 cœurs, cf. spark-defaults) ;
5. le driver découpe le travail en tâches, l'executor les exécute (lecture S3, jointures, écriture Parquet) ;
6. fin du script → `spark.stop()` → l'executor est arrêté (`KILLED 143` dans les logs du worker = normal).

Variantes : `make ingest DATASET=bank_transactions COUNTRY=CI`.

### Ajouter un nouveau script
1. Créer `spark/jobs/mon_job.py` (il est immédiatement visible dans le container, grâce au montage) :
```python
from common.session import get_spark
from common.logging_utils import get_logger

log = get_logger("mon_job")

def main() -> None:
    spark = get_spark("mon_job")                    # Iceberg + S3 déjà configurés
    df = spark.table("lakehouse.raw.bank_transactions")
    summary = df.groupBy("country_code").count()
    summary.writeTo("lakehouse.raw.demo_summary").using("iceberg").createOrReplace()
    log.info("ok", extra={"ctx": {"rows": summary.count()}})
    spark.stop()

if __name__ == "__main__":
    main()
```
2. Le lancer :
```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 /opt/spark-apps/mon_job.py
```
3. (Optionnel) ajouter une cible dans le `Makefile`.
4. Si le job a besoin d'une nouvelle librairie Python → l'ajouter à `spark/requirements.txt` puis
   `docker compose build spark-master spark-worker && make up` (c'est le **seul** cas où il faut reconstruire).

---

## 5. Décorticage de `spark/jobs/ingest_raw.py`

Les modules utilisés (dans `spark/jobs/common/`) :

| Module | Rôle |
|---|---|
| `datasets.py` | **catalogue des 8 jeux** : schéma typé, clé, partitionnement, contrôles, clés étrangères, colonnes PII |
| `quality.py` | règles de validation → colonne `_reject_reason` |
| `pii.py` | hachage salé + masquage |
| `landing.py` | lister les CSV (boto3) et archiver |
| `session.py` | créer la SparkSession |
| `logging_utils.py` | logs JSON |

### Le chemin d'exécution
```
main()                                  # lit les arguments, boucle sur les jeux dans INGESTION_ORDER
 └─ ingest_dataset(ds)                  # pour UN jeu de données
     ├─ list_landing_files(prefix)      # ① quels CSV attendent ?        (landing.py, boto3)
     ├─ read_landing(paths)             # ② lecture typée + lignes cassées capturées
     ├─ with_reject_reason(df)          # ③④ validation + clés étrangères (quality.py)
     ├─ write_rejects(rejected)         # ⑤ rejets -> raw.rejected_records
     ├─ merge_valid(valid)              # ⑥ dédoublonnage + PII + MERGE
     └─ archive_files(keys)             # ⑦ raw-landing -> archive
```

### ① `main()` — l. 189-209
- `INGESTION_ORDER` (dans `datasets.py`) : customers, products, branches, **accounts** (qui référence
  customers et products), **puis** les transactions. L'ordre garantit que les contrôles de clés trouvent
  leurs référentiels.
- Chaque jeu est dans un `try/except` : un échec est journalisé et n'empêche pas les autres… **sauf**
  un échec de référentiel (`break`) : inutile d'ingérer des transactions sans référentiel.
- Le code de retour (`1` si échec) permettra à Airflow de savoir que la tâche a échoué (niveau 2).

### ② `read_landing()` — l. 85-99
```python
schema = ds.schema + _corrupt_record        # schéma EXPLICITE (datasets.py) + colonne "poubelle"
spark.read.option("mode", "PERMISSIVE")      # une ligne illisible ne fait pas planter le job...
     .option("columnNameOfCorruptRecord", CORRUPT_COL)   # ...elle est copiée brute dans _corrupt_record
     .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") # format ISO 8601 attendu
     .schema(schema).csv(paths)
     .withColumn("_source_file", F.input_file_name())    # traçabilité : de quel fichier vient la ligne
return df.cache()                            # Spark exige un cache pour filtrer sur _corrupt_record
```
Pourquoi un schéma explicite ? Sans lui, Spark *devine* les types (lent, et faux si un fichier est sale).
Avec lui, `"abc"` dans `amount` (DOUBLE) devient une ligne corrompue détectée.

### ③④ `with_reject_reason()` — `common/quality.py`
Construit **une seule colonne** `_reject_reason` avec un `CASE WHEN` en cascade (premier motif gagnant) :
`MALFORMED_RECORD` → `MISSING_KEY` → `INVALID_COUNTRY` → `INVALID_ENTITY_TYPE` → `INVALID_TIMESTAMP`
→ `CURRENCY_MISMATCH` (XOF/GHS selon le pays) → `INVALID_AMOUNT` → valeurs d'énumération
→ `ORPHAN_<COLONNE>`.

Contrôle de clé étrangère = **jointure gauche** avec le référentiel :
```python
ref_keys = spark.table("lakehouse.raw.accounts").select("account_id").distinct()
df.join(ref_keys, df.account_id == ref_keys.account_id, "left")   # pas trouvé -> NULL -> ORPHAN_ACCOUNT_ID
```
Les petits référentiels (branches, products) sont **broadcastés** (copiés sur chaque executor : pas de shuffle).

### ⑤ `write_rejects()` — l. 103-128
- Garde la ligne brute (`raw_record`) + son motif + son fichier source.
- `record_hash` = empreinte SHA-256 (fichier + ligne) → MERGE sur ce hash : ré-ingérer ne duplique
  pas non plus les rejets.

### ⑥ `merge_valid()` — l. 131-155 (le cœur)
```python
w = Window.partitionBy(key).orderBy(_source_file.desc())
valid.withColumn("_rn", row_number().over(w)).filter("_rn = 1")   # 1 seule ligne par clé dans le lot
     .withColumn("_ingested_at", lit(now))                        # horodatage FIGÉ (pas current_timestamp)
staged = materialize(protect(staged, pii_columns))                # PII protégées, puis données figées
ensure_tables(...)                                                # CREATE TABLE IF NOT EXISTS
MERGE INTO raw.X t USING staged s
ON t.key = s.key AND t.country_code = s.country_code              # country_code : MERGE limité aux bonnes partitions
WHEN NOT MATCHED THEN INSERT *                                    # transactions : insertion seule
-- référentiels : + WHEN MATCHED THEN UPDATE SET *                (upsert, SCD type 1)
```
- `protect()` (`pii.py`) : `iban` → `iban_hash` (SHA-256 de *sel secret + IBAN*) + `iban_masked`
  (`CI93****…1234`) ; la colonne en clair est supprimée **avant** écriture.
- `materialize()` (l. 47-56) = `df.checkpoint(eager=True)` : écrit le lot dans
  `s3a://lakehouse/_spark_checkpoints/` et **coupe le lignage**. Raison : Iceberg refuse un MERGE dont
  la source contient des expressions non déterministes (`input_file_name()`, `row_number()`) — cf. piège n°3.

### ⑦ `archive_files()` — `common/landing.py`
`copy_object` vers `archive` → `head_object` pour comparer les tailles → `delete_object` de la source
**seulement si** la copie est identique. En cas d'échec, le fichier reste en place (et sera re-traité
sans doublon grâce au MERGE).

---

## 6. Trino — le moteur SQL

### Où est la configuration
| Fichier | Rôle |
|---|---|
| `docker-compose.yml` → service `trino` | image, port (`TRINO_PORT`, 8088), montage du dossier catalog |
| `trino/catalog/iceberg.properties` | **le seul fichier qu'on a écrit** : le catalogue `iceberg` |
| (dans l'image) `/etc/trino/config.properties`, `jvm.config` | config serveur par défaut, inchangée |

Chaque fichier `trino/catalog/<nom>.properties` crée un **catalogue** nommé `<nom>` dans Trino.
Notre `iceberg.properties` :
```properties
connector.name=iceberg                       # quel connecteur utiliser
iceberg.catalog.type=rest                    # le registre est un catalogue REST...
iceberg.rest-catalog.uri=http://iceberg-rest:8181   # ...le même que Spark
fs.native-s3.enabled=true                    # lecteur S3 intégré de Trino
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}   # injecté depuis l'environnement (pas de secret en dur)
```
Donc : **catalogue `iceberg`** → **schéma `raw`** → **table `bank_transactions`**, soit
`iceberg.raw.bank_transactions`.

### Et le fichier `sql/level1_checks.sql` ?
**Aucun lien de configuration** avec Trino : c'est juste une **liste de requêtes** qu'on a écrite pour
vérifier les critères. On l'envoie au client Trino par l'entrée standard (`<`) :
```bash
docker compose exec -T trino trino --catalog iceberg --schema raw < sql/level1_checks.sql
#                  │                 │                                └─ le fichier est "tapé" dans le client
#                  │                 └─ catalogue et schéma par défaut
#                  └─ -T : pas de terminal interactif (nécessaire quand on redirige un fichier)
```

### Comment Trino exécute une requête
1. le client envoie le SQL au **coordinator** Trino ;
2. Trino demande au catalogue REST où est la table → lit les métadonnées Iceberg dans MinIO ;
3. il **élague** : ne garde que les fichiers Parquet des partitions/valeurs utiles ;
4. il lit ces fichiers **directement dans MinIO**, calcule en mémoire, renvoie le résultat.
Trino **ne stocke rien** (d'où sa recréation sans perte).

### L'utiliser au quotidien
| Usage | Comment |
|---|---|
| Console interactive | `make trino` puis `SHOW TABLES;`, `SELECT ...;`, `quit` |
| Une requête ponctuelle | `docker compose exec -T trino trino --catalog iceberg --execute "SELECT ..."` |
| Un fichier de requêtes | `docker compose exec -T trino trino --catalog iceberg --schema raw < fichier.sql` |
| Export CSV | ajouter `--output-format CSV_HEADER > resultat.csv` |
| Interface web (suivi des requêtes) | http://localhost:8088 (utilisateur : n'importe quel nom) |
| DBeaver / outil BI | JDBC `jdbc:trino://localhost:8088/iceberg/raw`, utilisateur quelconque, sans mot de passe |
| Superset (niveau 4) | `trino://user@trino:8080/iceberg` (adresse interne au réseau Docker) |

---

## 7. Générateur Streamlit (rappel)

| Fichier | Rôle |
|---|---|
| `generator/Dockerfile`, `requirements.txt` | image Python 3.11 + numpy/pandas/boto3/streamlit |
| `generator/app.py` | l'interface web (3 onglets) |
| `generator/waba_gen/config.py` | **tous les paramètres métier** (pays, devises, probabilités, NPL/loss ratio cibles) |
| `generator/waba_gen/referentials.py`, `transactions.py` | la génération |
| `generator/waba_gen/storage.py` | l'écriture dans MinIO (boto3) et la numérotation `_NN` |
| `generator/tests/` | 14 tests (`make test`) |

`make referentials` / `make generate` = `docker compose exec generator python -m waba_gen ...` : la
même logique que l'interface, en ligne de commande (utile pour Airflow au niveau 2).
