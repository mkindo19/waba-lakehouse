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
