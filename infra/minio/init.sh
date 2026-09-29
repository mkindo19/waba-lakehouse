#!/bin/sh
# Initialisation idempotente de MinIO : buckets + compte applicatif dédié (pas de root pour les apps)
set -eu

mc alias set local "http://minio:9000" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in raw-landing lakehouse archive; do
  mc mb --ignore-existing "local/$bucket"
done
# Versioning sur l'archive : aucune suppression définitive accidentelle
mc version enable local/archive

# Compte de service utilisé par Streamlit, Spark, Trino, Iceberg REST
mc admin user add local "$WABA_S3_ACCESS_KEY" "$WABA_S3_SECRET_KEY"
mc admin policy attach local readwrite --user "$WABA_S3_ACCESS_KEY" 2>/dev/null || true

echo "MinIO initialisé : buckets raw-landing, lakehouse, archive"
