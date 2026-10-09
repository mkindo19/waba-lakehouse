{{/* Étiquettes communes à toutes les ressources */}}
{{- define "waba.labels" -}}
app.kubernetes.io/part-of: waba-platform
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* Sélecteur + étiquettes d'un composant : include "waba.component" (dict "ctx" $ "name" "minio") */}}
{{- define "waba.selector" -}}
app.kubernetes.io/name: {{ .name }}
{{- end -}}

{{- define "waba.component" -}}
{{ include "waba.labels" .ctx }}
{{ include "waba.selector" . }}
{{- end -}}

{{/* Namespace d'un domaine : include "waba.ns" (dict "ctx" $ "domain" "ingestion") */}}
{{- define "waba.ns" -}}
{{- index .ctx.Values.namespaces .domain -}}
{{- end -}}

{{/* Nom DNS complet d'un service : include "waba.svc" (dict "ctx" $ "domain" "processing" "name" "minio") */}}
{{- define "waba.svc" -}}
{{ .name }}.{{ include "waba.ns" . }}.svc.cluster.local
{{- end -}}

{{/* Endpoint S3 (MinIO) vu depuis n'importe quel namespace */}}
{{- define "waba.s3Endpoint" -}}
http://{{ include "waba.svc" (dict "ctx" . "domain" "processing" "name" "minio") }}:9000
{{- end -}}

{{/* Variables S3 communes (identifiants lus dans le Secret waba-secrets du namespace) */}}
{{- define "waba.s3Env" -}}
- name: S3_ENDPOINT
  value: {{ include "waba.s3Endpoint" . | quote }}
- name: AWS_REGION
  value: {{ .Values.s3.region | quote }}
- name: RAW_BUCKET
  value: raw-landing
- name: ARCHIVE_BUCKET
  value: archive
- name: AWS_ACCESS_KEY_ID
  valueFrom: {secretKeyRef: {name: waba-secrets, key: WABA_S3_ACCESS_KEY}}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom: {secretKeyRef: {name: waba-secrets, key: WABA_S3_SECRET_KEY}}
{{- end -}}

{{/*
Configuration Spark commune aux SparkApplication (streaming et batch).
En mode Kubernetes, le driver est lancé avec --properties-file : le spark-defaults.conf de l'image n'est PAS lu.
On reprend donc ici le catalogue Iceberg et l'accès S3 (hôtes du namespace processing).
*/}}
{{- define "waba.sparkConf" -}}
spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lakehouse: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type: rest
spark.sql.catalog.lakehouse.uri: "http://iceberg-rest:8181"
spark.sql.catalog.lakehouse.warehouse: "s3://lakehouse/"
spark.sql.catalog.lakehouse.io-impl: org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakehouse.s3.endpoint: "http://minio:9000"
spark.sql.catalog.lakehouse.s3.path-style-access: "true"
spark.sql.defaultCatalog: lakehouse
spark.hadoop.fs.s3a.endpoint: "http://minio:9000"
spark.hadoop.fs.s3a.path.style.access: "true"
spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
spark.hadoop.fs.s3a.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
spark.sql.session.timeZone: UTC
spark.memory.fraction: "0.7"
# Métriques Prometheus natives de Spark (servlet sur le port de l'UI, 4040) — supervision 4.4
spark.ui.prometheus.enabled: "true"
spark.metrics.conf.*.sink.prometheusServlet.class: org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheusServlet.path: /metrics/prometheus
spark.metrics.appStatusSource.enabled: "true"
{{- end -}}

{{/* Variables d'environnement des pods Spark (driver et executors) */}}
{{- define "waba.sparkEnv" -}}
- name: AWS_ACCESS_KEY_ID
  valueFrom: {secretKeyRef: {name: waba-secrets, key: WABA_S3_ACCESS_KEY}}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom: {secretKeyRef: {name: waba-secrets, key: WABA_S3_SECRET_KEY}}
- name: PII_HASH_SALT
  valueFrom: {secretKeyRef: {name: waba-secrets, key: PII_HASH_SALT}}
- {name: AWS_REGION, value: {{ .Values.s3.region | quote }}}
- {name: S3_ENDPOINT, value: "http://minio:9000"}
- {name: RAW_BUCKET, value: raw-landing}
- {name: ARCHIVE_BUCKET, value: archive}
- name: KAFKA_BOOTSTRAP
  value: "{{ include "waba.svc" (dict "ctx" . "domain" "ingestion" "name" "kafka") }}:9092"
- {name: WABA_GHS_PER_EUR, value: {{ .Values.spark.ghsPerEur | quote }}}
- {name: STREAM_EVENT_WATERMARK, value: {{ .Values.spark.watermark | quote }}}
- {name: LIQUIDITY_MIN_COVERAGE, value: {{ .Values.spark.liquidityMinCoverage | quote }}}
{{- end -}}
