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

{{/*
Modèle de SparkApplication des jobs batch (lancés par Airflow, cf. airflow/dags/waba_sparkapp.py).
Airflow complète : metadata, mainApplicationFile, arguments et étiquettes (DAG, tâche, pays).
*/}}
{{- define "waba.sparkBatchApp" -}}
{{- $b := .Values.spark.batch -}}
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "{{ .Values.spark.image }}:{{ .Values.localImages.tag }}"
  imagePullPolicy: {{ .Values.localImages.pullPolicy }}
  sparkVersion: {{ .Values.spark.version | quote }}
  restartPolicy: {type: Never}            # les nouvelles tentatives sont gérées par Airflow (retries)
  timeToLiveSeconds: {{ $b.ttlSeconds }}
  sparkConf:
    {{- include "waba.sparkConf" . | nindent 4 }}
    spark.sql.shuffle.partitions: {{ $b.shufflePartitions | quote }}
    spark.sql.adaptive.enabled: "true"
    spark.cleaner.referenceTracking.cleanCheckpoints: "true"
  driver:
    cores: {{ $b.driver.cores }}
    coreRequest: {{ $b.driver.coreRequest | quote }}
    memory: {{ $b.driver.memory | quote }}
    serviceAccount: spark
    labels:
      {{- include "waba.component" (dict "ctx" . "name" "spark-batch") | nindent 6 }}
    annotations:
      prometheus.io/scrape: "true"
      prometheus.io/port: "4040"
      prometheus.io/path: /metrics/prometheus/
    env: {{- include "waba.sparkEnv" . | nindent 6 }}
  executor:
    instances: {{ $b.executor.instances }}
    cores: {{ $b.executor.cores }}
    coreRequest: {{ $b.executor.coreRequest | quote }}
    memory: {{ $b.executor.memory | quote }}
    memoryOverhead: {{ $b.executor.memoryOverhead | quote }}   # limite du pod = memory + memoryOverhead
    labels:
      {{- include "waba.component" (dict "ctx" . "name" "spark-batch-exec") | nindent 6 }}
    env: {{- include "waba.sparkEnv" . | nindent 6 }}
{{- end -}}

{{/* Configuration Airflow commune au scheduler et au webserver (variables d'environnement, aucun secret en clair) */}}
{{- define "waba.airflowEnv" -}}
- name: AIRFLOW_DB_PASSWORD
  valueFrom: {secretKeyRef: {name: waba-secrets, key: AIRFLOW_DB_PASSWORD}}
# $(VAR) : substitution faite par Kubernetes à partir de la variable précédente (mot de passe hexadécimal)
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
  value: "postgresql+psycopg2://airflow:$(AIRFLOW_DB_PASSWORD)@airflow-db:5432/airflow"
- name: AIRFLOW__CORE__FERNET_KEY
  valueFrom: {secretKeyRef: {name: waba-secrets, key: AIRFLOW_FERNET_KEY}}
- name: AIRFLOW__WEBSERVER__SECRET_KEY        # partagée : signe aussi l'accès aux journaux du scheduler
  valueFrom: {secretKeyRef: {name: waba-secrets, key: AIRFLOW_WEBSERVER_SECRET_KEY}}
- {name: AIRFLOW__CORE__EXECUTOR, value: LocalExecutor}
- {name: AIRFLOW__CORE__LOAD_EXAMPLES, value: "false"}
- {name: AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION, value: "true"}
- {name: AIRFLOW__CORE__DEFAULT_TIMEZONE, value: UTC}
- {name: AIRFLOW__CORE__PARALLELISM, value: "8"}
# Le webserver lit les journaux des tâches sur le scheduler (port 8793) : on publie l'IP du pod,
# car le nom d'hôte d'un pod n'est pas résolu par le DNS du cluster.
- {name: AIRFLOW__CORE__HOSTNAME_CALLABLE, value: airflow.utils.net.get_host_ip_address}
- {name: AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL, value: "30"}
- {name: AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK, value: "true"}
- {name: AIRFLOW__WEBSERVER__WORKERS, value: {{ .Values.airflow.webserverWorkers | quote }}}
- {name: AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX, value: "true"}   # derrière l'Ingress NGINX
- {name: AIRFLOW__WEBSERVER__BASE_URL, value: "http://airflow.{{ .Values.ingress.domain }}:{{ .Values.ingress.port }}"}
- {name: AIRFLOW_VAR_WABA_GHS_PER_EUR, value: {{ .Values.spark.ghsPerEur | quote }}}
# Spark : SparkApplication via Spark Operator (cf. airflow/dags/waba_sparkapp.py)
- {name: WABA_SPARK_MODE, value: k8s}
- {name: WABA_SPARK_NAMESPACE, value: {{ include "waba.ns" (dict "ctx" . "domain" "processing") }}}
- {name: WABA_SPARK_APP_TEMPLATE, value: /opt/airflow/spark-k8s/sparkapplication.yaml}
{{- if .Values.monitoring.enabled }}
# Métriques StatsD (UDP) -> statsd-exporter -> Prometheus (supervision 4.4 : taux d'erreur des DAGs)
- {name: AIRFLOW__METRICS__STATSD_ON, value: "True"}
- {name: AIRFLOW__METRICS__STATSD_HOST, value: {{ include "waba.svc" (dict "ctx" . "domain" "monitoring" "name" "statsd-exporter") | quote }}}
- {name: AIRFLOW__METRICS__STATSD_PORT, value: "9125"}
- {name: AIRFLOW__METRICS__STATSD_PREFIX, value: airflow}
{{- end }}
{{ include "waba.s3Env" . }}
{{- end -}}

{{/* Superset : URI SQLAlchemy de Trino, variables d'environnement et volumes communs (init + serveur) */}}
{{- define "waba.supersetTrinoUri" -}}
trino://{{ .Values.superset.trinoUser }}@{{ include "waba.svc" (dict "ctx" . "domain" "serving" "name" "trino") }}:8080/iceberg
{{- end -}}

{{- define "waba.supersetEnv" -}}
- name: SUPERSET_SECRET_KEY
  valueFrom: {secretKeyRef: {name: waba-secrets, key: SUPERSET_SECRET_KEY}}
- name: SUPERSET_DB_PASSWORD
  valueFrom: {secretKeyRef: {name: waba-secrets, key: SUPERSET_DB_PASSWORD}}
- name: SUPERSET_ADMIN_USER
  valueFrom: {secretKeyRef: {name: waba-secrets, key: SUPERSET_ADMIN_USER}}
- name: SUPERSET_ADMIN_PASSWORD
  valueFrom: {secretKeyRef: {name: waba-secrets, key: SUPERSET_ADMIN_PASSWORD}}
- {name: WABA_TRINO_URI, value: {{ include "waba.supersetTrinoUri" . | quote }}}
- {name: SCARF_ANALYTICS, value: "false"}      # pas de télémétrie sortante
- {name: SUPERSET_PORT, value: "8088"}          # port d'écoute de gunicorn (cf. enableServiceLinks)
{{- end -}}

{{- define "waba.supersetMounts" -}}
- {name: files, mountPath: /app/waba, readOnly: true}
- {name: files, mountPath: /app/pythonpath/superset_config.py, subPath: superset_config.py, readOnly: true}
{{- end -}}
