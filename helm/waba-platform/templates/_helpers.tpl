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
