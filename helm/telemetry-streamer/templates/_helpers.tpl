{{- define "telemetry-streamer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "telemetry-streamer.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/* Role-suffixed names for everything except the Deployment and Service. */}}
{{- define "telemetry-streamer.role" -}}
{{- printf "%s-%s" (include "telemetry-streamer.fullname" .ctx | trunc 52 | trimSuffix "-") .role }}
{{- end }}

{{- define "telemetry-streamer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "telemetry-streamer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "telemetry-streamer.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "telemetry-streamer.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* The broker addresses, comma-separated: each broker pod through the headless
     Service when brokers.replicas > 0, else streamer.target (empty = discard). */}}
{{- define "telemetry-streamer.targets" -}}
{{- $b := .Values.brokers -}}
{{- if gt (int $b.replicas) 0 -}}
{{- $list := list -}}
{{- range $i := until (int $b.replicas) -}}
{{- $list = append $list (printf "%s-%d.%s:%d" $b.statefulSet $i $b.headlessService (int $b.port)) -}}
{{- end -}}
{{- join "," $list -}}
{{- else -}}
{{- .Values.streamer.target -}}
{{- end -}}
{{- end }}
