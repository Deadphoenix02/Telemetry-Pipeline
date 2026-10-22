{{/* True when a ScaledObject should be rendered: KEDA enabled and its CRD installed. */}}
{{- define "collector.keda" -}}
{{- if and .Values.keda.enabled (.Capabilities.APIVersions.Has "keda.sh/v1alpha1/ScaledObject") -}}true{{- end -}}
{{- end }}

{{/* The broker addresses the collector consumes from, comma-separated. */}}
{{- define "collector.brokers" -}}
{{- $b := .Values.brokers -}}
{{- if gt (int $b.replicas) 0 -}}
{{- $list := list -}}
{{- range $i := until (int $b.replicas) -}}
{{- $list = append $list (printf "%s-%d.%s:%d" $b.statefulSet $i $b.headlessService (int $b.grpcPort)) -}}
{{- end -}}
{{- join "," $list -}}
{{- else -}}
{{- .Values.collector.broker -}}
{{- end -}}
{{- end }}

{{/* The /lag URLs KEDA (in another namespace) calls: one per broker, fully qualified. */}}
{{- define "collector.lagURLs" -}}
{{- $b := .Values.brokers -}}
{{- if gt (int $b.replicas) 0 -}}
{{- $list := list -}}
{{- range $i := until (int $b.replicas) -}}
{{- $list = append $list (printf "http://%s-%d.%s.%s.svc.cluster.local:%d/lag" $b.statefulSet $i $b.headlessService $.Release.Namespace (int $b.httpPort)) -}}
{{- end -}}
{{- join "," $list -}}
{{- else -}}
{{- include "collector.lagURL" . -}}
{{- end -}}
{{- end }}

{{/* The single broker's /lag URL as KEDA (in another namespace) must call it. */}}
{{- define "collector.lagURL" -}}
{{- .Values.keda.lagURL | default (printf "http://broker.%s.svc.cluster.local:8080/lag" .Release.Namespace) -}}
{{- end }}
