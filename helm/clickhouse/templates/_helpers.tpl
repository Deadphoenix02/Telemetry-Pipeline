{{/* Names are kept short: operator pod names add the cluster, shard and replica. */}}
{{- define "ts.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 30 | trimSuffix "-" -}}
{{- end -}}

{{- define "ts.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 30 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 30 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 30 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "ts.keeperName" -}}
{{- printf "%s-keeper" (include "ts.fullname" .) -}}
{{- end -}}

{{- define "ts.secretName" -}}
{{- default (printf "%s-credentials" (include "ts.fullname" .)) .Values.auth.existingSecret -}}
{{- end -}}

{{- define "ts.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "ts.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Call with (dict "root" . "component" "clickhouse"). */}}
{{- define "ts.selector" -}}
app.kubernetes.io/name: {{ include "ts.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Per-replica service names the operator creates: chi-<chi>-<cluster>-<shard>-<replica>. */}}
{{- define "ts.hosts" -}}
{{- $fn := include "ts.fullname" . -}}
{{- $cl := .Values.topology.clusterName -}}
{{- $list := list -}}
{{- range $s := until (int .Values.topology.shards) -}}
{{- range $r := until (int $.Values.topology.replicas) -}}
{{- $list = append $list (printf "chi-%s-%s-%d-%d" $fn $cl $s $r) -}}
{{- end -}}
{{- end -}}
{{- join "," $list -}}
{{- end -}}

{{/* Ports for NetworkPolicies. TLS ports are listed only when TLS is on. */}}
{{- define "ts.nativePorts" -}}
- port: 9000
  protocol: TCP
{{- if .Values.security.tls.enabled }}
- port: 9440
  protocol: TCP
{{- end }}
{{- end -}}

{{- define "ts.clientPorts" -}}
{{ include "ts.nativePorts" . }}
- port: 8123
  protocol: TCP
{{- if .Values.security.tls.enabled }}
- port: 8443
  protocol: TCP
{{- end }}
{{- end -}}

{{- define "ts.interserverPorts" -}}
- port: 9009
  protocol: TCP
{{- if .Values.security.tls.enabled }}
- port: 9010
  protocol: TCP
{{- end }}
{{- end -}}

{{/* Egress to cluster DNS. */}}
{{- define "ts.dnsEgress" -}}
- to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kube-system
      podSelector:
        matchLabels:
          k8s-app: kube-dns
  ports:
    - port: 53
      protocol: UDP
    - port: 53
      protocol: TCP
{{- end -}}

{{/* Pod and container hardening shared by ClickHouse, Keeper and the sidecars (image user 101). */}}
{{- define "ts.podSecurity" -}}
runAsNonRoot: true
runAsUser: 101
runAsGroup: 101
fsGroup: 101
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "ts.containerSecurity" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
{{- end -}}

{{/* Hash of the credentials Secret as it is now in the cluster. On first install there is
     nothing to look up, which is fine: the pods are new anyway. Removing a key and upgrading
     changes the hash, the operator rolls the pods, and they read the regenerated value. */}}
{{- define "ts.credentialsChecksum" -}}
{{- $s := lookup "v1" "Secret" .Release.Namespace (include "ts.secretName" .) -}}
{{- if $s -}}
{{- toJson $s.data | sha256sum | trunc 16 | quote -}}
{{- else -}}
"initial"
{{- end -}}
{{- end -}}

{{/* Prometheus job names the alert rules match: the static jobs of the chart's own Prometheus,
     or the Service names a ServiceMonitor produces. */}}
{{- define "ts.jobClickHouse" -}}
{{- if .Values.observability.prometheus.enabled -}}clickhouse{{- else -}}{{ include "ts.fullname" . }}-metrics{{- end -}}
{{- end -}}
{{- define "ts.jobKeeper" -}}
{{- if .Values.observability.prometheus.enabled -}}keeper{{- else -}}{{ include "ts.fullname" . }}-keeper-metrics{{- end -}}
{{- end -}}

{{/* Keeper pod service names the operator creates: chk-<chk>-keeper-0-<replica>. */}}
{{- define "ts.keeperHosts" -}}
{{- $kn := include "ts.keeperName" . -}}
{{- $list := list -}}
{{- range $r := until (int .Values.keeper.replicas) -}}
{{- $list = append $list (printf "chk-%s-keeper-0-%d" $kn $r) -}}
{{- end -}}
{{- join "," $list -}}
{{- end -}}

{{/* TLS listeners and OpenSSL settings for ClickHouse (config.d/tls.xml), when security.tls.enabled. */}}
{{- define "ts.tlsXml" -}}
{{- $mode := ternary "strict" "relaxed" .Values.security.tls.requireClientCert -}}
<clickhouse>
  <tcp_port_secure>9440</tcp_port_secure>
  <https_port>8443</https_port>
  <interserver_https_port>9010</interserver_https_port>
  <openSSL>
    <server>
      <certificateFile>/etc/clickhouse-server/tls/clickhouse.crt</certificateFile>
      <privateKeyFile>/etc/clickhouse-server/tls/clickhouse.key</privateKeyFile>
      <caConfig>/etc/clickhouse-server/tls/ca.crt</caConfig>
      <loadDefaultCAFile>false</loadDefaultCAFile>
      <verificationMode>{{ $mode }}</verificationMode>
      <cacheSessions>true</cacheSessions>
      <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
      <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
      <certificateFile>/etc/clickhouse-server/tls/clickhouse.crt</certificateFile>
      <privateKeyFile>/etc/clickhouse-server/tls/clickhouse.key</privateKeyFile>
      <caConfig>/etc/clickhouse-server/tls/ca.crt</caConfig>
      <loadDefaultCAFile>false</loadDefaultCAFile>
      <verificationMode>strict</verificationMode>
      <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
      <invalidCertificateHandler><name>RejectCertificateHandler</name></invalidCertificateHandler>
    </client>
  </openSSL>
</clickhouse>
{{- end -}}

{{/* S3 endpoint the backup sidecar talks to. */}}
{{- define "ts.s3Endpoint" -}}
{{- if .Values.backup.s3.endpoint -}}
{{- .Values.backup.s3.endpoint -}}
{{- else if .Values.backup.minio.enabled -}}
{{- printf "http://%s-minio:9000" (include "ts.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "ts.validate" -}}
{{- if not (has .Values.topology.mode (list "single" "replicated")) -}}
{{- fail "topology.mode must be single or replicated" -}}
{{- end -}}
{{- if eq .Values.topology.mode "single" -}}
{{- if or (ne (int .Values.topology.shards) 1) (ne (int .Values.topology.replicas) 1) -}}
{{- fail "topology.mode=single requires shards=1 and replicas=1" -}}
{{- end -}}
{{- else -}}
{{- if lt (int .Values.topology.shards) 1 -}}
{{- fail "topology.shards must be at least 1" -}}
{{- end -}}
{{- if lt (int .Values.topology.replicas) 1 -}}
{{- fail "topology.replicas must be at least 1" -}}
{{- end -}}
{{- if or (lt (int .Values.keeper.replicas) 3) (eq (mod (int .Values.keeper.replicas) 2) 0) -}}
{{- fail "keeper.replicas must be odd and at least 3" -}}
{{- end -}}
{{- end -}}
{{- if not (regexMatch "^[0-9]+ (HOUR|DAY)$" .Values.retention.ttl) -}}
{{- fail "retention.ttl must look like '6 HOUR' or '7 DAY'" -}}
{{- end -}}
{{- if not (has .Values.retention.partition (list "hour" "day")) -}}
{{- fail "retention.partition must be hour or day" -}}
{{- end -}}
{{- if not .Values.security.allowedNetworks -}}
{{- fail "security.allowedNetworks must list the client source networks (for example the pod CIDR)" -}}
{{- end -}}
{{- range $n := .Values.security.allowedNetworks -}}
{{- if not (regexMatch "^[0-9a-fA-F:.]+/[0-9]+$" $n) -}}
{{- fail (printf "security.allowedNetworks entry %q is not a CIDR" $n) -}}
{{- end -}}
{{- end -}}
{{- range $k, $v := .Values.clickhouse.config.threadPools -}}
{{- if not (regexMatch "^(background_[a-z_]+_pool_size|max_[a-z_]*thread_pool[a-z_]*size)$" $k) -}}
{{- fail (printf "clickhouse.config.threadPools: %q is not a thread pool setting" $k) -}}
{{- end -}}
{{- end -}}
{{- $grace := int .Values.clickhouse.terminationGracePeriodSeconds -}}
{{- $needed := add (int .Values.clickhouse.preStopSleepSeconds) (int .Values.clickhouse.shutdownWaitSeconds) -}}
{{- if ge $needed $grace -}}
{{- fail (printf "clickhouse.preStopSleepSeconds + shutdownWaitSeconds (%d) must be less than terminationGracePeriodSeconds (%d)" $needed $grace) -}}
{{- end -}}
{{- if not (regexMatch "^[A-Za-z0-9_]+$" .Values.topology.clusterName) -}}
{{- fail "topology.clusterName may contain only letters, digits and underscores" -}}
{{- end -}}
{{- end -}}

{{/* Server tuning. Written to config.d/telemetry.xml. */}}
{{- define "ts.configXml" -}}
{{- $c := .Values.clickhouse.config -}}
<clickhouse>
  <max_server_memory_usage_to_ram_ratio>{{ $c.maxServerMemoryRatio }}</max_server_memory_usage_to_ram_ratio>
  <mark_cache_size>{{ $c.markCacheSize | int64 }}</mark_cache_size>
  <background_pool_size>{{ $c.backgroundPoolSize | int }}</background_pool_size>
{{- range $k, $v := $c.threadPools }}
  <{{ $k }}>{{ $v | int }}</{{ $k }}>
{{- end }}
  {{- /* Let running queries (inserts in flight) finish on SIGTERM instead of cancelling them. */}}
  <shutdown_wait_unfinished_queries>1</shutdown_wait_unfinished_queries>
  <shutdown_wait_unfinished>{{ .Values.clickhouse.shutdownWaitSeconds | int }}</shutdown_wait_unfinished>
{{- /* ClickHouse refuses to start (Code 36) when these merge-tree thresholds (defaults 20 and 25)
       exceed background_pool_size x background_merges_mutations_concurrency_ratio (default 2). */}}
{{- $slots := mul ($c.backgroundPoolSize | int) 2 }}
{{- $free := int (max 2 (min 20 (div $slots 2))) }}
  <merge_tree>
    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>{{ $free }}</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
    <number_of_free_entries_in_pool_to_execute_mutation>{{ $free }}</number_of_free_entries_in_pool_to_execute_mutation>
    <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>{{ $free }}</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
  </merge_tree>
  <prometheus>
    <endpoint>/metrics</endpoint>
    <port>9363</port>
    <metrics>true</metrics>
    <events>true</events>
    <asynchronous_metrics>true</asynchronous_metrics>
  </prometheus>
{{- if $c.systemLogs.enabled }}
{{- range $t := list "query_log" "part_log" "metric_log" "asynchronous_metric_log" "trace_log" "text_log" }}
  <{{ $t }}>
    <database>system</database>
    <table>{{ $t }}</table>
    {{- /* Drop keys inherited from the image's config.xml: they conflict with <engine>. */}}
    <partition_by remove="1"/>
    <order_by remove="1"/>
    <ttl remove="1"/>
    <engine>ENGINE = MergeTree PARTITION BY event_date ORDER BY (event_date, event_time) TTL event_date + INTERVAL {{ $c.systemLogs.ttlDays | int }} DAY SETTINGS ttl_only_drop_parts = 1</engine>
  </{{ $t }}>
{{- end }}
{{- else }}
{{- range $t := list "query_log" "part_log" "metric_log" "asynchronous_metric_log" "trace_log" "text_log" "query_thread_log" "query_views_log" "session_log" "opentelemetry_span_log" "processors_profile_log" }}
  <{{ $t }} remove="1"/>
{{- end }}
{{- end }}
</clickhouse>
{{- end -}}

{{/*
Users and their limits. Password hashes come from the Secret through environment
variables, so no credential is stored in the installation resource. Grants cover the
public table names; in replicated mode they also cover the per-shard local tables,
because a Distributed query runs on the shards as the calling user.
Keep config/dev-users.xml (the docker test harness) in sync with this.
*/}}
{{- define "ts.usersXml" -}}
{{- $replicated := eq .Values.topology.mode "replicated" -}}
{{- $l := .Values.limits.api -}}
<clickhouse>
  <profiles>
    <api_profile>
      <max_execution_time>{{ $l.maxExecutionTime | int }}</max_execution_time>
      <max_memory_usage>{{ $l.maxMemoryUsage | int64 }}</max_memory_usage>
      <max_result_rows>{{ $l.maxResultRows | int64 }}</max_result_rows>
      <result_overflow_mode>throw</result_overflow_mode>
      <max_rows_to_read>{{ $l.maxRowsToRead | int64 }}</max_rows_to_read>
      <read_overflow_mode>throw</read_overflow_mode>
      <max_threads>2</max_threads>
      <max_concurrent_queries_for_user>{{ $l.maxConcurrentQueries | int }}</max_concurrent_queries_for_user>
{{- if $replicated }}
      <optimize_skip_unused_shards>1</optimize_skip_unused_shards>
{{- end }}
      {{- /* readonly=2: no writes or DDL, settings may change except the constrained caps below. */}}
      <readonly>2</readonly>
      <constraints>
{{- range $k := list "max_execution_time" "max_memory_usage" "max_result_rows" "result_overflow_mode" "max_rows_to_read" "read_overflow_mode" "max_threads" "max_concurrent_queries_for_user" }}
        <{{ $k }}><readonly/></{{ $k }}>
{{- end }}
      </constraints>
    </api_profile>
    <collector_profile>
      <async_insert>0</async_insert>
{{- if $replicated }}
      <insert_distributed_sync>1</insert_distributed_sync>
{{- end }}
    </collector_profile>
  </profiles>
  <quotas>
    <api_quota>
      <interval>
        <duration>60</duration>
        <queries>{{ $l.quota.queriesPerMinute | int }}</queries>
        <errors>{{ $l.quota.errorsPerMinute | int }}</errors>
        <read_rows>{{ $l.quota.readRowsPerMinute | int64 }}</read_rows>
        <result_rows>0</result_rows>
        <execution_time>0</execution_time>
      </interval>
    </api_quota>
  </quotas>
  <users>
    <migrator>
      <password_sha256_hex from_env="MIGRATOR_PASSWORD_SHA256"/>
      {{- include "ts.userNetworks" . | nindent 6 }}
      <profile>default</profile>
      <grants>
        <query>GRANT CREATE, ALTER, DROP, SELECT, INSERT ON default.*</query>
        <query>GRANT CLUSTER ON *.*</query>
{{- if $replicated }}
        <query>GRANT REMOTE ON *.*</query>
        {{- /* migrate.sh waits for every host to be a cluster member before running DDL. */}}
        <query>GRANT SELECT ON system.clusters</query>
{{- end }}
      </grants>
    </migrator>
    <collector>
      <password_sha256_hex from_env="COLLECTOR_PASSWORD_SHA256"/>
      {{- include "ts.userNetworks" . | nindent 6 }}
      <profile>collector_profile</profile>
      <grants>
        <query>GRANT INSERT ON default.telemetry</query>
        <query>GRANT INSERT ON default.gpus</query>
{{- if $replicated }}
        <query>GRANT INSERT ON default.telemetry_local</query>
        <query>GRANT INSERT ON default.gpus_local</query>
{{- end }}
      </grants>
    </collector>
    <backup>
      <password_sha256_hex from_env="BACKUP_PASSWORD_SHA256"/>
      <networks><ip>127.0.0.1</ip><ip>::1</ip></networks>
      <profile>default</profile>
      {{- /* What clickhouse-backup needs: freeze, create, attach and drop on the data tables,
             read the system tables it inspects, and SYSTEM commands such as SYNC REPLICA. */}}
      <grants>
        <query>GRANT SHOW, SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE ON default.*</query>
        <query>GRANT SELECT ON system.*</query>
        <query>GRANT SYSTEM ON *.*</query>
        <query>GRANT CREATE DATABASE ON *.*</query>
      </grants>
    </backup>
    <api>
      <password_sha256_hex from_env="API_PASSWORD_SHA256"/>
      {{- include "ts.userNetworks" . | nindent 6 }}
      <profile>api_profile</profile>
      <quota>api_quota</quota>
      <grants>
        <query>GRANT SELECT ON default.telemetry</query>
        <query>GRANT SELECT ON default.gpus</query>
{{- if $replicated }}
        <query>GRANT SELECT ON default.telemetry_local</query>
        <query>GRANT SELECT ON default.gpus_local</query>
{{- end }}
      </grants>
    </api>
  </users>
</clickhouse>
{{- end -}}

{{/* Source networks the network-facing users accept. Localhost is always allowed (kubectl exec, probes). */}}
{{- define "ts.userNetworks" -}}
<networks>
  <ip>127.0.0.1</ip>
  <ip>::1</ip>
{{- range $n := .Values.security.allowedNetworks }}
  <ip>{{ $n }}</ip>
{{- end }}
</networks>
{{- end -}}
