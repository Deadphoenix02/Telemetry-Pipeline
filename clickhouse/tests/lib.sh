#!/usr/bin/env bash
# Shared helpers for the cluster tests. Source this file; it does not run anything.
# Talks to a deployed replicated release through kubectl. Admin queries run as the
# built-in default user inside each pod (it only accepts connections from localhost).

NS="${NS:-telemetry}"
FN="${FULLNAME:-telemetry-store}"          # the chart's fullname (release name when it contains the chart name)
CLUSTER="${CLUSTER:-telemetry}"
SHARDS="${SHARDS:-2}"
REPLICAS="${REPLICAS:-2}"
SECRET="${SECRET:-${FN}-credentials}"

pass=0; fail=0
ok()    { echo "PASS  $1"; pass=$((pass+1)); }
bad()   { echo "FAIL  $1 ($2)"; fail=$((fail+1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1" "expected [$2] got [$3]"; fi; }
note()  { echo "----  $*"; }

pod()  { echo "chi-${FN}-${CLUSTER}-$1-$2-0"; }     # pod shard replica
sts()  { echo "chi-${FN}-${CLUSTER}-$1-$2"; }

secret_val() { kubectl -n "$NS" get secret "$SECRET" -o "jsonpath={.data.$1}" | base64 -d; }

# admin <shard> <replica> <query>   (default user, inside the pod)
admin() {
  kubectl -n "$NS" exec "$(pod "$1" "$2")" -c clickhouse -- clickhouse-client --query "$3"
}
# as <user> <password> <shard> <replica> <query>
as() {
  kubectl -n "$NS" exec "$(pod "$3" "$4")" -c clickhouse -- \
    clickhouse-client --user "$1" --password "$2" --query "$5"
}
COLLECTOR_PW=""; API_PW=""
load_creds() { COLLECTOR_PW="$(secret_val collector-password)"; API_PW="$(secret_val api-password)"; }
qc() { as collector "$COLLECTOR_PW" 0 0 "$1"; }   # collector through shard 0 replica 0
qa() { as api "$API_PW" 0 0 "$1"; }

wait_pod_ready() { kubectl -n "$NS" wait --for=condition=Ready "pod/$1" --timeout="${2:-300s}" >/dev/null; }
wait_all_ready() {
  for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 0 $((REPLICAS-1))); do
    wait_pod_ready "$(pod "$s" "$r")" "${1:-300s}" || return 1
  done; done
}
sync_all() {
  for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 0 $((REPLICAS-1))); do
    admin "$s" "$r" "SYSTEM SYNC REPLICA telemetry_local" >/dev/null 2>&1
    admin "$s" "$r" "SYSTEM SYNC REPLICA gpus_local" >/dev/null 2>&1
  done; done
}
# Waits until no replica reports is_readonly (Keeper reachable again).
wait_writable() {
  local deadline=$(( $(date +%s) + ${1:-300} ))
  while (( $(date +%s) < deadline )); do
    local ro=0
    for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 0 $((REPLICAS-1))); do
      v="$(admin "$s" "$r" "SELECT countIf(is_readonly) FROM system.replicas WHERE table='telemetry_local'" 2>/dev/null || echo 1)"
      ro=$((ro + v))
    done; done
    [[ "$ro" == "0" ]] && return 0
    sleep 3
  done
  return 1
}
# Shard index (0-based) a GPU uuid lands on: equal weights, so cityHash64 modulo shard count.
shard_of() { admin 0 0 "SELECT cityHash64(toUUID('$1')) % $SHARDS"; }
# gpu_for_shard <n>: a stable uuid that hashes to shard n.
gpu_for_shard() {
  local i u
  for i in $(seq 1 200); do
    u="$(printf '00000000-0000-0000-0000-%012d' "$i")"
    [[ "$(shard_of "$u")" == "$1" ]] && { echo "$u"; return; }
  done
}
# insert_rows <gpu> <metric> <n> [streamer_id] [base_expr]: n rows with distinct keys through the
# Distributed table as collector. base_expr is a SQL expression (default: this minute), so rows
# are recent enough to survive the TTL; pass a fixed literal to replay identical rows.
insert_rows() {
  local base="${5:-toStartOfMinute(now())}"
  qc "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value)
      SELECT '$1', toDateTime64($base, 9) + toIntervalMillisecond(number), '$2', ${4:-1}, number FROM numbers($3)
      SETTINGS insert_distributed_sync=1"
}
# rejects <regex> <cmd...>: prints yes when the command fails AND its output matches the regex.
# The output is captured first: piping into grep under pipefail reports "no" for a command that
# exits non-zero, which is exactly what a correctly denied statement does.
rejects() {
  local re="$1" out rc; shift
  out="$("$@" 2>&1)"; rc=$?
  if [[ $rc -ne 0 ]] && grep -qiE "$re" <<<"$out"; then echo yes; else echo no; fi
}
# Alerts from the chart's dev Prometheus (observability.prometheus.enabled). prom_pod is empty
# when it is not deployed, and the alert checks are then skipped.
prom_pod() { kubectl -n "$NS" get pods -l app.kubernetes.io/component=prometheus -o name 2>/dev/null | head -1; }
alerts_json() { kubectl -n "$NS" exec "$(prom_pod)" -- wget -qO- http://localhost:9090/api/v1/alerts 2>/dev/null; }
# alert_active <alertname>: yes when the alert is pending or firing.
alert_active() {
  alerts_json | python3 -c "import sys,json; a=json.load(sys.stdin)['data']['alerts']; print('yes' if any(x['labels']['alertname']=='$1' for x in a) else 'no')" 2>/dev/null || echo no
}
# wait_alert <alertname> <yes|no> [timeout seconds]: waits until alert_active returns the wanted value.
wait_alert() {
  local deadline=$(( $(date +%s) + ${3:-180} ))
  while (( $(date +%s) < deadline )); do
    [[ "$(alert_active "$1")" == "$2" ]] && { echo "$2"; return 0; }
    sleep 5
  done
  alert_active "$1"
}
# check_alert <description> <alertname> <yes|no> [timeout]: skipped without Prometheus.
check_alert() {
  if [[ -z "$(prom_pod)" ]]; then note "SKIP  $1 (no Prometheus in this release)"; return; fi
  check "$1" "$3" "$(wait_alert "$2" "$3" "${4:-180}")"
}

# A long-running client pod that reaches ClickHouse the way a collector does: through the cluster
# Service, from a pod labelled as a client (so the network policy admits it). It reuses the
# ClickHouse image (already on the node) for clickhouse-client; the collector's password reaches
# it through the environment, not the command line.
CLIENT_POD="${FN}-loadgen"
SERVICE_HOST="${SERVICE_HOST:-clickhouse-${FN}}"
# start_client_pod [name] [memory limit]: defaults to $CLIENT_POD and 256Mi.
start_client_pod() {
  local name="${1:-$CLIENT_POD}" mem="${2:-256Mi}"
  local image; image="$(kubectl -n "$NS" get pod "$(pod 0 0)" -o jsonpath='{.spec.containers[?(@.name=="clickhouse")].image}')"
  kubectl -n "$NS" delete pod "$name" --ignore-not-found --wait=true >/dev/null 2>&1
  kubectl -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels: {telemetry-store/client: "true", app.kubernetes.io/component: loadgen}
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext: {runAsNonRoot: true, runAsUser: 101, runAsGroup: 101, seccompProfile: {type: RuntimeDefault}}
  containers:
    - name: client
      image: $image
      command: ["sleep", "infinity"]
      env:
        - {name: CLICKHOUSE_USER, value: collector}
        - name: CLICKHOUSE_PASSWORD
          valueFrom: {secretKeyRef: {name: $SECRET, key: collector-password}}
      resources: {requests: {cpu: 50m, memory: 64Mi}, limits: {memory: $mem}}
      securityContext: {allowPrivilegeEscalation: false, capabilities: {drop: ["ALL"]}}
YAML
  kubectl -n "$NS" wait --for=condition=Ready "pod/$name" --timeout=120s >/dev/null
}
stop_client_pod() { kubectl -n "$NS" delete pod "${1:-$CLIENT_POD}" --ignore-not-found --wait=false >/dev/null 2>&1; }
# service_insert <gpu> <metric> <n> <streamer_id>: one insert through the Service, a new
# connection each time (so the Service picks a pod per insert, as a fresh collector connection would).
service_insert() {
  kubectl -n "$NS" exec "$CLIENT_POD" -- clickhouse-client --host "$SERVICE_HOST" --port 9000 --query \
    "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value)
     SELECT '$1', toDateTime64(toStartOfMinute(now()), 9) + toIntervalMillisecond(number), '$2', $4, number
     FROM numbers($3) SETTINGS insert_distributed_sync=1" </dev/null
}

summary() { echo; echo "passed=$pass failed=$fail"; [[ $fail -eq 0 ]]; }
