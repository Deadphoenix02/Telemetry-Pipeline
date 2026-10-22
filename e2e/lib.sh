#!/usr/bin/env bash
# Shared helpers for the cluster end-to-end suite. Source this file; it does not run anything.
# Builds on clickhouse/tests/lib.sh (ok/bad/check/note, admin queries inside the ClickHouse pods,
# replica sync) and adds: a disposable streamer release, port-forwards to the broker and the
# cluster Prometheus, exact acked counts from the streamers' shutdown lines, and the loss check.

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$E2E_DIR/.." && pwd)"
# shellcheck source=../clickhouse/tests/lib.sh
source "$ROOT/clickhouse/tests/lib.sh"            # NS, FN, SHARDS, REPLICAS, ok/bad/check/note, admin, sync_all

REL="${REL:-e2e-streamer}"                          # the test's own streamer release (pods e2e-streamer-N)
MAX_STREAMERS=10                                    # the streamer chart's schema allows 0..10
PROM_PORT="${PROM_PORT:-29090}"
BROKER_PORT="${BROKER_PORT:-28080}"                 # broker-i's HTTP is forwarded to BROKER_PORT+i
STREAMER_PORT="${STREAMER_PORT:-29100}"             # e2e-streamer-i's metrics to STREAMER_PORT+i
MINIKUBE_MEM_BYTES="${MINIKUBE_MEM_BYTES:-$((8 * 1024 * 1024 * 1024))}"
MINIKUBE_CPUS="${MINIKUBE_CPUS:-$(python3 -c "import json,os; print(json.load(open(os.path.expanduser('~/.minikube/profiles/minikube/config.json')))['CPUs'])" 2>/dev/null || echo 4)}"
STATE_DIR="$E2E_DIR/results"
DEV_STREAMERS_FILE="$STATE_DIR/paused-dev-streamers"  # replicas of the dev streamer release, paused for the run
RUN="${RUN:-$STATE_DIR/$(date +%Y%m%d-%H%M%S)}"
REPORT="$RUN/report.md"
mkdir -p "$RUN"

declare -A PF_PID=()          # port-forwards by name
declare -a FOLLOW_PIDS=()     # kubectl logs -f followers of the current streamer set
CASE_LOG_PREFIX=""            # where the followers write
DOWN_STS=()                   # ClickHouse StatefulSets scaled down by a case (restored on exit)
COLLECTOR_CHANGED=0
BROKER_CHANGED=0
GATEWAY_CHANGED=0

say()    { echo "$(date +%H:%M:%S)  $*"; }
report() { echo "$*" >> "$REPORT"; }
now_ns() { date +%s%N; }

# ---- port-forwards ------------------------------------------------------------------------------
pf_start() { # name target local:remote
  pf_stop "$1"
  kubectl -n "${4:-$NS}" port-forward "$2" "$3" >"$RUN/pf-$1.log" 2>&1 &
  PF_PID[$1]=$!
  local port="${3%%:*}"
  for _ in $(seq 1 50); do curl -s -m 1 "localhost:$port" >/dev/null 2>&1 && return 0; sleep 0.2; done
}
pf_stop() { [[ -n "${PF_PID[$1]:-}" ]] && kill "${PF_PID[$1]}" 2>/dev/null; unset "PF_PID[$1]"; }
# Every broker is an independent queue: forward each one's HTTP port.
brokers() { kubectl -n "$NS" get sts broker -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 1; }
pf_broker() { local i; for ((i = 0; i < $(brokers); i++)); do pf_start "broker$i" "pod/broker-$i" "$((BROKER_PORT + i)):8080"; done; }
pf_prom()   { pf_start prom svc/prometheus-server "$PROM_PORT:80" monitoring; }

# ---- metrics ------------------------------------------------------------------------------------
# prom <promql>: the first sample's value (empty when there is none).
prom() {
  curl -s -m 10 --data-urlencode "query=$1" "localhost:$PROM_PORT/api/v1/query" | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["data"]["result"]
    print(r[0]["value"][1] if r else "")
except Exception:
    print("")'
}
# prom_by <label> <promql>: "label=value" pairs, one per line.
prom_by() {
  curl -s -m 10 --data-urlencode "query=$2" "localhost:$PROM_PORT/api/v1/query" | python3 -c '
import json, sys
for r in json.load(sys.stdin)["data"]["result"]:
    print(r["metric"].get(sys.argv[1], "?") + "=" + r["value"][1])' "$1" 2>/dev/null
}
# broker_metric_on <i> <name>: one broker's counter or gauge, read directly (no scrape delay).
broker_metric_on() { curl -s -m 3 "localhost:$((BROKER_PORT + $1))/metrics" | awk -v m="$2" '$1==m {print $2}'; }
# broker_metric <name> [sum|max]: over every broker; sum for counters, max for 0/1 gauges ("any").
# Empty when no broker answered.
broker_metric() {
  local i v out="" mode="${2:-sum}"
  for ((i = 0; i < $(brokers); i++)); do
    v="$(broker_metric_on "$i" "$1")"; [[ -z "$v" ]] && continue
    out="$(python3 -c 'import sys; a,b,m=sys.argv[1:]; x=float(b) if a=="" else (float(a)+float(b) if m=="sum" else max(float(a),float(b))); print("%g" % x)' "$out" "$v" "$mode")"
  done
  echo "$out"
}
# lag: records not yet committed, over every broker; empty unless every broker answered.
lag() {
  local i l total=0
  for ((i = 0; i < $(brokers); i++)); do
    l="$(curl -s -m 3 "localhost:$((BROKER_PORT + i))/lag" | python3 -c 'import json,sys; print(json.load(sys.stdin)["lag"])' 2>/dev/null)"
    [[ -z "$l" ]] && return
    total=$((total + l))
  done
  echo "$total"
}
# streamer_metric <i> <series>: e2e-streamer-i's metric, read through a port-forward (pf_streamers).
streamer_metric() { curl -s -m 3 "localhost:$((STREAMER_PORT + $1))/metrics" | awk -v m="$2" '$1==m {print $2}'; }
pf_streamers() { # <count>
  local i
  for ((i = 0; i < $1; i++)); do pf_start "streamer$i" "pod/$REL-$i" "$((STREAMER_PORT + i)):9100"; done
}
fmt() { python3 -c 'import sys; v=sys.argv[1]; print("-" if v=="" else ("%.3g" % float(v) if abs(float(v))<1000 else "{:,.0f}".format(float(v))))' "${1:-}"; }

wait_lag_zero() { # [timeout seconds]
  local deadline=$(( $(date +%s) + ${1:-300} )) l
  while (( $(date +%s) < deadline )); do
    l="$(lag)"; [[ "$l" == "0" ]] && return 0
    [[ -z "$l" ]] && pf_broker
    sleep 2
  done
  return 1
}
collector_replicas() { kubectl -n "$NS" get deploy collector -o jsonpath='{.status.replicas}'; }
# wait_prom <promql> <want> [timeout]: polls until the query returns want; prints the last value.
wait_prom() {
  local deadline=$(( $(date +%s) + ${3:-120} )) v=""
  while (( $(date +%s) < deadline )); do
    v="$(prom "$1")"; [[ "$v" == "$2" ]] && break
    sleep 5
  done
  echo "${v:-none}"
}

# ---- memory and restarts ------------------------------------------------------------------------
minikube_mem_pct() { docker stats --no-stream --format '{{.MemPerc}}' minikube 2>/dev/null | tr -d '%'; }
minikube_mem_bytes() {
  docker stats --no-stream --format '{{.MemUsage}}' minikube 2>/dev/null | awk '{
    v=$1; u=v; gsub(/[0-9.]/,"",u); gsub(/[A-Za-z]/,"",v)
    m = (u=="GiB") ? 1073741824 : (u=="MiB") ? 1048576 : (u=="KiB") ? 1024 : 1
    printf "%d\n", v*m }'
}
# restart_snapshot: namespace/pod=restarts for the pipeline namespaces, one per line.
restart_snapshot() {
  kubectl get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}={.status.containerStatuses[*].restartCount}{"\n"}{end}' 2>/dev/null |
    grep -E '^(telemetry|monitoring|keda)/' | sort
}
# oom_since <snapshot file>: pods that restarted since the snapshot, with the reason.
oom_since() {
  local before="$1" ns_pod now prev reason
  while IFS='=' read -r ns_pod now; do
    prev="$(grep -F "$ns_pod=" "$before" | head -1 | cut -d= -f2)"
    if [[ -n "$prev" && "$now" != "$prev" ]]; then
      reason="$(kubectl -n "${ns_pod%%/*}" get pod "${ns_pod#*/}" -o jsonpath='{.status.containerStatuses[*].lastState.terminated.reason}' 2>/dev/null)"
      echo "${ns_pod#*/}:${reason:-restarted}"
    fi
  done < <(restart_snapshot)
}

# ---- the disposable streamer release ------------------------------------------------------------
STREAMER_ARGS=()   # extra helm --set flags for the next streamers_up (resource limits for load)
# valid_streamers <n>: the suite never asks for a count the chart would refuse.
valid_streamers() { [[ "$1" =~ ^[0-9]+$ ]] && (( $1 <= MAX_STREAMERS )); }
streamers_up() { # <count> <rows/s each> <case>
  local n="$1" rate="$2" case="$3"
  if ! valid_streamers "$n"; then
    bad "streamer count $n" "must be 0..$MAX_STREAMERS"; return 1
  fi
  helm upgrade --install "$REL" "$ROOT/helm/telemetry-streamer" -n "$NS" \
    -f "$ROOT/helm/telemetry-streamer/values-dev.yaml" --set image.tag=dev \
    --set fullnameOverride="$REL" --set streamer.replicas="$n" --set streamer.rateRowsPerSec="$rate" \
    --set streamer.statsInterval=10s "${STREAMER_ARGS[@]}" --wait --timeout 3m >"$RUN/helm-$REL.log" 2>&1 ||
    { bad "streamers up ($n x $rate rows/s)" "helm failed, see $RUN/helm-$REL.log"; return 1; }
  kubectl -n "$NS" rollout status "sts/$REL" --timeout=180s >/dev/null 2>&1
  CASE_LOG_PREFIX="$RUN/$case"
  FOLLOW_PIDS=()
  local p
  for p in $(kubectl -n "$NS" get pods -l "app.kubernetes.io/instance=$REL" -o jsonpath='{.items[*].metadata.name}'); do
    kubectl -n "$NS" logs -f "$p" >"$CASE_LOG_PREFIX-$p.log" 2>&1 &
    FOLLOW_PIDS+=($!)
  done
  say "streamers: $n x $rate rows/s ($((n * rate)) rows/s)"
}
# streamers_down: scale to 0; every pod logs its final acked count on SIGTERM before it goes.
streamers_down() {
  helm upgrade "$REL" "$ROOT/helm/telemetry-streamer" -n "$NS" --reuse-values \
    --set streamer.replicas=0 --wait --timeout 3m >>"$RUN/helm-$REL.log" 2>&1
  local deadline=$(( $(date +%s) + 90 )) pid
  for pid in "${FOLLOW_PIDS[@]}"; do
    while kill -0 "$pid" 2>/dev/null && (( $(date +%s) < deadline )); do sleep 1; done
    kill "$pid" 2>/dev/null
  done
  FOLLOW_PIDS=()
  say "streamers: stopped"
}
# acked_counts <case>: prints "acked unacked stopped_pods" from the case's shutdown lines.
acked_counts() {
  cat "$RUN/$1"-*.log 2>/dev/null | grep "streamer stopped" | awk '{
    for (i=1;i<=NF;i++) { split($i,kv,"="); if (kv[1]=="records_acked") a+=kv[2]; if (kv[1]=="unacked_records") u+=kv[2] }
    n++ } END { printf "%d %d %d\n", a, u, n }'
}

# ---- ClickHouse ---------------------------------------------------------------------------------
# distinct_since <ns>: rows with ts at or after <ns>, counted once per deduplication key.
# FINAL collapses rows that share the ReplacingMergeTree key while streaming the sorted parts,
# so it stays within ClickHouse's memory cap where uniqExact over millions of keys does not.
# The key includes ts and the table is partitioned by time, so no key spans two partitions.
distinct_since() {
  sync_all
  admin 0 0 "SELECT count() FROM telemetry FINAL WHERE ts >= fromUnixTimestamp64Nano(toInt64($1))
             SETTINGS do_not_merge_across_partitions_select_final = 1"
}
newest_age_ms() { # <since ns>: now minus the newest ts stored since <ns>, in ms
  admin 0 0 "SELECT toUnixTimestamp64Milli(now64(3)) - toUnixTimestamp64Milli(max(ts)) FROM telemetry WHERE ts >= fromUnixTimestamp64Nano(toInt64($1))"
}
# loss_check <case> <since ns> <expected stopped pods>
loss_check() {
  local c="$1" since="$2" pods="$3" acked unacked stopped distinct
  read -r acked unacked stopped <<<"$(acked_counts "$c")"
  check "$c: every streamer logged its final acked count" "$pods" "$stopped"
  wait_lag_zero 600 && ok "$c: queue drained (lag 0)" || bad "$c: queue drained" "lag $(lag)"
  distinct="$(distinct_since "$since")"
  say "$c: streamers acked $acked (unacked $unacked); ClickHouse holds $distinct distinct rows"
  report "- $c: acked $acked, unacked $unacked, distinct rows in ClickHouse $distinct"
  if [[ "$unacked" == "0" ]]; then
    check "$c: every acked record is in ClickHouse once (no loss)" "$acked" "$distinct"
  elif (( distinct >= acked && distinct <= acked + unacked )); then
    ok "$c: no loss (distinct within acked..acked+unacked)"
  else
    bad "$c: loss check" "acked=$acked unacked=$unacked distinct=$distinct"
  fi
}

# ---- ClickHouse outage (scale both replicas of shard 1 to 0) -------------------------------------
db_down() { local s; for s in "$(sts 1 0)" "$(sts 1 1)"; do kubectl -n "$NS" scale sts "$s" --replicas=0 >/dev/null; DOWN_STS+=("$s"); done; }
db_up() {
  local s
  for s in "${DOWN_STS[@]:-}"; do [[ -n "$s" ]] && kubectl -n "$NS" scale sts "$s" --replicas=1 >/dev/null 2>&1; done
  DOWN_STS=()
  wait_all_ready 300s
}

# ---- component overrides (always restored on exit) ----------------------------------------------
collector_set() { # extra --set flags
  COLLECTOR_CHANGED=1
  helm upgrade collector "$ROOT/helm/collector" -n "$NS" -f "$ROOT/helm/collector/values-dev.yaml" \
    --set image.tag=dev "$@" --wait --timeout 3m >"$RUN/helm-collector.log" 2>&1
}
broker_set() {
  BROKER_CHANGED=1
  helm upgrade broker "$ROOT/helm/broker" -n "$NS" -f "$ROOT/helm/broker/values-dev.yaml" \
    --set image.tag=dev "$@" --wait --timeout 3m >"$RUN/helm-broker.log" 2>&1
  kubectl -n "$NS" rollout status sts/broker --timeout=180s >/dev/null 2>&1
  pf_broker
}

# ---- the API gateway ------------------------------------------------------------------------------------
# api_base: the API through the dev NodePort.
api_base() {
  local port; port="$(kubectl -n "$NS" get svc api-gateway-nodeport -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null)"
  [[ -n "$port" ]] && echo "http://$(minikube ip):$port/api/v1"
}
gateway_set() { # extra --set flags; restored on exit
  GATEWAY_CHANGED=1
  helm upgrade api-gateway "$ROOT/helm/api-gateway" -n "$NS" -f "$ROOT/helm/api-gateway/values-dev.yaml" \
    --set image.tag=dev "$@" --wait --timeout 3m >"$RUN/helm-api-gateway.log" 2>&1 &&
    kubectl -n "$NS" rollout status deploy/api-gateway --timeout=120s >/dev/null 2>&1
}
# api_checks <case> <check> [args...]: runs e2e/api_checks.py, which prints one
# "PASS|FAIL<TAB>name<TAB>detail" line per check, plus "REPORT<TAB>text" lines for report.md.
api_checks() {
  local c="$1" kind name detail; shift
  while IFS=$'\t' read -r kind name detail; do
    case "$kind" in
      PASS) ok "$c: $name" ;;
      FAIL) bad "$c: $name" "$detail" ;;
      REPORT) say "$c: $name"; report "- $c: $name" ;;
      EXIT) [[ "$name" == "0" ]] || bad "$c: api_checks.py $*" "exit $name, see $RUN/$c-api-checks.err" ;;
    esac
  done < <(python3 "$E2E_DIR/api_checks.py" "$@" --api "$(api_base)" --ns "$NS" \
             --ch-pods "$(for s in $(seq 0 $((SHARDS - 1))); do printf '%s,' "$(pod "$s" 0)"; done)" \
             --swagger "$ROOT/api-gateway/openapi/swagger.json" 2>"$RUN/$c-api-checks.err"; printf 'EXIT\t%s\n' "$?")
}

# ---- the dev streamer release ----------------------------------------------------------------------
# The loss check counts every row stored since a case began, so the dev release's own streamers
# (make deploy) are paused for the run; the count is kept in a file so make e2e-clean can restore it.
pause_dev_streamers() {
  local n; n="$(kubectl -n "$NS" get sts telemetry-streamer -o jsonpath='{.spec.replicas}' 2>/dev/null)"
  [[ -z "$n" || "$n" == "0" ]] && return 0
  echo "$n" >"$DEV_STREAMERS_FILE"
  kubectl -n "$NS" scale sts telemetry-streamer --replicas=0 >/dev/null
  kubectl -n "$NS" wait --for=delete pod -l app.kubernetes.io/instance=telemetry-streamer --timeout=120s >/dev/null 2>&1
  say "paused the dev streamer release ($n replicas) for the run"
}
resume_dev_streamers() {
  [[ -f "$DEV_STREAMERS_FILE" ]] || return 0
  kubectl -n "$NS" scale sts telemetry-streamer --replicas="$(cat "$DEV_STREAMERS_FILE")" >/dev/null && rm -f "$DEV_STREAMERS_FILE"
  say "resumed the dev streamer release"
}

# ---- network partition -----------------------------------------------------------------------------
# partition_on <pod ip>: drop every packet between the pod and other pods at the minikube node, in
# the raw table's PREROUTING, so established streams stall instead of being reset. Traffic to and
# from the node's own addresses passes, so the kubelet's probes still reach the pod: it stays up and
# Ready, as it would behind a network split. Calico never accepts packets in that chain, so its
# rules can't bypass the drop.
partition_on() {
  minikube ssh -- "sudo iptables -t raw -N e2e-partition &&
    for a in \$(ip -4 -o addr show | awk '{print \$4}' | cut -d/ -f1); do
      sudo iptables -t raw -A e2e-partition -s \$a -j RETURN; sudo iptables -t raw -A e2e-partition -d \$a -j RETURN; done &&
    sudo iptables -t raw -A e2e-partition -j DROP &&
    sudo iptables -t raw -I PREROUTING 1 -s $1 -j e2e-partition &&
    sudo iptables -t raw -I PREROUTING 1 -d $1 -j e2e-partition" >/dev/null
}
# partition_off: remove what partition_on added (also after an interrupted run).
partition_off() {
  minikube ssh -- 'sudo iptables -t raw -S PREROUTING | grep -- "-j e2e-partition" | sed "s/^-A /-D /" |
    while read -r rule; do eval sudo iptables -t raw $rule; done;
    sudo iptables -t raw -F e2e-partition 2>/dev/null; sudo iptables -t raw -X e2e-partition 2>/dev/null; true' >/dev/null 2>&1
}

# ---- cleanup ------------------------------------------------------------------------------------
# cleanup <since ns>: remove everything the run created and restore what it changed
# (make e2e-clean runs this alone, after an interrupted run).
cleanup() {
  local since="$1" pid
  say "cleanup: streamers, overrides, ClickHouse outage, test rows"
  for pid in "${FOLLOW_PIDS[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null; done
  partition_off
  helm uninstall "$REL" -n "$NS" --wait >/dev/null 2>&1
  [[ ${#DOWN_STS[@]} -gt 0 ]] && db_up
  if [[ "$COLLECTOR_CHANGED" == "1" ]]; then make -s -C "$ROOT" deploy SERVICE=collector >"$RUN/restore-collector.log" 2>&1; COLLECTOR_CHANGED=0; fi
  if [[ "$BROKER_CHANGED" == "1" ]]; then make -s -C "$ROOT" deploy SERVICE=broker >"$RUN/restore-broker.log" 2>&1; BROKER_CHANGED=0; fi
  if [[ "$GATEWAY_CHANGED" == "1" ]]; then make -s -C "$ROOT" deploy SERVICE=api-gateway >"$RUN/restore-api-gateway.log" 2>&1; GATEWAY_CHANGED=0; fi
  if [[ -n "$since" ]]; then
    pf_broker; wait_lag_zero 300 >/dev/null
    admin 0 0 "ALTER TABLE telemetry_local ON CLUSTER '$CLUSTER' DELETE WHERE ts >= fromUnixTimestamp64Nano(toInt64($since)) SETTINGS mutations_sync = 2" >/dev/null 2>&1
    admin 0 0 "ALTER TABLE gpus_local ON CLUSTER '$CLUSTER' DELETE WHERE last_seen >= toDateTime(intDiv($since, 1000000000)) SETTINGS mutations_sync = 2" >/dev/null 2>&1
    local left; left="$(admin 0 0 "SELECT count() FROM telemetry WHERE ts >= fromUnixTimestamp64Nano(toInt64($since))" 2>/dev/null)"
    check "cleanup: no test rows left in ClickHouse" "0" "${left:-?}"
  fi
  check "cleanup: streamer release removed" "" "$(helm list -n "$NS" -q -f "^$REL\$" 2>/dev/null)"
  resume_dev_streamers
  for pid in "${PF_PID[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null; done
}

# failovers <i>: e2e-streamer-i's broker switches caused by a lost broker (error or ack timeout).
# failovers_for <streamer index> <reason>: one streamer_failovers_total series.
failovers_for() {
  curl -s -m 3 "localhost:$((STREAMER_PORT + $1))/metrics" |
    awk -v r="streamer_failovers_total{reason=\"$2\"}" '$1 == r { n += $2 } END { print n + 0 }'
}
failovers() {
  curl -s -m 3 "localhost:$((STREAMER_PORT + $1))/metrics" |
    awk '$1 ~ /^streamer_failovers_total\{reason="(error|ack_timeout)"\}$/ { n += $2 } END { print n + 0 }'
}
