#!/usr/bin/env bash
# End-to-end suite against the minikube deployment: streamers -> 3 brokers -> collectors -> ClickHouse
# -> API gateway.
# It deploys its own streamer release, drives load, breaks things on purpose, checks that no
# acknowledged record is lost, and records metrics and latency in e2e/results/<run>/report.md.
# Everything it creates is removed on exit (also on Ctrl-C), and everything it changes is restored.
#
#   make e2e                                        all cases
#   make e2e CASES="happy scale"                    a subset
#   LOAD_STEPS="2:5000 4:10000" STEP_SECONDS=60 make e2e CASES=load
#   LOAD_READERS=4 make e2e CASES=load              the same, with 4 API readers during each step
#
# Cases: limits happy load scale broker-crash broker-partition collector-crash db-down backpressure
#        api api-chunks api-load api-rollout api-db-down
set -uo pipefail

source "$(cd "$(dirname "$0")" && pwd)/lib.sh"

ALL_CASES="limits happy load scale broker-crash broker-partition collector-crash db-down backpressure api api-chunks api-load api-rollout api-db-down"
CASES="${CASES:-$ALL_CASES}"
STEP_SECONDS="${STEP_SECONDS:-90}"
LOAD_STEPS="${LOAD_STEPS:-2:5000 4:10000 8:12500 10:20000 10:50000}"   # streamers:rows/s each
LOAD_READERS="${LOAD_READERS:-0}"   # API readers running through each load step (0 = ingest only)

T0=""
finish() {
  local rc=$?
  trap - EXIT INT TERM
  cleanup "$T0"
  echo; echo "cluster e2e: $pass passed, $fail failed  (report: $REPORT)"
  report ""; report "**Result: $pass passed, $fail failed**"
  [[ "$fail" == "0" && "$rc" == "0" ]] || exit 1
}
trap finish EXIT
trap 'echo; say "interrupted"; exit 130' INT TERM

# ---- preflight ----------------------------------------------------------------------------------
preflight() {
  local ctx; ctx="$(kubectl config current-context 2>/dev/null)"
  [[ "$ctx" == "minikube" ]] || { echo "kube context is '$ctx', expected minikube" >&2; exit 2; }
  local r
  for r in "broker:$NS" "collector:$NS" "api-gateway:$NS" "telemetry-store:$NS" "keda:keda" "prometheus:monitoring"; do
    helm status "${r%%:*}" -n "${r##*:}" >/dev/null 2>&1 ||
      { echo "release ${r%%:*} is not installed in ${r##*:}: make deploy" >&2; exit 2; }
  done
  minikube image ls 2>/dev/null | grep -q "telemetry-streamer:dev" ||
    { echo "telemetry-streamer:dev is not in minikube: make docker SERVICE=telemetry-streamer" >&2; exit 2; }
  if helm status "$REL" -n "$NS" >/dev/null 2>&1; then
    echo "a previous run left release $REL behind: make e2e-clean" >&2; exit 2
  fi
  wait_all_ready 120s || { echo "ClickHouse is not ready" >&2; exit 2; }
  kubectl -n "$NS" rollout status sts/broker --timeout=60s >/dev/null || { echo "brokers not ready" >&2; exit 2; }
  curl -s -m 5 -o /dev/null "$(api_base)/gpus" || { echo "the API is not reachable at $(api_base)" >&2; exit 2; }
}

preflight
pause_dev_streamers
T0="$(now_ns)"
echo "$T0" > "$STATE_DIR/last-T0"
pf_prom; pf_broker
{ echo "# Cluster e2e run $(basename "$RUN")"; echo; echo "Cases: $CASES"; echo; } > "$REPORT"
say "run $RUN; cases: $CASES"

# ---- cases --------------------------------------------------------------------------------------
case_limits() {
  note "limits: the streamer chart allows 0..10 replicas"
  local n rc
  for n in -1 0 10 11; do
    helm template x "$ROOT/helm/telemetry-streamer" --set streamer.replicas=$n >/dev/null 2>&1; rc=$?
    if [[ $n -ge 0 && $n -le 10 ]]; then check "limits: replicas=$n accepted" "0" "$rc"
    else check "limits: replicas=$n rejected" "1" "$rc"; fi
  done
  check "limits: the suite refuses 11 streamers" "no" "$(valid_streamers 11 && echo yes || echo no)"
  check "limits: the suite accepts 10 streamers" "yes" "$(valid_streamers 10 && echo yes || echo no)"
}

case_happy() {
  local nb; nb="$(brokers)"
  note "happy: $nb streamers x 1000 rows/s for 75 s, one per broker"
  local t i acc0=() acc; t="$(now_ns)"
  for ((i = 0; i < nb; i++)); do acc0[i]="$(broker_metric_on "$i" broker_records_accepted_total)"; done
  streamers_up "$nb" 1000 happy || return
  pf_streamers "$nb"
  sleep 20
  for ((i = 0; i < nb; i++)); do
    check "happy: e2e-streamer-$i streams to its home broker-$((i % nb))" "$((i % nb))" "$(streamer_metric "$i" streamer_connected_broker)"
  done
  local i age max_age=0
  for i in $(seq 1 6); do
    age="$(newest_age_ms "$t")"; [[ -n "$age" ]] && (( age > max_age )) && max_age=$age
    sleep 5
  done
  local job
  for job in broker collector telemetry-streamer; do
    check "happy: Prometheus scrapes $job" "1" "$(wait_prom "min(up{job=\"kubernetes-pods\",app_kubernetes_io_name=\"$job\"})" 1 90)"
  done
  check "happy: Prometheus scrapes ClickHouse" "1" "$(wait_prom 'min(up{service="telemetry-store-metrics"})' 1 30)"
  sleep 15
  local p95; p95="$(prom 'histogram_quantile(0.95, sum by (le) (increase(broker_ack_latency_seconds_bucket[90s])))')"
  local ins; ins="$(prom 'histogram_quantile(0.95, sum by (le) (increase(collector_insert_duration_seconds_bucket[90s])))')"
  for ((i = 0; i < nb; i++)); do
    acc="$(broker_metric_on "$i" broker_records_accepted_total)"
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[2] or 0) > float(sys.argv[1] or 0) else 1)' "${acc0[i]}" "$acc" &&
      ok "happy: broker-$i accepted records (load is spread)" || bad "happy: broker-$i accepted" "${acc0[i]:-?} -> ${acc:-?}"
  done
  streamers_down
  say "happy: freshness max ${max_age} ms, ack p95 $(fmt "$p95") s, insert p95 $(fmt "$ins") s"
  report "- happy: freshness (now - newest row) max ${max_age} ms; producer ack p95 $(fmt "$p95") s; insert p95 $(fmt "$ins") s"
  (( max_age < 5000 )) && ok "happy: rows queryable within 5 s (max ${max_age} ms)" || bad "happy: freshness" "${max_age} ms"
  python3 -c "import sys; sys.exit(0 if float(sys.argv[1] or 9) < 0.1 else 1)" "$p95" &&
    ok "happy: producer ack p95 under 100 ms ($(fmt "$p95") s)" || bad "happy: ack p95" "$p95 s"
  loss_check happy "$t" "$nb"
}

# One load step: streamers x rate for STEP_SECONDS; prints a table row and the verdict.
VERDICT=""
load_step() {
  local n="$1" rate="$2" target=$(( $1 * $2 )) snap="$RUN/restarts-$1x$2"
  local planned=$(( n * 128 * 1024 * 1024 )) used; used="$(minikube_mem_bytes)"
  if (( used + planned > MINIKUBE_MEM_BYTES * 90 / 100 )); then
    VERDICT="stopped: $n streamers would push minikube past 90% memory"; return
  fi
  restart_snapshot > "$snap"
  STREAMER_ARGS=(--set resources.limits.cpu=500m --set resources.limits.memory=128Mi --set streamer.goMemLimit=100MiB)
  streamers_up "$n" "$rate" "load-$n-$rate" || { VERDICT="stopped: streamers did not start"; STREAMER_ARGS=(); return; }
  STREAMER_ARGS=()
  sleep 30
  local rpid="" rout="$RUN/load-$n-$rate-readers.tsv"
  if (( LOAD_READERS > 0 )); then # readers for the measured part of the step, as in api-load
    python3 "$E2E_DIR/api_checks.py" load --threads "$LOAD_READERS" --seconds "$(( STEP_SECONDS - 35 ))" --api "$(api_base)" \
      --ns "$NS" --ch-pods "$(for s in $(seq 0 $((SHARDS - 1))); do printf '%s,' "$(pod "$s" 0)"; done)" \
      --swagger "$ROOT/api-gateway/openapi/swagger.json" >"$rout" 2>"$RUN/load-$n-$rate-readers.err" &
    rpid=$!
  fi
  local lag0; lag0="$(lag)"; local mem_peak=0 m end=$(( $(date +%s) + STEP_SECONDS - 30 ))
  while (( $(date +%s) < end )); do
    m="$(minikube_mem_pct)"; m="${m%.*}"; (( ${m:-0} > mem_peak )) && mem_peak=${m:-0}
    (( mem_peak > 85 )) && break
    sleep 5
  done
  local lag1; lag1="$(lag)"; local secs=$(( STEP_SECONDS - 30 ))
  local w="60s"
  local emitted accepted blocked ackp95 inserted insp95 colls cpu_b cpu_c cpu_s cpu_ch
  emitted="$(prom "sum(rate(streamer_rows_emitted_total{app_kubernetes_io_instance=\"$REL\"}[$w]))")"
  accepted="$(prom "sum(rate(broker_records_accepted_total[$w]))")"
  blocked="$(prom "sum(rate(streamer_backpressure_blocked_seconds_total{app_kubernetes_io_instance=\"$REL\"}[$w])) / $n")"
  ackp95="$(prom "histogram_quantile(0.95, sum by (le) (rate(broker_ack_latency_seconds_bucket[$w])))")"
  inserted="$(prom "sum(rate(collector_rows_inserted_total[$w]))")"
  insp95="$(prom "histogram_quantile(0.95, sum by (le) (rate(collector_insert_duration_seconds_bucket[$w])))")"
  colls="$(collector_replicas)"
  # CPU from cAdvisor (pod-level series, recorded every 30 s, so a 2 m window)
  local cw="2m" cpu_node
  cpu_b="$(prom "sum(rate(container_cpu_usage_seconds_total{namespace=\"$NS\",container=\"pod\",pod=\"broker-0\"}[$cw]))")"
  cpu_c="$(prom "sum(rate(container_cpu_usage_seconds_total{namespace=\"$NS\",container=\"pod\",pod=~\"collector-.*\"}[$cw]))")"
  cpu_s="$(prom "sum(rate(container_cpu_usage_seconds_total{namespace=\"$NS\",container=\"pod\",pod=~\"$REL-.*\"}[$cw]))")"
  cpu_ch="$(prom "sum(rate(container_cpu_usage_seconds_total{namespace=\"$NS\",container=\"pod\",pod=~\"chi-.*\"}[$cw]))")"
  cpu_node="$(prom "sum(rate(container_cpu_usage_seconds_total{container=\"pod\"}[$cw]))")"
  # rows written into each ClickHouse pod's local table (MergeTreeDataWriterRows counts storage
  # writes only, not the receiving node's Distributed insert); instance IP mapped to sXrY
  local shards="" kv ip pname ipmap
  ipmap="$(kubectl -n "$NS" get pods -o jsonpath='{range .items[*]}{.status.podIP}={.metadata.name}{"\n"}{end}')"
  while read -r kv; do
    ip="${kv%%:*}"; pname="$(grep "^$ip=" <<<"$ipmap" | head -1 | cut -d= -f2)"
    pname="${pname#chi-$FN-$CLUSTER-}"
    [[ "$pname" =~ ^([0-9]+)-([0-9]+)-0$ ]] && pname="s${BASH_REMATCH[1]}r${BASH_REMATCH[2]}"
    shards+="${pname:-$ip}:$(fmt "${kv##*=}") "
  done < <(prom_by instance "sum by (instance) (rate(ClickHouseProfileEvents_MergeTreeDataWriterRows{service=\"telemetry-store-metrics\"}[$w]))")
  local ooms; ooms="$(oom_since "$snap" | tr '\n' ' ')"
  local readers=""
  if [[ -n "$rpid" ]]; then
    wait "$rpid"
    readers="; readers: $(awk -F'\t' '$1 == "REPORT" { sub(/^load: /, "", $2); print $2 }' "$rout")"
    awk -F'\t' '$1 == "FAIL"' "$rout" | grep -q . && bad "load $n x $rate: API readers" "$(awk -F'\t' '$1 == "FAIL" { print $2 ": " $3 }' "$rout" | head -2)"
  fi
  streamers_down

  # Verdict: generator, producer path (backpressure), consumer path (lag rising), memory, or sustained.
  VERDICT="$(python3 - "$target" "${emitted:-0}" "${accepted:-0}" "${blocked:-0}" "${lag0:-0}" "${lag1:-0}" "$secs" "$mem_peak" "$ooms" \
    "${cpu_b:-0}" "${cpu_c:-0}" "${colls:-1}" "${cpu_s:-0}" "$n" "${cpu_ch:-0}" "${cpu_node:-0}" "$MINIKUBE_CPUS" <<'PY'
import sys
t, em, acc, blk, l0, l1, secs, mem = (float(x) for x in sys.argv[1:9])
ooms = sys.argv[9].strip()
cb, cc, nc, cs, ns, cch, cnode, ncpu = (float(x) for x in sys.argv[10:18])
# Components with a CPU limit are measured against it; the node against minikube's CPUs.
util = {"broker": cb / 0.5, "collectors": cc / (0.2 * max(nc, 1)), "streamers": cs / (0.5 * ns)}
busiest = max(util, key=util.get)
node = cnode / ncpu
if node >= 0.85:
    hint = f"minikube CPU saturated ({cnode:.1f} of {ncpu:.0f} cores; ClickHouse {cch:.1f})"
else:
    hint = f"busiest: {busiest} at {util[busiest]*100:.0f}% of its CPU limit; node {node*100:.0f}%"
if ooms:
    print(f"stopped: restarts/OOM ({ooms}); {hint}")
elif mem > 85:
    print(f"stopped: minikube memory {mem:.0f}%; {hint}")
elif blk > 0.10:
    print(f"pipeline-limited (producer path: streamers blocked {blk*100:.0f}% of the time); {hint}")
elif (l1 - l0) > 0.10 * t * secs:
    print(f"pipeline-limited (consumer path: lag rose {l1-l0:,.0f}); {hint}")
elif em < 0.95 * t:
    print(f"generator-limited (streamers emitted {em:,.0f} of {t:,.0f}); {hint}")
else:
    print(f"sustained; {hint}")
PY
)"
  report "| $n x $(fmt "$rate") = $(fmt "$target") | $(fmt "$emitted") | $(fmt "$accepted") | $(fmt "$ackp95") | ${lag0:-?} -> ${lag1:-?} | $colls | $(fmt "$inserted") | $(fmt "$insp95") | ${shards:--} | b $(fmt "$cpu_b") / c $(fmt "$cpu_c") / s $(fmt "$cpu_s") / ch $(fmt "$cpu_ch") / node $(fmt "$cpu_node") | ${mem_peak}% | $VERDICT$readers |"
  say "load $n x $rate: emitted $(fmt "$emitted")/s accepted $(fmt "$accepted")/s inserted $(fmt "$inserted")/s lag ${lag0:-?}->${lag1:-?} collectors $colls -> $VERDICT"
}

case_load() {
  note "load: ramp ${LOAD_STEPS} (streamers:rows/s each), ${STEP_SECONDS}s per step"
  local t; t="$(now_ns)"
  report ""; report "## Load"; report ""
  report "| Streamers x rate | Emitted/s | Accepted/s | Ack p95 s | Lag start -> end | Collectors | Inserted/s | Insert p95 s | Rows/s stored per ClickHouse pod (shard s, replica r) | CPU cores (broker/collectors/streamers/ClickHouse/all pods) | minikube mem | Verdict |"
  report "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
  local step n rate steps=0 ceiling=""
  for step in $LOAD_STEPS; do
    n="${step%%:*}"; rate="${step##*:}"
    load_step "$n" "$rate"; steps=$((steps + 1))
    if [[ "$VERDICT" != sustained* ]]; then ceiling="$((n * rate)) rows/s: $VERDICT"; break; fi
  done
  report ""
  report "Ceiling: ${ceiling:-not reached (every step sustained)}"
  say "load: ceiling ${ceiling:-not reached}"
  ok "load: $steps step(s) measured"
  local stopped; stopped="$(cat "$RUN"/load-*.log 2>/dev/null | grep -c "streamer stopped")"
  wait_lag_zero 900 >/dev/null
  local acked unacked; read -r acked unacked _ <<<"$(cat "$RUN"/load-*.log 2>/dev/null | grep "streamer stopped" | awk '{
    for (i=1;i<=NF;i++) { split($i,kv,"="); if (kv[1]=="records_acked") a+=kv[2]; if (kv[1]=="unacked_records") u+=kv[2] } }
    END { printf "%d %d\n", a, u }')"
  local distinct; distinct="$(distinct_since "$t")"
  report "- load: acked $acked, unacked $unacked, distinct rows $distinct"
  if [[ "$unacked" == "0" ]]; then check "load: no loss across all steps" "$acked" "$distinct"
  else (( distinct >= acked && distinct <= acked + unacked )) && ok "load: no loss" || bad "load: loss" "acked=$acked unacked=$unacked distinct=$distinct"; fi
}

case_scale() {
  note "scale: watch it in OpenLens (Config > Horizontal Pod Autoscalers > keda-hpa-collector-scaler)"
  local t; t="$(now_ns)"
  collector_set --set keda.lagTarget=2000 --set keda.pollingInterval=5 --set keda.cooldownPeriod=30 \
    --set keda.scaleUp.pods=2 --set keda.scaleUp.periodSeconds=15 \
    --set keda.scaleDown.stabilizationSeconds=30 --set keda.scaleDown.pods=1 --set keda.scaleDown.periodSeconds=15 ||
    { bad "scale: faster KEDA settings" "helm failed"; return; }
  local s
  say "scale: waiting for the collectors to settle at 1 (an earlier case may have scaled them up)"
  for s in $(seq 1 60); do [[ "$(collector_replicas)" == "1" ]] && break; sleep 5; done
  local start; start="$(collector_replicas)"; local maxr="$start" r last="$start"
  report "- scale: timeline (replicas): start $start"
  streamers_up 6 5000 scale || return
  for s in $(seq 1 24); do
    sleep 5; r="$(collector_replicas)"
    [[ "$r" != "$last" ]] && { say "scale: collectors $last -> $r (lag $(lag))"; report "  - $(date +%T) up: $last -> $r"; last=$r; }
    (( r > maxr )) && maxr=$r
  done
  streamers_down
  (( maxr >= start + 2 )) && ok "scale: collectors scaled up ($start -> $maxr)" || bad "scale: scale-up" "$start -> $maxr"
  local down_at=""
  for s in $(seq 1 60); do
    sleep 5; r="$(collector_replicas)"
    [[ "$r" != "$last" ]] && { say "scale: collectors $last -> $r (lag $(lag))"; report "  - $(date +%T) down: $last -> $r"; last=$r; }
    (( r <= 1 )) && { down_at=$((s * 5)); break; }
  done
  [[ -n "$down_at" ]] && ok "scale: collectors back to 1 in ${down_at}s after the load stopped" || bad "scale: scale-down" "still $last after 300s"
  loss_check scale "$t" 6
  make -s -C "$ROOT" deploy SERVICE=collector >"$RUN/restore-collector.log" 2>&1 && COLLECTOR_CHANGED=0
}

# broker-0 dies under load. Its streamer must move to another broker at once, without pausing
# (its blocked-time counter must not move), come back home once broker-0 is back, and nothing
# acknowledged may be lost: what broker-0 held is consumed after its restart.
case_broker_crash() {
  local mode flag t uid0 uid ready i n=2 nb; nb="$(brokers)"
  if (( nb < 2 )); then bad "broker-crash" "needs at least 2 brokers, found $nb"; return; fi
  for mode in graceful hard; do
    note "broker-crash ($mode): delete broker-0 under $n x 5000 rows/s; e2e-streamer-0 must fail over without pausing"
    t="$(now_ns)"
    streamers_up "$n" 5000 "broker-$mode" || return
    pf_streamers "$n"
    sleep 20
    check "broker-crash ($mode): e2e-streamer-0 starts on its home broker-0" "0" "$(streamer_metric 0 streamer_connected_broker)"
    local blocked0=() fo0=() lastack=() lastt=() maxgap=() seen=() a now t_del
    for ((i = 0; i < n; i++)); do
      blocked0[i]="$(streamer_metric "$i" streamer_backpressure_blocked_seconds_total)"
      fo0[i]="$(failovers "$i")"
    done
    flag=(--wait=false); [[ "$mode" == "hard" ]] && flag+=(--grace-period=0 --force)
    uid0="$(kubectl -n "$NS" get pod broker-0 -o jsonpath='{.metadata.uid}')"
    t_del="$(date +%s%3N)"
    kubectl -n "$NS" delete pod broker-0 "${flag[@]}" >/dev/null 2>&1
    for ((i = 0; i < n; i++)); do lastack[i]="$(streamer_metric "$i" streamer_records_acked_total)"; lastt[i]="$t_del"; maxgap[i]=0; done
    # Sample the acked counters until broker-0's replacement is Ready: the longest stretch without
    # a new ack is each streamer's failover gap.
    ready=""
    for k in $(seq 1 600); do
      now="$(date +%s%3N)"
      for ((i = 0; i < n; i++)); do
        a="$(streamer_metric "$i" streamer_records_acked_total)"
        if [[ -n "$a" && "$a" != "${lastack[i]}" ]]; then
          (( now - lastt[i] > maxgap[i] )) && maxgap[i]=$(( now - lastt[i] ))
          lastack[i]="$a"; lastt[i]="$now"
        fi
      done
      seen+=("$(streamer_metric 0 streamer_connected_broker)")
      if (( k % 8 == 0 )); then
        read -r uid ready <<<"$(kubectl -n "$NS" get pod broker-0 -o jsonpath='{.metadata.uid} {.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
        [[ -n "$uid" && "$uid" != "$uid0" && "$ready" == "True" ]] && break
      fi
      sleep 0.25
    done
    local outage=$(( $(date +%s%3N) - t_del ))
    [[ "$ready" == "True" ]] && ok "broker-crash ($mode): broker-0 back after ${outage} ms" || bad "broker-crash ($mode): broker-0 back" "not Ready"
    pf_broker
    for ((i = 0; i < n; i++)); do
      local b1; b1="$(streamer_metric "$i" streamer_backpressure_blocked_seconds_total)"
      python3 -c 'import sys; sys.exit(0 if float(sys.argv[1] or 0) == float(sys.argv[2] or 0) else 1)' "${blocked0[i]}" "$b1" &&
        ok "broker-crash ($mode): e2e-streamer-$i never paused (blocked time unchanged)" ||
        bad "broker-crash ($mode): e2e-streamer-$i paused" "blocked ${blocked0[i]} -> $b1 s"
    done
    (( $(failovers 0) > ${fo0[0]:-0} )) && ok "broker-crash ($mode): e2e-streamer-0 failed over" ||
      bad "broker-crash ($mode): e2e-streamer-0 failover" "failovers ${fo0[0]} -> $(failovers 0)"
    printf '%s\n' "${seen[@]}" | grep -qE '^[1-9]' && ok "broker-crash ($mode): e2e-streamer-0 streamed to another broker meanwhile" ||
      bad "broker-crash ($mode): e2e-streamer-0 moved" "connected brokers seen: $(printf '%s\n' "${seen[@]}" | sort -u | tr '\n' ' ')"
    say "broker-crash ($mode): longest gap without acks: e2e-streamer-0 ${maxgap[0]} ms, e2e-streamer-1 ${maxgap[1]} ms; outage ${outage} ms"
    report "- broker-crash ($mode): broker-0 outage ${outage} ms; longest gap without acks e2e-streamer-0 ${maxgap[0]} ms, e2e-streamer-1 ${maxgap[1]} ms"
    local home=""
    for _ in $(seq 1 60); do [[ "$(streamer_metric 0 streamer_connected_broker)" == "0" ]] && { home=1; break; }; sleep 1; done
    [[ -n "$home" ]] && ok "broker-crash ($mode): e2e-streamer-0 returned to its home broker-0" || bad "broker-crash ($mode): return home" "still on $(streamer_metric 0 streamer_connected_broker)"
    kubectl -n "$NS" logs broker-0 | grep -q "producer table rebuilt from the log" &&
      ok "broker-crash ($mode): broker-0 rebuilt its dedupe table from the log" || bad "broker-crash ($mode)" "no rebuild log line"
    local members=""
    for _ in $(seq 1 30); do members="$(broker_metric_on 0 broker_group_members)"; [[ "${members%.*}" =~ ^[1-9] ]] && break; sleep 1; done
    [[ "${members%.*}" =~ ^[1-9] ]] && ok "broker-crash ($mode): collectors rejoined broker-0's group" ||
      bad "broker-crash ($mode): collectors on broker-0" "members ${members:-?}"
    streamers_down
    loss_check "broker-$mode" "$t" "$n"
  done
}

# broker-partition: broker-0 keeps running but nothing reaches it (a lost node or a network split).
# Its streams stall rather than break, so this is the slow failover path: the ack timeout. The
# rate per streamer is PARTITION_RATE (default 5000); above about 20000 the window's record cap,
# not its 3 s, bounds it.
case_broker_partition() {
  local n=2 rate="${PARTITION_RATE:-5000}" t nb ip i k a now t_cut; nb="$(brokers)"
  if (( nb < 2 )); then bad "broker-partition" "needs at least 2 brokers, found $nb"; return; fi
  note "broker-partition: cut broker-0 off the network for 20 s under $n x $rate rows/s; e2e-streamer-0 must fail over without pausing"
  t="$(now_ns)"
  (( rate > 5000 )) && STREAMER_ARGS=(--set resources.limits.cpu=1 --set resources.limits.memory=128Mi --set streamer.goMemLimit=100MiB)
  streamers_up "$n" "$rate" "broker-partition-$rate" || { STREAMER_ARGS=(); return; }
  STREAMER_ARGS=()
  pf_streamers "$n"
  sleep 20
  check "broker-partition: e2e-streamer-0 starts on its home broker-0" "0" "$(streamer_metric 0 streamer_connected_broker)"
  local blocked0=() acktimeouts0 lastack=() lastt=() maxgap=() seen=()
  for ((i = 0; i < n; i++)); do blocked0[i]="$(streamer_metric "$i" streamer_backpressure_blocked_seconds_total)"; done
  acktimeouts0="$(failovers_for 0 ack_timeout)"
  local restarts0; restarts0="$(kubectl -n "$NS" get pod broker-0 -o jsonpath='{.status.containerStatuses[0].restartCount}')"
  ip="$(kubectl -n "$NS" get pod broker-0 -o jsonpath='{.status.podIP}')"
  t_cut="$(date +%s%3N)"
  partition_on "$ip"
  say "broker-partition: broker-0 ($ip) cut off"
  for ((i = 0; i < n; i++)); do lastack[i]="$(streamer_metric "$i" streamer_records_acked_total)"; lastt[i]="$t_cut"; maxgap[i]=0; done
  for k in $(seq 1 80); do # 20 s: the longest stretch without a new ack is each streamer's failover gap
    now="$(date +%s%3N)"
    for ((i = 0; i < n; i++)); do
      a="$(streamer_metric "$i" streamer_records_acked_total)"
      if [[ -n "$a" && "$a" != "${lastack[i]}" ]]; then
        (( now - lastt[i] > maxgap[i] )) && maxgap[i]=$(( now - lastt[i] ))
        lastack[i]="$a"; lastt[i]="$now"
      fi
    done
    seen+=("$(streamer_metric 0 streamer_connected_broker)")
    sleep 0.25
  done
  partition_off
  say "broker-partition: broker-0 reachable again"
  check "broker-partition: broker-0 stayed up throughout (no restart)" "$restarts0" \
    "$(kubectl -n "$NS" get pod broker-0 -o jsonpath='{.status.containerStatuses[0].restartCount}')"
  for ((i = 0; i < n; i++)); do
    local b1; b1="$(streamer_metric "$i" streamer_backpressure_blocked_seconds_total)"
    python3 -c 'import sys; sys.exit(0 if float(sys.argv[1] or 0) == float(sys.argv[2] or 0) else 1)' "${blocked0[i]}" "$b1" &&
      ok "broker-partition: e2e-streamer-$i never paused (blocked time unchanged)" ||
      bad "broker-partition: e2e-streamer-$i paused" "blocked ${blocked0[i]} -> $b1 s"
  done
  (( $(failovers_for 0 ack_timeout) > acktimeouts0 )) && ok "broker-partition: e2e-streamer-0 failed over on the ack timeout" ||
    bad "broker-partition: e2e-streamer-0 ack-timeout failover" "ack_timeout failovers $acktimeouts0 -> $(failovers_for 0 ack_timeout)"
  printf '%s\n' "${seen[@]}" | grep -qE '^[1-9]' && ok "broker-partition: e2e-streamer-0 streamed to another broker meanwhile" ||
    bad "broker-partition: e2e-streamer-0 moved" "connected brokers seen: $(printf '%s\n' "${seen[@]}" | sort -u | tr '\n' ' ')"
  say "broker-partition: longest gap without acks: e2e-streamer-0 ${maxgap[0]} ms, e2e-streamer-1 ${maxgap[1]} ms"
  report "- broker-partition ($n x $rate rows/s): longest gap without acks e2e-streamer-0 ${maxgap[0]} ms, e2e-streamer-1 ${maxgap[1]} ms"
  local home=""
  for _ in $(seq 1 60); do [[ "$(streamer_metric 0 streamer_connected_broker)" == "0" ]] && { home=1; break; }; sleep 1; done
  [[ -n "$home" ]] && ok "broker-partition: e2e-streamer-0 returned to its home broker-0" || bad "broker-partition: return home" "still on $(streamer_metric 0 streamer_connected_broker)"
  streamers_down
  loss_check "broker-partition-$rate" "$t" "$n"
}

case_collector_crash() {
  note "collector-crash: force-delete a collector under 10k rows/s"
  local t; t="$(now_ns)"
  streamers_up 2 5000 collector-crash || return
  sleep 20
  local rb0 victim; rb0="$(broker_metric broker_rebalances_total)"
  victim="$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=collector -o jsonpath='{.items[0].metadata.name}')"
  kubectl -n "$NS" delete pod "$victim" --grace-period=0 --force >/dev/null 2>&1
  say "collector-crash: killed $victim"
  sleep 30
  local rb1; rb1="$(broker_metric broker_rebalances_total)"
  (( ${rb1%.*} > ${rb0%.*} )) && ok "collector-crash: group rebalanced ($rb0 -> $rb1)" || bad "collector-crash: rebalance" "$rb0 -> $rb1"
  streamers_down
  loss_check collector-crash "$t" 2
}

case_db_down() {
  note "db-down: ClickHouse shard 1 down for 60 s under 10k rows/s"
  local t; t="$(now_ns)"
  streamers_up 2 5000 db-down || return
  sleep 15
  local lag_before err0; lag_before="$(lag)"
  err0="$(prom 'sum(collector_insert_errors_total)')"
  db_down; say "db-down: shard 1 scaled to 0"
  sleep 60
  local lag_down; lag_down="$(lag)"
  (( ${lag_down:-0} > ${lag_before:-0} + 50000 )) && ok "db-down: nothing committed while ClickHouse is down (lag ${lag_before} -> ${lag_down})" ||
    bad "db-down: lag growth" "${lag_before} -> ${lag_down}"
  local err1; err1="$(prom 'sum(collector_insert_errors_total)')"
  python3 -c "import sys; sys.exit(0 if float(sys.argv[2] or 0) > float(sys.argv[1] or 0) else 1)" "$err0" "$err1" &&
    ok "db-down: collectors report insert errors and retry" || bad "db-down: insert errors" "$err0 -> $err1"
  db_up; say "db-down: shard 1 back"
  streamers_down
  local t_drain; t_drain="$(date +%s)"
  wait_lag_zero 600 && report "- db-down: backlog of ${lag_down} records drained in $(( $(date +%s) - t_drain ))s after ClickHouse returned"
  loss_check db-down "$t" 2
}

# A broker at its quota stops reading its producers: to a streamer that is a broker unavailable
# for writes, so it moves to another one (ack timeout). Every broker filling up, the case where
# streaming pauses, is deliberately not asserted.
case_backpressure() {
  note "backpressure: broker quota 16 MB with ClickHouse down; full brokers pause producers, streamers move on, nothing dropped"
  local t; t="$(now_ns)"
  broker_set --set broker.quotaMB=16 || { bad "backpressure: set quota" "helm failed"; return; }
  streamers_up 2 5000 backpressure || return
  pf_streamers 2
  local fo0; fo0="$(( $(failovers 0) + $(failovers 1) ))"
  sleep 10
  db_down; say "backpressure: shard 1 scaled to 0"
  local blocked="" s
  for s in $(seq 1 45); do
    [[ "$(broker_metric broker_quota_blocked max)" == "1" ]] && { blocked=$((s * 2)); break; }
    sleep 2
  done
  [[ -n "$blocked" ]] && ok "backpressure: broker paused producers at the quota after ~${blocked}s" || bad "backpressure: quota" "never blocked"
  sleep 15
  local fo1; fo1="$(( $(failovers 0) + $(failovers 1) ))"
  (( fo1 > fo0 )) && ok "backpressure: streamers moved off the full broker ($fo0 -> $fo1 failovers)" ||
    bad "backpressure: streamers moved off the full broker" "failovers $fo0 -> $fo1"
  report "- backpressure: quota reached after ~${blocked:-?}s; streamers failed over $((fo1 - fo0)) time(s)"
  db_up; say "backpressure: shard 1 back"
  local cleared=""
  for s in $(seq 1 120); do
    [[ "$(broker_metric broker_quota_blocked max)" == "0" ]] && { cleared=$((s * 2)); break; }
    sleep 2
  done
  [[ -n "$cleared" ]] && ok "backpressure: producers resumed ${cleared}s after ClickHouse returned" || bad "backpressure: resume" "still blocked"
  streamers_down
  loss_check backpressure "$t" 2
  make -s -C "$ROOT" deploy SERVICE=broker >"$RUN/restore-broker.log" 2>&1 && BROKER_CHANGED=0
  pf_broker
}

# ---- the API gateway, live ------------------------------------------------------------------------
# What the API answers is compared with what ClickHouse holds (e2e/api_checks.py).

case_api() {
  note "api: GPU list, full history, windows, limit and errors equal ClickHouse"
  local t; t="$(now_ns)"
  streamers_up 2 1000 api || return
  sleep 60
  streamers_down
  wait_lag_zero 300 || bad "api: queue drained" "lag $(lag)"
  sync_all
  api_checks api correctness
  loss_check api "$t" 2
}

# The gateway reads a full history in chunks; with 500-row chunks a busy GPU crosses many boundaries.
case_api_chunks() {
  note "api-chunks: full histories read in 500-row chunks equal ClickHouse"
  gateway_set --set gateway.chunkRows=500 || { bad "api-chunks: set chunkRows" "helm failed"; return; }
  sync_all
  api_checks api-chunks chunks --chunk-rows 500 --min-rows 1500
  make -s -C "$ROOT" deploy SERVICE=api-gateway >"$RUN/restore-api-gateway.log" 2>&1 && GATEWAY_CHANGED=0
}

case_api_load() {
  note "api-load: 8 readers for 60 s during 2 x 1000 rows/s of ingest"
  local t; t="$(now_ns)"
  streamers_up 2 1000 api-load || return
  sleep 10
  api_checks api-load load --threads 8 --seconds 60
  streamers_down
  loss_check api-load "$t" 2
}

case_api_rollout() {
  note "api-rollout: the gateway restarts under 4 readers; no request may fail"
  api_checks api-rollout rollout --threads 4
}

# One ClickHouse replica gone: reads use the other. A whole shard gone: its GPUs get a JSON 5xx,
# the other shard's GPUs are still served, and everything is served again once it is back.
case_api_db_down() {
  note "api-db-down: one replica down, then a whole shard down"
  sync_all
  local g0 g1 victim
  g0="$(admin 0 0 "SELECT toString(gpu_uuid) FROM telemetry_local GROUP BY gpu_uuid ORDER BY count() DESC LIMIT 1")"
  g1="$(admin 1 0 "SELECT toString(gpu_uuid) FROM telemetry_local GROUP BY gpu_uuid ORDER BY count() DESC LIMIT 1")"
  victim="$(pod 1 0)"
  # The api user's ClickHouse quota is per minute, and the api-load and api-rollout cases before this
  # one use it up on purpose: start in a fresh minute, so a 429 here can only come from the replica.
  sleep $(( 61 - 10#$(date +%S) ))
  kubectl -n "$NS" delete pod "$victim" --wait=false >/dev/null 2>&1
  say "api-db-down: deleted $victim (shard 1, replica 0)"
  api_checks api-db-down replica --gpu "$g1" --killed "$victim" --count-pod "$(pod 0 0)"
  kubectl -n "$NS" wait --for=condition=Ready "pod/$victim" --timeout=300s >/dev/null 2>&1
  db_down; say "api-db-down: shard 1 scaled to 0"
  api_checks api-db-down shard-down --gpu-up "$g0" --gpu-down "$g1"
  db_up; say "api-db-down: shard 1 back"
  api_checks api-db-down shard-up --gpu-down "$g1"
}

for c in $CASES; do
  fn="case_${c//-/_}"
  if declare -F "$fn" >/dev/null; then
    echo; "$fn"
  else
    bad "unknown case $c" "valid: $ALL_CASES"
  fi
done
