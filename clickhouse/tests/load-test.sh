#!/usr/bin/env bash
# Write load test against a deployed release. Ramps the insert rate in steps and measures, per
# step: rows/s achieved, insert latency, failed/delayed/rejected inserts, parts per partition,
# memory against the cap, CPU used by ClickHouse, and compressed bytes per row.
#
# Loaders behave like collectors: client pods generate realistic rows (many GPUs and metrics,
# noisy values) with clickhouse-local and send one batch per loader per second, in Native format,
# through the cluster Service, waiting for the acknowledgement (insert_distributed_sync=1).
#
# It TRUNCATES telemetry before and after every step (to stay inside the disk budget), so it only
# runs with CONFIRM_WIPE=yes. Never point it at a release holding data you need.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"

[[ "${CONFIRM_WIPE:-}" == "yes" ]] || { echo "load-test truncates telemetry; rerun with CONFIRM_WIPE=yes" >&2; exit 2; }

STEPS="${STEPS:-10000 30000 100000 250000 500000}"   # target rows/s, in order
STEP_SECONDS="${STEP_SECONDS:-90}"
LOADERS="${LOADERS:-4}"
PASS_RATE="${PASS_RATE:-30000}"           # every step up to this rate must pass
MAX_BYTES="${MAX_BYTES:-3000000000}"      # stop a step if telemetry_local holds more (all replicas)
GPUS="${GPUS:-250}"; METRICS="${METRICS:-40}"
STOP_ON_FAIL="${STOP_ON_FAIL:-1}"

WORK="$(mktemp -d)"
LOADER_PODS=(); for l in $(seq 1 "$LOADERS"); do LOADER_PODS+=("${FN}-load-$l"); done
cleanup() {
  for p in "${LOADER_PODS[@]}"; do stop_client_pod "$p"; done
  rm -rf "$WORK"
}
trap cleanup EXIT

wipe() { admin 0 0 "TRUNCATE TABLE telemetry_local ON CLUSTER $CLUSTER SYNC" >/dev/null; }
# cluster_value <sql over clusterAllReplicas>: one number from the admin view of all replicas.
cv() { admin 0 0 "$1" 2>/dev/null || echo 0; }
events_total() { cv "SELECT sum(value) FROM clusterAllReplicas('$CLUSTER', system.events) WHERE event = '$1'"; }
cpu_usec_total() { # CPU time used by all ClickHouse containers, from their cgroups
  local t=0 u
  for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 0 $((REPLICAS-1))); do
    u="$(kubectl -n "$NS" exec "$(pod "$s" "$r")" -c clickhouse -- sh -c "awk '/^usage_usec/{print \$2}' /sys/fs/cgroup/cpu.stat" 2>/dev/null || echo 0)"
    t=$((t + u))
  done; done
  echo "$t"
}

# The loader, run inside each loader pod: one batch per second for the step's duration. It prints
# "rc ms rows" per insert to stdout, which streams back to the host (so a killed pod loses nothing).
# Args: rows_per_batch seconds streamer_id gpus metrics service_host
read -r -d '' LOADER_SCRIPT <<'EOS'
rows=$1; secs=$2; sid=$3; G=$4; M=$5; host=$6
end=$(( $(date +%s) + secs ))
while (( $(date +%s) < end )); do
  t0=$(date +%s%N)
  clickhouse-local --query "
    SELECT
      toUUID(concat('00000000-0000-4000-8000-', leftPad(toString(n % $G), 12, '0'))) AS gpu_uuid,
      fromUnixTimestamp64Nano(toInt64($t0)) + toIntervalMicrosecond(intDiv(n, $G * $M)) AS ts,
      CAST(concat('DCGM_FI_DEV_M', toString(intDiv(n, $G) % $M)) AS LowCardinality(String)) AS metric_name,
      toUInt16($sid) AS streamer_id,
      round(50 + 30 * sin(n / 977) + randNormal(0, 3), 2) AS value
    FROM (SELECT number AS n FROM numbers($rows))
    FORMAT Native" \
  | clickhouse-client --host "$host" --port 9000 \
      --query "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value) SETTINGS insert_distributed_sync = 1 FORMAT Native"
  rc=$?
  ms=$(( ($(date +%s%N) - t0) / 1000000 ))
  echo "$rc $ms $rows"
  (( ms < 1000 )) && sleep "0.$(printf %03d $((1000 - ms)))"
done
exit 0
EOS

note "starting $LOADERS loader pods"
for p in "${LOADER_PODS[@]}"; do start_client_pod "$p" 512Mi & done; wait
wipe

results=()
step=0; stopped=""
for rate in $STEPS; do
  step=$((step+1))
  per_loader=$(( (rate + LOADERS - 1) / LOADERS ))
  note "step $step: $rate rows/s target ($LOADERS loaders x $per_loader rows/s, ${STEP_SECONDS}s)"

  d0=$(events_total DelayedInserts); r0=$(events_total RejectedInserts); c0=$(cpu_usec_total)
  : >"$WORK/samples"; rm -f "$WORK/abort" "$WORK/done"
  step_start=$(date +%s)

  # Sampler: max parts per partition, memory against the server cap, merges, bytes on disk.
  (
    while [[ ! -e "$WORK/done" ]]; do
      cv "SELECT
            (SELECT max(value) FROM clusterAllReplicas('$CLUSTER', system.asynchronous_metrics) WHERE metric = 'MaxPartCountForPartition'),
            (SELECT max(u.value / c.cap) FROM
               (SELECT hostName() h, value FROM clusterAllReplicas('$CLUSTER', system.asynchronous_metrics) WHERE metric = 'jemalloc.resident') u
               JOIN (SELECT hostName() h, toFloat64(value) AS cap FROM clusterAllReplicas('$CLUSTER', system.server_settings) WHERE name = 'max_server_memory_usage') c USING h),
            (SELECT count() FROM clusterAllReplicas('$CLUSTER', system.merges)),
            (SELECT sum(bytes_on_disk) FROM clusterAllReplicas('$CLUSTER', system.parts) WHERE active AND table = 'telemetry_local')
          FORMAT TSV" >>"$WORK/samples"
      if [[ "$(tail -1 "$WORK/samples" | cut -f4)" -gt "$MAX_BYTES" ]] 2>/dev/null; then touch "$WORK/abort"; fi
      sleep 5
    done
  ) &
  SAMPLER=$!

  rm -f "$WORK"/lat.* "$WORK"/err.*
  start=$(date +%s); PIDS=()
  for l in $(seq 1 "$LOADERS"); do
    kubectl -n "$NS" exec "${LOADER_PODS[$((l-1))]}" -- bash -c "$LOADER_SCRIPT" loader \
      "$per_loader" "$STEP_SECONDS" "$((step * 100 + l))" "$GPUS" "$METRICS" "$SERVICE_HOST" \
      >"$WORK/lat.$l" 2>"$WORK/err.$l" &
    PIDS+=($!)
  done
  # Wait for the loaders; if the disk guard trips, delete the loader pods to stop them at once.
  while kill -0 "${PIDS[@]}" 2>/dev/null; do
    if [[ -e "$WORK/abort" ]]; then
      note "disk guard: telemetry_local passed $MAX_BYTES bytes; stopping this step"
      for p in "${LOADER_PODS[@]}"; do kubectl -n "$NS" delete pod "$p" --grace-period=0 --force >/dev/null 2>&1; done
      break
    fi
    sleep 2
  done
  wait "${PIDS[@]}" 2>/dev/null
  elapsed=$(( $(date +%s) - start ))
  touch "$WORK/done"; wait "$SAMPLER" 2>/dev/null

  # Client-side results from the loader logs: "rc ms rows" per insert.
  cat "$WORK"/lat.* 2>/dev/null | grep -E '^[0-9]+ [0-9]+ [0-9]+$' >"$WORK/lat"
  ok_rows=$(awk '$1==0{s+=$3} END{print s+0}' "$WORK/lat")
  inserts=$(wc -l <"$WORK/lat"); failed=$(awk '$1!=0' "$WORK/lat" | wc -l)
  read -r p50 p99 pmax <<<"$(awk '$1==0{print $2}' "$WORK/lat" | sort -n | awk '{a[NR]=$1} END{if(NR==0){print "- - -"; exit} i50=int(NR*0.5); if(i50<1)i50=1; i99=int(NR*0.99); if(i99<1)i99=1; print a[i50], a[i99], a[NR]}')"
  # The loaders run for exactly STEP_SECONDS unless the step was stopped early.
  secs=$STEP_SECONDS; [[ -e "$WORK/abort" ]] && secs=$elapsed
  achieved=$(( ok_rows / (secs > 0 ? secs : 1) ))
  # Server side: rows actually stored for this step (unique keys), and storage cost.
  stored=$(cv "SELECT count() FROM telemetry WHERE intDiv(streamer_id, 100) = $step")
  # Server-side insert latency (coordinator, including the synchronous hop to the shards).
  admin 0 0 "SYSTEM FLUSH LOGS ON CLUSTER $CLUSTER" >/dev/null 2>&1
  read -r s50 s99 smax serr <<<"$(cv "SELECT round(quantileExactIf(0.5)(query_duration_ms, type = 'QueryFinish')),
        round(quantileExactIf(0.99)(query_duration_ms, type = 'QueryFinish')), maxIf(query_duration_ms, type = 'QueryFinish'),
        countIf(type != 'QueryFinish')
      FROM clusterAllReplicas('$CLUSTER', system.query_log)
      WHERE event_time >= toDateTime($step_start) AND user = 'collector' AND is_initial_query
        AND query_kind = 'Insert' AND type != 'QueryStart' FORMAT TSV" | tr '\t' ' ')"
  bpr=$(cv "SELECT round(sum(data_compressed_bytes) / greatest(sum(rows), 1), 2) FROM clusterAllReplicas('$CLUSTER', system.parts) WHERE active AND table = 'telemetry_local'")
  d1=$(events_total DelayedInserts); r1=$(events_total RejectedInserts); c1=$(cpu_usec_total)
  delayed=$((d1 - d0)); rejected=$((r1 - r0))
  cores=$(awk -v u=$((c1 - c0)) -v s="$elapsed" 'BEGIN{printf "%.1f", u / 1e6 / (s > 0 ? s : 1)}')
  maxparts=$(cut -f1 "$WORK/samples" | sort -n | tail -1); maxmem=$(cut -f2 "$WORK/samples" | sort -g | tail -1)
  maxmerges=$(cut -f3 "$WORK/samples" | sort -n | tail -1)
  memcap=$(awk -v m="${maxmem:-0}" 'BEGIN{printf "%d%%", m * 100}')

  verdict=PASS
  (( achieved * 100 < rate * 95 )) && verdict=FAIL
  (( failed > 0 || rejected > 0 )) && verdict=FAIL
  [[ -e "$WORK/abort" ]] && verdict="STOP (disk)"
  results+=("$(printf '%-8s %-9s %-9s %-7s %-6s %-14s %-14s %-9s %-6s %-9s %-6s %-7s %s' \
    "$rate" "$achieved" "$stored" "$inserts" "$failed" "${s50}/${s99}/${smax}" "${p50}/${p99}/${pmax}" "$delayed/$rejected" \
    "${maxparts:-?}" "$memcap" "$cores" "$bpr" "$verdict")")
  note "step $step: $verdict  achieved $achieved rows/s, server insert latency p50/p99/max ${s50}/${s99}/${smax} ms (client incl. process start ${p50}/${p99}/${pmax}), failed $failed, delayed/rejected $delayed/$rejected, max parts ${maxparts:-?}, memory peak $memcap of cap, $cores cores, $bpr bytes/row"
  if [[ "$verdict" != PASS && $rate -le $PASS_RATE ]]; then bad "target $rate rows/s" "$verdict"; else
    [[ "$verdict" == PASS ]] && ok "target $rate rows/s"; fi
  errs="$(cat "$WORK"/err.* 2>/dev/null | grep -oE 'Code: [0-9]+[^.]*' | sort | uniq -c | sort -rn | head -3 | tr '\n' ';')"
  [[ -n "$errs" ]] && note "insert errors: $errs"
  # A stopped step deleted the loader pods; bring them back if the ramp continues.
  if [[ -e "$WORK/abort" && "$STOP_ON_FAIL" != "1" ]]; then for p in "${LOADER_PODS[@]}"; do start_client_pod "$p" 512Mi & done; wait; fi

  wipe
  if [[ "$verdict" != PASS && "$STOP_ON_FAIL" == "1" ]]; then stopped="$rate"; break; fi
  sleep 10   # let merges and the TRUNCATE settle before the next step
done

echo
echo "target   achieved  stored    inserts failed server ms      client ms      delay/rej parts  mem/cap   cores  B/row   result"
for l in "${results[@]}"; do echo "$l"; done
[[ -n "$stopped" ]] && note "ramp stopped at $stopped rows/s"
echo "(server ms / client ms = insert latency p50/p99/max; client includes starting clickhouse-local and"
echo " clickhouse-client for every batch, which a collector with a connection pool does not pay;"
echo " stored = rows kept for the step; mem/cap = peak jemalloc resident memory as a share of the server cap;"
echo " cores = CPU used by all ClickHouse pods together; B/row = compressed bytes per row per copy)"
summary
