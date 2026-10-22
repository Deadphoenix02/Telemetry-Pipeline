#!/usr/bin/env bash
# Replicated-mode and failure-mode tests against a deployed release (make deploy SERVICE=clickhouse).
# Needs kubectl access to the namespace. Every test uses its own metric name, so reruns and
# leftover data do not interfere. The failure tests scale StatefulSets down and back up and
# always restore them on exit.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"

RUN="t$(date +%s)"
load_creds
trap 'restore_all' EXIT

DOWN_STS=()
scale_down() { for s in "$@"; do kubectl -n "$NS" scale sts "$s" --replicas=0 >/dev/null; DOWN_STS+=("$s"); done; }
restore_all() {
  for s in "${DOWN_STS[@]:-}"; do [[ -n "$s" ]] && kubectl -n "$NS" scale sts "$s" --replicas=1 >/dev/null 2>&1; done
  DOWN_STS=()
}
keeper_sts() { # names of the StatefulSets that own the Keeper pods
  kubectl -n "$NS" get pods -l "app.kubernetes.io/component=keeper" \
    -o jsonpath='{range .items[*]}{.metadata.ownerReferences[0].name}{"\n"}{end}' | sort
}
count_metric() { qa "SELECT count() FROM telemetry WHERE metric_name='$1'"; }
local_count()  { admin "$1" "$2" "SELECT count() FROM telemetry_local WHERE metric_name='$3'"; }

G0="$(gpu_for_shard 0)"; G1="$(gpu_for_shard 1)"
[[ -n "$G0" && -n "$G1" ]] || { echo "could not find GPU ids for both shards" >&2; exit 1; }

# ---- 1. topology ----
note "1. topology"
check "all ClickHouse pods ready" "0" "$(wait_all_ready 120s && echo 0 || echo 1)"
check "cluster has shards x replicas hosts" "$((SHARDS*REPLICAS))" \
  "$(admin 0 0 "SELECT count() FROM system.clusters WHERE cluster='$CLUSTER'")"
check "cluster has the expected shard count" "$SHARDS" \
  "$(admin 0 0 "SELECT uniqExact(shard_num) FROM system.clusters WHERE cluster='$CLUSTER'")"
check "Keeper answers" "1" "$(admin 0 0 "SELECT count() > 0 FROM system.zookeeper WHERE path = '/'")"
for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 0 $((REPLICAS-1))); do
  check "pod $s-$r: telemetry_local is replicated" "ReplicatedReplacingMergeTree" \
    "$(admin "$s" "$r" "SELECT engine FROM system.tables WHERE database='default' AND name='telemetry_local'")"
  check "pod $s-$r: replica is writable" "0" \
    "$(admin "$s" "$r" "SELECT countIf(is_readonly) FROM system.replicas WHERE table IN ('telemetry_local','gpus_local')")"
done; done

# ---- 2. sharding and replication ----
note "2. sharding and replication"
M="sharding_$RUN"
insert_rows "$G0" "$M" 500; insert_rows "$G1" "$M" 300
check "Distributed count is the sum of both inserts" "800" "$(count_metric "$M")"
sync_all
check "shard 0 holds only its GPU's rows" "500" "$(local_count 0 0 "$M")"
check "shard 1 holds only its GPU's rows" "300" "$(local_count 1 0 "$M")"
for s in $(seq 0 $((SHARDS-1))); do
  check "shard $s replicas hold identical data" "$(local_count "$s" 0 "$M")" "$(local_count "$s" 1 "$M")"
done
check "API reads one GPU's rows" "500" "$(qa "SELECT count() FROM telemetry WHERE gpu_uuid='$G0' AND metric_name='$M'")"

# ---- 3. idempotent replay ----
note "3. idempotent replay"
M="dedup_$RUN"; BASE="toDateTime64('$(admin 0 0 "SELECT toStartOfMinute(now())")', 0)"
insert_rows "$G0" "$M" 100 1 "$BASE"; insert_rows "$G0" "$M" 100 1 "$BASE"
sync_all
check "LIMIT 1 BY hides replayed rows" "100" \
  "$(qa "SELECT count() FROM (SELECT ts FROM telemetry WHERE gpu_uuid='$G0' AND metric_name='$M' ORDER BY ts, metric_name, streamer_id LIMIT 1 BY gpu_uuid, ts, metric_name, streamer_id)")"
admin 0 0 "OPTIMIZE TABLE telemetry_local ON CLUSTER $CLUSTER FINAL" >/dev/null 2>&1
sync_all
check "merge collapses replays to unique keys" "100" "$(count_metric "$M")"

# ---- 4. access control on the cluster tables ----
note "4. access control"
DENIED='ACCESS_DENIED|Not enough privileges'
check "api cannot insert through the Distributed table" "yes" \
  "$(rejects "$DENIED" as api "$API_PW" 0 0 "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value) VALUES ('$G0', now(), 'x', 1, 1)")"
check "collector cannot read the local table" "yes" \
  "$(rejects "$DENIED" as collector "$COLLECTOR_PW" 0 0 "SELECT count() FROM telemetry_local")"
check "collector cannot read the Distributed table" "yes" \
  "$(rejects "$DENIED" as collector "$COLLECTOR_PW" 0 0 "SELECT count() FROM telemetry")"

check "api cannot raise its caps (constraint)" "yes" \
  "$(rejects 'SETTING_CONSTRAINT_VIOLATION|should not be changed' as api "$API_PW" 0 0 "SELECT 1 SETTINGS max_result_rows = 0")"
check "api cannot run DDL" "yes" \
  "$(rejects 'READONLY|ACCESS_DENIED|Not enough privileges|readonly' as api "$API_PW" 0 0 "CREATE TABLE api_probe (x UInt8) ENGINE = Memory")"

# ---- 5. one replica down ----
note "5. one replica down: inserts continue, the replica catches up"
M="replica_down_$RUN"
insert_rows "$G0" "$M" 100
kubectl -n "$NS" delete pod "$(pod 0 1)" --wait=false >/dev/null
sleep 2
check "insert succeeds while a replica is down" "0" "$(insert_rows "$G0" "$M" 100 2 >/dev/null 2>&1; echo $?)"
check "insert to the other shard succeeds" "0" "$(insert_rows "$G1" "$M" 100 2 >/dev/null 2>&1; echo $?)"
check "API can still read" "300" "$(count_metric "$M")"
check_alert "alert ClickHouseDown fires for the missing replica" ClickHouseDown yes 120
wait_pod_ready "$(pod 0 1)" 300s; wait_writable 300; sync_all
check "restarted replica caught up" "$(local_count 0 0 "$M")" "$(local_count 0 1 "$M")"

# ---- 6. one Keeper down ----
note "6. one Keeper down: quorum holds"
M="keeper1_$RUN"
KS=($(keeper_sts))
check "found three Keeper StatefulSets" "3" "${#KS[@]}"
scale_down "${KS[0]}"; sleep 10
check "insert succeeds with 2 of 3 Keepers" "0" "$(insert_rows "$G0" "$M" 100 >/dev/null 2>&1; echo $?)"
restore_all; kubectl -n "$NS" rollout status "sts/${KS[0]}" --timeout=300s >/dev/null

# ---- 7. Keeper quorum lost ----
note "7. Keeper quorum lost: writes stop, reads continue, recovery is automatic"
M="quorum_$RUN"
insert_rows "$G0" "$M" 100
scale_down "${KS[0]}" "${KS[1]}"
sleep 20
check "insert fails without quorum" "fail" "$(insert_rows "$G0" "$M" 100 2 >/dev/null 2>&1 && echo ok || echo fail)"
check "reads of existing data still work" "100" "$(count_metric "$M")"
check_alert "alert KeeperQuorumLost fires" KeeperQuorumLost yes 120
check_alert "alert ClickHouseReplicaReadOnly fires" ClickHouseReplicaReadOnly yes 120
restore_all
kubectl -n "$NS" rollout status "sts/${KS[0]}" --timeout=300s >/dev/null
kubectl -n "$NS" rollout status "sts/${KS[1]}" --timeout=300s >/dev/null
check "replicas return to writable" "0" "$(wait_writable 300 && echo 0 || echo 1)"
check "insert succeeds after recovery" "0" "$(insert_rows "$G0" "$M" 100 3 >/dev/null 2>&1; echo $?)"
sync_all
check "no rows lost across the outage" "200" "$(count_metric "$M")"
check_alert "alert KeeperQuorumLost clears after recovery" KeeperQuorumLost no 240
check_alert "alert ClickHouseReplicaReadOnly clears after recovery" ClickHouseReplicaReadOnly no 240

# ---- 8. a whole shard down ----
note "8. whole shard down: only that shard's GPUs are affected"
M="shard_down_$RUN"
insert_rows "$G0" "$M" 100; insert_rows "$G1" "$M" 100
scale_down "$(sts 1 0)" "$(sts 1 1)"
sleep 15
check "insert for the healthy shard succeeds" "0" "$(insert_rows "$G0" "$M" 100 2 >/dev/null 2>&1; echo $?)"
check "insert for the dead shard fails" "fail" "$(insert_rows "$G1" "$M" 100 2 >/dev/null 2>&1 && echo ok || echo fail)"
check "API reads the healthy shard's GPU" "200" \
  "$(qa "SELECT count() FROM telemetry WHERE gpu_uuid='$G0' AND metric_name='$M' SETTINGS optimize_skip_unused_shards=1")"
check "query touching the dead shard errors" "fail" "$(count_metric "$M" >/dev/null 2>&1 && echo ok || echo fail)"
check_alert "alert ClickHouseDown fires for the dead shard" ClickHouseDown yes 120
restore_all
wait_all_ready 300s; wait_writable 300; sync_all
check "all data present after the shard returns" "300" "$(count_metric "$M")"
check_alert "alert ClickHouseDown clears after the shard returns" ClickHouseDown no 240

# ---- 9. rolling restart under load ----
# The loader behaves like a collector: it inserts through the cluster Service and retries a batch
# until ClickHouse acknowledges it. The guarantee under test: every pod restarts once and no batch
# is lost; retries are allowed and counted. Retried batches may land twice; they share keys, so
# the count is checked on unique keys (duplicates collapse on merge and LIMIT 1 BY hides them).
note "9. rolling restart under load: collector-style loader through the Service loses nothing"
M="rolling_$RUN"
WORK="$(mktemp -d)"; STOP="$WORK/stop"
start_client_pod
trap 'stop_client_pod; restore_all' EXIT
(
  b=0
  while [[ ! -e "$STOP" ]]; do
    b=$((b+1)); g="$([[ $((b%2)) -eq 0 ]] && echo "$G0" || echo "$G1")"; tries=0
    until out="$(service_insert "$g" "$M" 200 "$b" 2>&1)"; do
      tries=$((tries+1))
      echo "$(date +%s) batch=$b try=$tries $(grep -oE 'Code: [0-9]+|[A-Z_]{8,}|error: [a-z ]+|connection [a-z ]+' <<<"$out" | sort -u | tr '\n' ' ')" >>"$WORK/retries"
      if (( tries >= 120 )); then echo "$b" >>"$WORK/lost"; continue 2; fi
      sleep 0.5
    done
    echo "$b" >>"$WORK/acked"
    sleep 0.5
  done
) &
LOADER=$!
for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 0 $((REPLICAS-1))); do
  echo "$(date +%s) restart $(pod "$s" "$r")" >>"$WORK/timeline"
  kubectl -n "$NS" rollout restart "sts/$(sts "$s" "$r")" >/dev/null
  kubectl -n "$NS" rollout status "sts/$(sts "$s" "$r")" --timeout=300s >/dev/null
  wait_pod_ready "$(pod "$s" "$r")" 300s
done; done
sleep 5; touch "$STOP"; wait "$LOADER"
wait_writable 300; sync_all
acked=$(wc -l <"$WORK/acked" 2>/dev/null || echo 0)
lost=$(cat "$WORK/lost" 2>/dev/null | wc -l)
retries=$(cat "$WORK/retries" 2>/dev/null | wc -l)
retried_batches=$(awk '{print $2}' "$WORK/retries" 2>/dev/null | sort -u | wc -l)
note "load: $acked batches acknowledged, $retried_batches needed a retry ($retries retries in all), $lost gave up"
if [[ -s "$WORK/retries" ]]; then
  note "retry reasons: $(cut -d' ' -f4- "$WORK/retries" | sort | uniq -c | sort -rn | head -5 | awk '{$1=$1};1' | tr '\n' ';')"
  # Which restart each retry fell into: the last restart started before it.
  note "retries per restart: $(awk 'NR==FNR{t[NR]=$1; p[NR]=$3; n=NR; next} {w="before"; for(i=1;i<=n;i++) if($1>=t[i]) w=p[i]; c[w]++} END{for(k in c) printf "%s=%d ", k, c[k]}' "$WORK/timeline" "$WORK/retries")"
fi
# Unique keys per acknowledged batch, in one query: every acknowledged batch must have all 200.
qa "SELECT streamer_id, uniqExact(ts) FROM telemetry WHERE metric_name='$M' GROUP BY streamer_id FORMAT TSV" >"$WORK/counts"
complete=0
for b in $(cat "$WORK/acked"); do
  [[ "$(awk -v b="$b" '$1==b{print $2}' "$WORK/counts")" == "200" ]] && complete=$((complete+1))
done
check "no batch was lost (the loader never gave up)" "0" "$lost"
check "every acknowledged batch has all 200 rows" "$acked" "$complete"
check "the loader kept writing through all four restarts" "1" "$([[ $acked -gt 40 ]] && echo 1 || echo 0)"
dups="$(qa "SELECT count() - uniqExact(streamer_id, ts) FROM telemetry WHERE metric_name='$M'")"
note "rows written twice by retries (collapse on merge): $dups"
stop_client_pod
rm -rf "$WORK"

summary
