#!/usr/bin/env bash
# Backup and restore drill against a deployed replicated release.
#   1. write known data through the Distributed table
#   2. back up the first replica of every shard to object storage (clickhouse-backup sidecar)
#   3. destroy the tables on every replica (cluster-wide DROP ... SYNC)
#   4. restore schema and data on the first replica of each shard, then the schema on the others,
#      which pull the parts through replication
#   5. check the data is back, identical on both replicas, and that new writes work
# The drill mutates the cluster: only run it on a dev or minikube release.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"

load_creds
RUN="drill$(date +%s)"
TABLES="default.telemetry_local,default.gpus_local"
bk() { # bk <shard> <replica> <clickhouse-backup args...>
  local s="$1" r="$2"; shift 2
  kubectl -n "$NS" exec "$(pod "$s" "$r")" -c clickhouse-backup -- clickhouse-backup "$@"
}
# Runs on replica <r> but reads the backup that replica 0 of the same shard uploaded.
bk_from_first() { # bk_from_first <shard> <replica> <args...>
  local s="$1" r="$2"; shift 2
  kubectl -n "$NS" exec "$(pod "$s" "$r")" -c clickhouse-backup -- \
    env "S3_PATH=${BACKUP_PATH:-backup}/$(pod "$s" 0)" clickhouse-backup "$@"
}
total() { qa "SELECT count() FROM telemetry WHERE metric_name LIKE 'drill%'"; }

note "preparing data"
G0="$(gpu_for_shard 0)"; G1="$(gpu_for_shard 1)"
M="${RUN}_data"
insert_rows "$G0" "$M" 1000; insert_rows "$G1" "$M" 700
qc "INSERT INTO gpus (gpu_uuid, last_seen) VALUES ('$G0', now()), ('$G1', now())"
sync_all
before="$(qa "SELECT count() FROM telemetry WHERE metric_name='$M'")"
gpus_before="$(qa "SELECT count() FROM gpus FINAL")"
check "seed data is in place" "1700" "$before"

note "backing up (first replica of each shard)"
for s in $(seq 0 $((SHARDS-1))); do
  check "shard $s: backup created and uploaded" "0" "$(bk "$s" 0 create_remote --tables="$TABLES" "$RUN" >/dev/null 2>&1; echo $?)"
  check "shard $s: backup is listed in object storage" "1" "$(bk "$s" 0 list remote 2>/dev/null | grep -c "$RUN")"
done

note "disaster: dropping the tables on every replica"
admin 0 0 "DROP TABLE telemetry_local ON CLUSTER $CLUSTER SYNC" >/dev/null
admin 0 0 "DROP TABLE gpus_local ON CLUSTER $CLUSTER SYNC" >/dev/null
check "data is gone" "fail" "$(total >/dev/null 2>&1 && echo ok || echo fail)"

note "restoring"
for s in $(seq 0 $((SHARDS-1))); do
  check "shard $s replica 0: schema and data restored" "0" \
    "$(bk "$s" 0 restore_remote --tables="$TABLES" "$RUN" >/dev/null 2>&1; echo $?)"
done
for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 1 $((REPLICAS-1))); do
  check "shard $s replica $r: schema restored" "0" \
    "$(bk_from_first "$s" "$r" restore_remote --schema --tables="$TABLES" "$RUN" >/dev/null 2>&1; echo $?)"
done; done
wait_writable 120; sync_all

note "verifying"
check "row count matches the pre-disaster count" "$before" "$(qa "SELECT count() FROM telemetry WHERE metric_name='$M'")"
check "GPU registry matches" "$gpus_before" "$(qa "SELECT count() FROM gpus FINAL")"
for s in $(seq 0 $((SHARDS-1))); do
  check "shard $s replicas are identical after restore" \
    "$(admin "$s" 0 "SELECT count() FROM telemetry_local WHERE metric_name='$M'")" \
    "$(admin "$s" 1 "SELECT count() FROM telemetry_local WHERE metric_name='$M'")"
done
check "new writes work after restore" "0" "$(insert_rows "$G0" "${RUN}_after" 50 >/dev/null 2>&1; echo $?)"
check "new writes are readable" "50" "$(qa "SELECT count() FROM telemetry WHERE metric_name='${RUN}_after'")"

summary
