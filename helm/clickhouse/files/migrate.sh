#!/usr/bin/env bash
# Applies versioned SQL files from MIGRATIONS_DIR in name order and records each
# in schema_migrations on CH_HOST. Safe to rerun: applied versions are skipped and
# every statement uses IF NOT EXISTS. In Kubernetes it runs as a Helm post-install /
# post-upgrade Job; locally it is used by the Makefile test harness.
set -euo pipefail

CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-${CLICKHOUSE_PASSWORD:-}}"
CH_DATABASE="${CH_DATABASE:-default}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$(dirname "$0")/migrations/single}"
CLIENT="${CLICKHOUSE_CLIENT:-clickhouse-client}"
WAIT_HOSTS="${WAIT_HOSTS:-$CH_HOST}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-600}"
CLUSTER="${CLUSTER:-telemetry}"
TOPOLOGY_MODE="${TOPOLOGY_MODE:-single}"
TTL_INTERVAL="${TTL_INTERVAL:-4 HOUR}"
PARTITION="${PARTITION:-hour}"

# These values are substituted into SQL text, so validate them strictly.
[[ "$CLUSTER" =~ ^[A-Za-z0-9_]+$ ]] || { echo "invalid CLUSTER" >&2; exit 1; }
[[ "$TOPOLOGY_MODE" =~ ^(single|replicated)$ ]] || { echo "invalid TOPOLOGY_MODE (single or replicated)" >&2; exit 1; }
[[ "$TTL_INTERVAL" =~ ^[0-9]+\ (HOUR|DAY)$ ]] || { echo "invalid TTL_INTERVAL (use e.g. '4 HOUR' or '7 DAY')" >&2; exit 1; }
case "$PARTITION" in
  hour) PARTITION_EXPR='toStartOfHour(ts)' ;;
  day)  PARTITION_EXPR='toYYYYMMDD(ts)' ;;
  *) echo "invalid PARTITION (hour or day)" >&2; exit 1 ;;
esac

# CLIENT may be several words (for example "docker exec -i name clickhouse-client").
read -r -a CLIENT_CMD <<<"$CLIENT"

# The password reaches clickhouse-client through CLICKHOUSE_PASSWORD, never as an argument, so it
# does not show up in the process list. (With a docker exec CLIENT the variable stays on the host;
# the local harness uses the passwordless default user.)
if [[ -n "$CH_PASSWORD" ]]; then export CLICKHOUSE_PASSWORD="$CH_PASSWORD"; fi
unset CH_PASSWORD

client() {
  local host="$1"; shift
  "${CLIENT_CMD[@]}" --host "$host" --port "$CH_PORT" --user "$CH_USER" \
    --database "$CH_DATABASE" --distributed_ddl_task_timeout 300 "$@"
}
ch() { client "$CH_HOST" "$@"; }

# Wait until every expected host answers. In a cluster this also means the
# operator has finished rolling the pods.
IFS=',' read -r -a hosts <<<"$WAIT_HOSTS"
deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
for h in "${hosts[@]}"; do
  until client "$h" --query "SELECT 1" </dev/null >/dev/null 2>&1; do
    if (( $(date +%s) >= deadline )); then
      echo "timed out waiting for $h" >&2
      exit 1
    fi
    echo "waiting for $h"
    sleep 3
  done
done

# Answering is not enough in a cluster: the operator adds replicas to the cluster definition
# (remote_servers) one by one, and ON CLUSTER DDL only reaches the hosts listed there at that
# moment. Wait until CH_HOST sees every expected host as a member.
if [[ "$TOPOLOGY_MODE" == "replicated" ]]; then
  expected=${#hosts[@]}
  while :; do
    # An error here is usually config still propagating (the operator applies users and
    # remote_servers after the upgrade), so report it and keep waiting until the deadline.
    if members="$(ch --query "SELECT count() FROM system.clusters WHERE cluster = '$CLUSTER'" </dev/null 2>&1)"; then
      [[ "$members" == "$expected" ]] && break
    else
      members="unknown ($(grep -oE 'Code: [0-9]+|ACCESS_DENIED|[A-Z_]{6,}' <<<"$members" | sort -u | tr '\n' ' '))"
    fi
    if (( $(date +%s) >= deadline )); then
      echo "timed out: cluster $CLUSTER lists $members of $expected hosts" >&2
      exit 1
    fi
    echo "waiting for cluster $CLUSTER to list $expected hosts (now $members)"
    sleep 3
  done
fi

render() {
  local s
  s="$(cat "$1")"
  s="${s//\$\{PARTITION_EXPR\}/$PARTITION_EXPR}"
  s="${s//\$\{TTL_INTERVAL\}/$TTL_INTERVAL}"
  s="${s//\$\{CLUSTER\}/$CLUSTER}"
  printf '%s\n' "$s"
}

ch --query "CREATE TABLE IF NOT EXISTS schema_migrations
  (version String, applied_at DateTime DEFAULT now())
  ENGINE = MergeTree ORDER BY version" </dev/null

shopt -s nullglob
for f in "$MIGRATIONS_DIR"/*.sql; do
  version="$(basename "$f")"
  applied="$(ch --query "SELECT count() FROM schema_migrations WHERE version = '$version'" </dev/null)"
  if [[ "$applied" != "0" ]]; then
    echo "skip   $version"
    continue
  fi
  echo "apply  $version"
  render "$f" | ch --multiquery
  ch --query "INSERT INTO schema_migrations (version) VALUES ('$version')" </dev/null
done

# Every host must have every table. A host can miss them when it joined the cluster after the
# DDL ran (or was rebuilt from an empty volume). Migrations are idempotent (IF NOT EXISTS), so
# the repair is to apply all of them again and check once more.
hosts_complete() {
  ch --query "
    SELECT countIf(n = (SELECT count() FROM system.tables WHERE database = '$CH_DATABASE' AND name != 'schema_migrations'))
    FROM (SELECT hostName() AS h, uniqExact(name) AS n
          FROM clusterAllReplicas('$CLUSTER', system.tables)
          WHERE database = '$CH_DATABASE' AND name != 'schema_migrations' GROUP BY h)" </dev/null
}
if [[ "$TOPOLOGY_MODE" == "replicated" ]]; then
  if [[ "$(hosts_complete)" != "$expected" ]]; then
    echo "repair: $(hosts_complete) of $expected hosts have every table; re-applying migrations"
    for f in "$MIGRATIONS_DIR"/*.sql; do
      echo "reapply $(basename "$f")"
      render "$f" | ch --multiquery
    done
  fi
  complete="$(hosts_complete)"
  if [[ "$complete" != "$expected" ]]; then
    echo "only $complete of $expected hosts have every table" >&2
    exit 1
  fi
  echo "check  all $expected hosts have every table"
fi

# Retention follows the values: a changed TTL is applied with ALTER, recorded as
# "retention=<ttl>" so an unchanged value is skipped on later runs. Tables are only
# created with IF NOT EXISTS above, so without this step a new TTL would be ignored.
# The partition expression cannot be changed on an existing table.
retention="retention=${TTL_INTERVAL}"
current="$(ch --query "SELECT version FROM schema_migrations WHERE version LIKE 'retention=%' ORDER BY applied_at DESC, version DESC LIMIT 1" </dev/null)"
if [[ "$current" == "$retention" ]]; then
  echo "skip   $retention"
else
  echo "apply  $retention"
  if [[ "$TOPOLOGY_MODE" == "replicated" ]]; then
    t_tbl="telemetry_local"; g_tbl="gpus_local"; on_cluster=" ON CLUSTER ${CLUSTER}"
  else
    t_tbl="telemetry"; g_tbl="gpus"; on_cluster=""
  fi
  ch --query "ALTER TABLE ${t_tbl}${on_cluster} MODIFY TTL toDateTime(ts) + INTERVAL ${TTL_INTERVAL}" </dev/null
  ch --query "ALTER TABLE ${g_tbl}${on_cluster} MODIFY TTL last_seen + INTERVAL ${TTL_INTERVAL}" </dev/null
  ch --query "INSERT INTO schema_migrations (version) VALUES ('${retention}')" </dev/null
fi
echo "migrations complete"
