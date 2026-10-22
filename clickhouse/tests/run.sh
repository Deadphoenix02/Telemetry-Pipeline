#!/usr/bin/env bash
# Integration tests against a running ClickHouse.
# CLICKHOUSE_CLIENT is the client command (default: docker exec into the dev container).
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CLIENT="${CLICKHOUSE_CLIENT:-docker exec -i ch-telemetry clickhouse-client}"
# Passwords match config/dev-users.xml (the docker harness defines the users).
COLLECTOR_PASSWORD="${COLLECTOR_PASSWORD:-collector-dev-pw}"
API_PASSWORD="${API_PASSWORD:-api-dev-pw}"
CHART="$HERE/../../helm/clickhouse"
export CLICKHOUSE_CLIENT="$CLIENT"
export MIGRATIONS_DIR="$CHART/files/migrations/single"
export TTL_INTERVAL="4 HOUR" PARTITION="hour"

# rejects <regex> <cmd...>: prints yes when the command fails AND its output matches the regex.
# The output is captured first: piping into grep under pipefail reports "no" for a command that
# exits non-zero, which is exactly what a correctly denied statement does.
rejects() {
  local re="$1" out rc; shift
  out="$("$@" 2>&1)"; rc=$?
  if [[ $rc -ne 0 ]] && grep -qiE "$re" <<<"$out"; then echo yes; else echo no; fi
}

pass=0; fail=0
ok()   { echo "PASS  $1"; pass=$((pass+1)); }
bad()  { echo "FAIL  $1 ($2)"; fail=$((fail+1)); }
check() { # name expected actual
  if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1" "expected [$2] got [$3]"; fi
}
q()  { $CLIENT --query "$1" </dev/null; }
qa() { $CLIENT --user api --password "$API_PASSWORD" --query "$1" </dev/null; }
qc() { $CLIENT --user collector --password "$COLLECTOR_PASSWORD" --query "$1" </dev/null; }

GPU='11111111-1111-1111-1111-111111111111'

# ---- 1. migrations apply twice, create tables and users ----
bash "$CHART/files/migrate.sh" >/dev/null 2>&1 || bad "migrate first run" "nonzero exit"
second="$(bash "$CHART/files/migrate.sh" 2>&1)"
check "migrate second run is a no-op" "0" "$(echo "$second" | grep -c '^apply')"
check "tables exist" "gpus,telemetry" \
  "$(q "SELECT arrayStringConcat(arraySort(groupArray(name)),',') FROM system.tables WHERE database=currentDatabase() AND name IN ('telemetry','gpus')")"
check "users exist" "api,collector" \
  "$(q "SELECT arrayStringConcat(arraySort(groupArray(name)),',') FROM system.users WHERE name IN ('api','collector')")"
check "TTL placeholder substituted" "1" \
  "$(q "SELECT countSubstrings(create_table_query, 'INTERVAL 4 HOUR') + countSubstrings(create_table_query, 'toIntervalHour(4)') FROM system.tables WHERE database=currentDatabase() AND name='telemetry'")"
check "migrations and retention recorded once each" "3" "$(q "SELECT count() FROM schema_migrations")"
changed="$(TTL_INTERVAL='12 HOUR' bash "$CHART/files/migrate.sh" 2>&1)"
check "a changed TTL is applied" "1" "$(echo "$changed" | grep -c '^apply  retention=12 HOUR')"
restored="$(TTL_INTERVAL='4 HOUR' bash "$CHART/files/migrate.sh" 2>&1)"
check "restoring the TTL is applied again" "1" "$(echo "$restored" | grep -c '^apply  retention=4 HOUR')"

# ---- 2. deduplication ----
DEDUP_BASE="$(q "SELECT toString(toStartOfMinute(now()) - 60)")"
q "TRUNCATE TABLE telemetry"
q "SYSTEM STOP MERGES telemetry"
ins="INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value)
SELECT '$GPU', toDateTime64('$DEDUP_BASE', 9) + number, 'DCGM_FI_DEV_GPU_UTIL', 1, number FROM numbers(10)"
q "$ins"; q "$ins"
check "duplicates visible before merge" "20" "$(q "SELECT count() FROM telemetry")"
check "LIMIT 1 BY hides duplicates" "10" \
  "$(q "SELECT count() FROM (SELECT ts FROM telemetry WHERE gpu_uuid='$GPU' ORDER BY ts, metric_name, streamer_id LIMIT 1 BY gpu_uuid, ts, metric_name, streamer_id)")"
q "SYSTEM START MERGES telemetry"
q "OPTIMIZE TABLE telemetry FINAL"
check "row count equals unique keys after merge" "10" "$(q "SELECT count() FROM telemetry")"

# ---- 3. query shapes: inclusive bounds, ordering, keyset pagination ----
BASE="$(q "SELECT toString(toStartOfMinute(now()) - 600)")"
q "TRUNCATE TABLE telemetry"
q "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value)
   SELECT '$GPU', toDateTime64('$BASE', 9) + intDiv(number, 2), if(number % 2 = 0, 'm_a', 'm_b'), 1, number
   FROM numbers(20)"
start="toDateTime64('$BASE', 9) + 2"; end="toDateTime64('$BASE', 9) + 5"
check "inclusive start and end" "8" \
  "$(q "SELECT count() FROM telemetry WHERE gpu_uuid='$GPU' AND ts >= $start AND ts <= $end")"
check "ordered by ts, metric, streamer" "1" \
  "$(q "SELECT groupArray((ts, metric_name, streamer_id)) AS a, a = arraySort(a) AS sorted FROM (SELECT ts, metric_name, streamer_id FROM telemetry WHERE gpu_uuid='$GPU' ORDER BY ts, metric_name, streamer_id) FORMAT TSV" | cut -f2)"

full="$(q "SELECT concat(toString(ts),'|',metric_name,'|',toString(streamer_id)) FROM telemetry WHERE gpu_uuid='$GPU' ORDER BY ts, metric_name, streamer_id")"
paged=""; c_ts="toDateTime64('1970-01-01 00:00:00', 9)"; c_m="''"; c_s=0; pages=0
while :; do
  page="$(qa "SELECT concat(toString(ts),'|',metric_name,'|',toString(streamer_id)), ts, metric_name, streamer_id FROM telemetry
              WHERE gpu_uuid='$GPU' AND (ts, metric_name, streamer_id) > ($c_ts, $c_m, $c_s)
              ORDER BY ts, metric_name, streamer_id
              LIMIT 1 BY gpu_uuid, ts, metric_name, streamer_id LIMIT 7 FORMAT TSV")"
  [[ -z "$page" ]] && break
  pages=$((pages+1))
  paged+="$(echo "$page" | cut -f1)"$'\n'
  last="$(echo "$page" | tail -1)"
  c_ts="toDateTime64('$(echo "$last" | cut -f2)', 9)"; c_m="'$(echo "$last" | cut -f3)'"; c_s="$(echo "$last" | cut -f4)"
  [[ $pages -gt 10 ]] && break
done
check "keyset pages cover every row once, in order" "$full" "${paged%$'\n'}"
check "keyset used 3 pages for 20 rows at limit 7" "3" "$pages"

# ---- 4. retention drops whole parts ----
q "DROP TABLE IF EXISTS ttl_test"
q "CREATE TABLE ttl_test (ts DateTime64(9), v Float64) ENGINE = MergeTree PARTITION BY toStartOfHour(ts) ORDER BY ts
   TTL toDateTime(ts) + INTERVAL 1 HOUR SETTINGS ttl_only_drop_parts = 1"
q "INSERT INTO ttl_test SELECT now() - INTERVAL 5 HOUR + number, number FROM numbers(100)"
q "INSERT INTO ttl_test SELECT now() + number, number FROM numbers(100)"
q "OPTIMIZE TABLE ttl_test FINAL"
sleep 2
check "expired hourly parts dropped, fresh kept" "100" "$(q "SELECT count() FROM ttl_test")"
q "DROP TABLE ttl_test"

# ---- 5. access control ----
check "api can select telemetry" "20" "$(qa "SELECT count() FROM telemetry")"
check "api cannot insert" "yes" \
  "$(rejects 'ACCESS_DENIED|Not enough privileges' qa "INSERT INTO telemetry (gpu_uuid, ts, metric_name, streamer_id, value) VALUES ('$GPU', now(), 'x', 1, 1)")"
check "collector can insert gpus" "ok" \
  "$(qc "INSERT INTO gpus (gpu_uuid, last_seen) VALUES ('$GPU', now())" 2>&1 && echo ok)"
check "collector cannot read" "yes" \
  "$(rejects 'ACCESS_DENIED|Not enough privileges' qc "SELECT count() FROM telemetry")"
check "api result row cap enforced" "yes" \
  "$(rejects 'TOO_MANY_ROWS' qa "SELECT number FROM numbers(200000)")"

check "api rows-read cap enforced" "yes" \
  "$(rejects 'TOO_MANY_ROWS' qa "SELECT count() FROM numbers(200000000)")"

check "api cannot raise its result-row cap" "yes" \
  "$(rejects 'SETTING_CONSTRAINT_VIOLATION|should not be changed' qa "SELECT 1 SETTINGS max_result_rows = 0")"
check "api cannot raise its execution-time cap" "yes" \
  "$(rejects 'SETTING_CONSTRAINT_VIOLATION|should not be changed' qa "SELECT 1 SETTINGS max_execution_time = 1000")"
check "api cannot create tables" "yes" \
  "$(rejects 'READONLY|ACCESS_DENIED|Not enough privileges|readonly' qa "CREATE TABLE api_probe (x UInt8) ENGINE = Memory")"

# ---- 6. GPU list ----
check "GPU list from registry" "$GPU" "$(qa "SELECT gpu_uuid FROM gpus FINAL ORDER BY gpu_uuid")"

echo; echo "passed=$pass failed=$fail"
[[ $fail -eq 0 ]]
