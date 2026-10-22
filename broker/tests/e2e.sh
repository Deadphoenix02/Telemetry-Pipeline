#!/usr/bin/env bash
# End-to-end loss check on one machine: ClickHouse in Docker (the dev config and
# users from clickhouse/config, schema from the chart's migrations), then the
# broker, two streamers and two collectors as local processes. Midway the
# broker and one collector are killed with SIGKILL and replaced. At the end,
# the distinct rows in ClickHouse must equal the records the streamers were
# told are durable, and the broker's WAL must have been deleted as it was consumed.
#
#   bash broker/tests/e2e.sh                 RATE=2000 RUN_SECONDS=40 bash broker/tests/e2e.sh
#
# Needs Docker and free ports 9000/8123 (ClickHouse uses the host network so the
# dev users, which only accept 127.0.0.1, can log in), 17070/18080 (broker),
# 19090-19092 (collectors) and 19100-19101 (streamers).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="${WORK:-$(mktemp -d -t e2e-XXXXXX)}"
CH_NAME="${CH_NAME:-ch-e2e}"
CH_IMAGE="${CH_IMAGE:-altinity/clickhouse-server:25.8.16.10002.altinitystable}"
RATE="${RATE:-1000}"                 # rows/s per streamer
RUN_SECONDS="${RUN_SECONDS:-24}"     # streaming time, split in thirds around the two kills
GRPC=127.0.0.1:17070
HTTP=127.0.0.1:18080
CH_PW=collector-dev-pw               # clickhouse/config/dev-users.xml
CLIENT=(docker exec -i "$CH_NAME" clickhouse-client)

pass=0; fail=0
ok()    { echo "PASS  $1"; pass=$((pass+1)); }
bad()   { echo "FAIL  $1 ($2)"; fail=$((fail+1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1" "expected [$2] got [$3]"; fi; }
note()  { echo "----  $*"; }

declare -A PID
cleanup() {
  for k in "${!PID[@]}"; do kill -9 "${PID[$k]}" 2>/dev/null; done
  docker rm -f "$CH_NAME" >/dev/null 2>&1
  echo "logs and data kept in $WORK"
}
trap cleanup EXIT

port_free() { ! ss -ltnH "sport = :$1" | grep -q .; }
for p in 9000 8123 17070 18080 19090 19091 19092 19100 19101; do
  port_free "$p" || { echo "port $p is in use (stop the ch-telemetry dev container or other services first)" >&2; exit 2; }
done

note "building binaries"
make -s -C "$ROOT" build >/dev/null || { echo "build failed" >&2; exit 2; }
BROKER="$ROOT/broker/bin/broker"; STREAMER="$ROOT/telemetry-streamer/bin/streamer"; COLLECTOR="$ROOT/collector/bin/collector"
CSV="$ROOT/telemetry-streamer/internal/resource/dcgm.csv"

note "starting ClickHouse ($CH_NAME) and applying the schema"
docker run -d --rm --name "$CH_NAME" --network host -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$ROOT/clickhouse/config/dev-config.xml:/etc/clickhouse-server/config.d/dev-config.xml:ro" \
  -v "$ROOT/clickhouse/config/dev-users.xml:/etc/clickhouse-server/users.d/dev-users.xml:ro" \
  "$CH_IMAGE" >/dev/null || { echo "docker run failed" >&2; exit 2; }
for _ in $(seq 1 60); do "${CLIENT[@]}" --query "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
CLICKHOUSE_CLIENT="${CLIENT[*]}" MIGRATIONS_DIR="$ROOT/helm/clickhouse/files/migrations/single" \
  TTL_INTERVAL="4 HOUR" PARTITION=hour bash "$ROOT/helm/clickhouse/files/migrate.sh" >"$WORK/migrate.log" 2>&1 ||
  { echo "migration failed, see $WORK/migrate.log" >&2; exit 2; }

start_broker() {
  "$BROKER" -data_dir "$WORK/broker" -listen "$GRPC" -http_addr "$HTTP" >>"$WORK/broker.log" 2>&1 &
  PID[broker]=$!
  for _ in $(seq 1 50); do curl -sf "http://$HTTP/readyz" >/dev/null && return 0; sleep 0.2; done
  echo "broker did not become ready" >&2; exit 2
}
start_collector() { # name http-port
  "$COLLECTOR" -broker "$GRPC" -client_id "$1" -ch_addr 127.0.0.1:9000 -ch_password "$CH_PW" \
    -http_addr "127.0.0.1:$2" -batch_rows 2000 >>"$WORK/$1.log" 2>&1 &
  PID[$1]=$!
}
start_streamer() { # id
  "$STREAMER" -csv_path "$CSV" -target "$GRPC" -streamer_id "$1" -rate_rows_per_sec "$RATE" \
    -metrics_addr "127.0.0.1:1910$1" -stats_interval 0 >>"$WORK/streamer$1.log" 2>&1 &
  PID[streamer$1]=$!
}
metric() { curl -s "http://$1/metrics" | awk -v m="$2" '$1==m {print $2}'; }
lag()    { curl -s "http://$HTTP/lag" | sed -n 's/.*"lag":\([0-9]*\),"partitions".*/\1/p'; }

note "starting the broker, 2 collectors and 2 streamers at $RATE rows/s each"
start_broker
start_collector c1 19090; start_collector c2 19091
start_streamer 0; start_streamer 1
third=$((RUN_SECONDS / 3))
sleep "$third"

note "SIGKILL the broker and restart it on the same data"
kill -9 "${PID[broker]}"; wait "${PID[broker]}" 2>/dev/null
sleep 1
start_broker
grep -q "producer table rebuilt from the log\" producers=2" "$WORK/broker.log" &&
  ok "restart rebuilt the dedupe table for both producers" || bad "producer table rebuild" "see broker.log"
sleep "$third"

note "SIGKILL collector c1 and start c3"
kill -9 "${PID[c1]}"; wait "${PID[c1]}" 2>/dev/null; unset 'PID[c1]'
start_collector c3 19092
sleep "$third"

note "stopping the streamers (they flush and wait for their acks)"
kill -TERM "${PID[streamer0]}" "${PID[streamer1]}"
wait "${PID[streamer0]}" "${PID[streamer1]}" 2>/dev/null
unset 'PID[streamer0]' 'PID[streamer1]'
acked=0; unacked=0
for i in 0 1; do
  line="$(grep "streamer stopped" "$WORK/streamer$i.log" | tail -1)"
  a="$(sed -n 's/.*records_acked=\([0-9]*\).*/\1/p' <<<"$line")"
  u="$(sed -n 's/.*unacked_records=\([0-9]*\).*/\1/p' <<<"$line")"
  acked=$((acked + ${a:-0})); unacked=$((unacked + ${u:-0}))
done
note "streamers report $acked records acked, $unacked unacked"
[[ "$acked" -gt 0 ]] && ok "streamers produced through the broker" || bad "streamers produced" "acked=$acked"

note "waiting for the collectors to drain the queue"
for _ in $(seq 1 90); do [[ "$(lag)" == "0" ]] && break; sleep 1; done
check "group lag drains to zero" "0" "$(lag)"

distinct="$("${CLIENT[@]}" --query "SELECT count() FROM (SELECT DISTINCT gpu_uuid, ts, metric_name, streamer_id FROM telemetry)")"
total="$("${CLIENT[@]}" --query "SELECT count() FROM telemetry")"
note "ClickHouse holds $total rows, $distinct distinct by the deduplication key ($((total - distinct)) replays)"
if [[ "$unacked" == "0" ]]; then
  check "every acked record is in ClickHouse exactly once by key (no loss)" "$acked" "$distinct"
else
  (( distinct >= acked && distinct <= acked + unacked )) && ok "no loss (distinct within acked..acked+unacked)" ||
    bad "loss check" "acked=$acked unacked=$unacked distinct=$distinct"
fi
gpus="$("${CLIENT[@]}" --query "SELECT count() FROM gpus FINAL")"
[[ "$gpus" -gt 0 ]] && ok "GPU registry populated ($gpus GPUs)" || bad "GPU registry" "0 rows"

sleep 2 # one truncation pass after the last commit
used="$(metric "$HTTP" broker_disk_used_bytes)"
note "broker WAL on disk after draining: ${used:-?} bytes"
(( ${used%.*} < 2 * 1024 * 1024 )) && ok "consumed WAL was deleted (under 2 MiB left)" || bad "WAL deletion" "${used} bytes"
check "no sequence gaps" "0" "$(metric "$HTTP" broker_sequence_gaps_total)"

note "graceful stop: collectors drain and leave, then the broker"
for c in c2 c3; do kill -TERM "${PID[$c]}"; done
rc=0; for c in c2 c3; do wait "${PID[$c]}" || rc=$?; unset "PID[$c]"; done
check "collectors exit cleanly on SIGTERM" "0" "$rc"
kill -TERM "${PID[broker]}"; wait "${PID[broker]}"; rc=$?; unset 'PID[broker]'
check "broker exits cleanly on SIGTERM" "0" "$rc"

echo
echo "e2e: $pass passed, $fail failed"
[[ "$fail" == "0" ]]
