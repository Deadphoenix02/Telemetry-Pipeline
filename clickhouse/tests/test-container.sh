#!/usr/bin/env bash
# A throwaway ClickHouse in Docker for Go tests that need a real database (the gateway's store,
# the collector's sink): the image the chart pins, the dev config and users, and the chart's own
# migrations.
#   up    start it (replacing a leftover), migrate, and print host:port on stdout
#   down  remove it
# make coverage and api-gateway/tests/clickhouse-test.sh use it.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
CH_IMAGE="${CH_IMAGE:-altinity/clickhouse-server:25.8.16.10002.altinitystable}"
NAME="${NAME:-rdb-go-test-ch}"
PORT="${PORT:-19000}"
OVERRIDE_DIR="${TMPDIR:-/tmp}/$NAME-users"

case "${1:-}" in
up)
  # dev-users.xml admits users from localhost only; the tests connect through the port
  # mapping, so this container (bound to 127.0.0.1) also admits the Docker bridge.
  mkdir -p "$OVERRIDE_DIR"
  cat >"$OVERRIDE_DIR/zz-test-networks.xml" <<'EOF'
<clickhouse>
  <users>
    <default><networks replace="replace"><ip>::/0</ip></networks></default>
    <api><networks replace="replace"><ip>::/0</ip></networks></api>
    <collector><networks replace="replace"><ip>::/0</ip></networks></collector>
  </users>
</clickhouse>
EOF
  chmod 644 "$OVERRIDE_DIR/zz-test-networks.xml"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --rm --name "$NAME" -p "127.0.0.1:$PORT:9000" -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v "$ROOT/clickhouse/config/dev-config.xml:/etc/clickhouse-server/config.d/dev-config.xml:ro" \
    -v "$ROOT/clickhouse/config/dev-users.xml:/etc/clickhouse-server/users.d/dev-users.xml:ro" \
    -v "$OVERRIDE_DIR/zz-test-networks.xml:/etc/clickhouse-server/users.d/zz-test-networks.xml:ro" \
    "$CH_IMAGE" >/dev/null
  for i in $(seq 1 60); do
    docker exec "$NAME" clickhouse-client --query "SELECT 1" </dev/null >/dev/null 2>&1 && break
    if [[ $i -eq 60 ]]; then echo "ClickHouse did not start" >&2; docker logs "$NAME" 2>&1 | tail -20 >&2; exit 1; fi
    sleep 1
  done
  CLICKHOUSE_CLIENT="docker exec -i $NAME clickhouse-client" \
    MIGRATIONS_DIR="$ROOT/helm/clickhouse/files/migrations/single" \
    bash "$ROOT/helm/clickhouse/files/migrate.sh" </dev/null >/dev/null
  echo "127.0.0.1:$PORT"
  ;;
down)
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$OVERRIDE_DIR"
  ;;
*)
  echo "usage: $0 up|down" >&2
  exit 2
  ;;
esac
