#!/usr/bin/env bash
# Runs the store's integration tests against a throwaway ClickHouse in Docker
# (clickhouse/tests/test-container.sh: the pinned image, dev users, chart migrations).
#   bash api-gateway/tests/clickhouse-test.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
CONTAINER="$ROOT/clickhouse/tests/test-container.sh"

addr="$(bash "$CONTAINER" up)"
trap 'bash "$CONTAINER" down' EXIT

cd "$ROOT/api-gateway"
GATEWAY_TEST_CH_ADDR="$addr" go test -race -count=1 -v -run 'TestClickHouse' ./internal/store/
