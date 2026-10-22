#!/usr/bin/env bash
# Regenerates the gRPC code: pkg/queuepb from queue.proto, and internal/ingestpb from the
# streamer's ingest.proto (one contract, two modules). Needs protoc, protoc-gen-go and
# protoc-gen-go-grpc on PATH. Run from anywhere: bash broker/proto/gen.sh
set -euo pipefail
cd "$(dirname "$0")/.."
INGEST='Mingest.proto=broker/internal/ingestpb;ingestpb'
protoc -I proto --go_out=. --go_opt=module=broker --go-grpc_out=. --go-grpc_opt=module=broker proto/queue.proto
protoc -I ../telemetry-streamer/proto --go_out=. --go_opt=module=broker --go_opt="$INGEST" \
  --go-grpc_out=. --go-grpc_opt=module=broker --go-grpc_opt="$INGEST" ingest.proto
echo "generated pkg/queuepb and internal/ingestpb"
