-- 0001 (replicated): per-shard replicated table plus a Distributed table.
-- Applications always use the name `telemetry`; `telemetry_local` is internal.
-- Rows are sharded by cityHash64(gpu_uuid), so one GPU lives on one shard and
-- ReplacingMergeTree deduplication works. The shard and replica macros are defined by the
-- operator on every host. migrate.sh substitutes CLUSTER, PARTITION_EXPR and TTL_INTERVAL.
CREATE TABLE IF NOT EXISTS telemetry_local ON CLUSTER ${CLUSTER}
(
    gpu_uuid    UUID,
    ts          DateTime64(9) CODEC(DoubleDelta, ZSTD),
    metric_name LowCardinality(String),
    streamer_id UInt16,
    value       Float64 CODEC(Gorilla, ZSTD)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/telemetry_local', '{replica}')
PARTITION BY ${PARTITION_EXPR}
ORDER BY (gpu_uuid, ts, metric_name, streamer_id)
TTL toDateTime(ts) + INTERVAL ${TTL_INTERVAL}
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS telemetry ON CLUSTER ${CLUSTER}
AS telemetry_local
ENGINE = Distributed(${CLUSTER}, currentDatabase(), telemetry_local, cityHash64(gpu_uuid));
