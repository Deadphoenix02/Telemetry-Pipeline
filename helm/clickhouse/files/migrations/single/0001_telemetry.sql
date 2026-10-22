-- 0001 (single node): telemetry table.
-- migrate.sh substitutes the placeholders PARTITION_EXPR and TTL_INTERVAL (written as dollar-brace tokens below).
CREATE TABLE IF NOT EXISTS telemetry
(
    gpu_uuid    UUID,
    ts          DateTime64(9) CODEC(DoubleDelta, ZSTD),
    metric_name LowCardinality(String),
    streamer_id UInt16,
    value       Float64 CODEC(Gorilla, ZSTD)
)
ENGINE = ReplacingMergeTree
PARTITION BY ${PARTITION_EXPR}
ORDER BY (gpu_uuid, ts, metric_name, streamer_id)
TTL toDateTime(ts) + INTERVAL ${TTL_INTERVAL}
SETTINGS ttl_only_drop_parts = 1;
