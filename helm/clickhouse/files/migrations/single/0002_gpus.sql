-- 0002 (single node): GPU registry so GET /gpus never scans telemetry.
CREATE TABLE IF NOT EXISTS gpus
(
    gpu_uuid  UUID,
    last_seen DateTime
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY gpu_uuid
TTL last_seen + INTERVAL ${TTL_INTERVAL};
