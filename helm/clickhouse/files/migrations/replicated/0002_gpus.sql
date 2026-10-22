-- 0002 (replicated): GPU registry, sharded the same way as telemetry.
CREATE TABLE IF NOT EXISTS gpus_local ON CLUSTER ${CLUSTER}
(
    gpu_uuid  UUID,
    last_seen DateTime
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/gpus_local', '{replica}', last_seen)
ORDER BY gpu_uuid
TTL last_seen + INTERVAL ${TTL_INTERVAL};

CREATE TABLE IF NOT EXISTS gpus ON CLUSTER ${CLUSTER}
AS gpus_local
ENGINE = Distributed(${CLUSTER}, currentDatabase(), gpus_local, cityHash64(gpu_uuid));
