# Decision Record: GPU Telemetry Storage

## TL;DR

We chose **ClickHouse**, deployed on Kubernetes with an operator and a 3-node ClickHouse Keeper, to store GPU telemetry at roughly 500k rows/sec.

- **Workload:** append-only, time-ordered, write-heavy metrics. Every read targets one GPU, optionally over a start/end time range.
- **SQL vs NoSQL** was the wrong axis. The data shape (time series, known schema) mattered more, and it pointed to an LSM-style, time-partitioned store rather than a general-purpose B-tree database.
- **Sharding:** hash by GPU, and partition the queue by the same key. Reads then hit one shard and writes spread evenly. Time-based sharding was rejected because it sends all current writes to one node. Not final.
- **Shortlist:** ClickHouse, VictoriaMetrics, ScyllaDB/Cassandra, Postgres + TimescaleDB. ClickHouse won on compression (about 10x less storage than Scylla), native aggregation, and sort-key reads.
- **Operator:** the Altinity operator, chosen for production maturity. ClickHouse Inc.'s newer operator was the alternative.
- **Keeper:** 3 nodes across zones, on their own pods and fast disks. If quorum is lost, inserts stop and the queue absorbs the backlog.
- **Open items:** see [the backlog](BACKLOG.md).

## As built

What the build settled, where it differs from the reasoning below:

| Topic | Proposed below | Built | Why |
| --- | --- | --- | --- |
| Sort key | `(gpu_uuid, metric_name, ts)` | `(gpu_uuid, ts, metric_name, streamer_id)` | The API returns all of a GPU's metrics ordered by time, so `ts` right after the GPU makes that read one contiguous scan; `streamer_id` completes the dedup key |
| Duplicates from redelivery | Insert-block deduplication | `ReplacingMergeTree` on that key, and the API hides unmerged copies with `LIMIT 1 BY` | Replays arrive in different blocks (rebalances, broker failover), which block dedup does not catch |
| Insert path | Collectors insert into each shard's local table, 50k-200k rows per insert | Into the Distributed table `telemetry` with `insert_distributed_sync`, 5,000-row blocks by default | A successful insert then means the rows are stored on their shard, which is what makes committing offsets after it safe. The load test reached 494,000 rows/s through the Service |
| GPU listing | A dimension table with hostname and model | `gpus(gpu_uuid, last_seen)`, cached for 5 s by the API | The API lists GPU ids; host and model are not required |
| Read path | The API hashes the GPU id and queries one shard | Queries go to the Distributed table; `optimize_skip_unused_shards` sends each to the GPU's shard | Verified: every gateway query runs under `force_optimize_skip_unused_shards=1` |
| Topology (dev) | 2-3 shards × 2 replicas | 2 shards × 2 replicas, 3 Keepers, a 4-hour TTL | Production-shaped on one laptop |
| Partitioning | By day | By hour in dev (by day for TTLs in days) | Dev's 4-hour TTL drops whole hourly parts |

## Context and use case

We need a database for GPU telemetry produced by DCGM exporters. A message queue feeds collector microservices, and the collectors write to the database. Nothing is bulk-loaded from the CSV: the CSV was only an example of how structured the data is.

One sample record, with the columns that carry information:

```csv
timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,value
2025-07-18T20:42:34Z,DCGM_FI_DEV_GPU_UTIL,0,nvidia0,GPU-5fd4f087-86f3-7a43-b711-4771313afc50,NVIDIA H100 80GB HBM3,mtv5-dgx1-hgpu-031,0
```

The export also has empty `container`, `pod` and `namespace` columns, and a `labels_raw` column that repeats the other fields. Each row is one sample: a GPU, a metric, a timestamp and a value.

The system is read through three APIs:

| Endpoint | Purpose | Data needed |
| --- | --- | --- |
| `GET /api/v1/gpus` | List GPUs | Small dimension data, not the telemetry table |
| `GET /api/v1/gpus/{id}/telemetry` | Telemetry for one GPU | Latest or full history for one GPU |
| `GET /api/v1/gpus/{id}/telemetry?start_time=...&end_time=...` | Telemetry for one GPU in a time window | Contiguous range for one GPU |

## Requirements and constraints

- **Write-heavy:** writes dominate reads at all times. Target is at least 500k rows/sec for a production-grade system.
- **Read patterns:** by ID, a single record, or a start/end time range. Every API read is scoped to one GPU.
- **Availability over consistency:** brief staleness on reads is acceptable, but ingestion should keep working through node and zone failures.
- **Queue-fed ingestion:** collectors consume from a message queue and write in batches. Delivery is at-least-once, so writes must tolerate duplicates.
- **Runs on Kubernetes:** the database must scale there, ideally via an operator.

The 500k rows/sec target needs checking. It should equal GPUs × metrics per GPU ÷ scrape interval. At about 40 metrics per GPU and a 15-second scrape, that is roughly 190,000 GPUs. At a 1-second scrape it is far fewer. This ratio decides whether we need one shard or several, so it is an open item below.

## SQL vs NoSQL

The first question was SQL or NoSQL. We concluded that the split matters less than the data shape: append-only, time-ordered rows with a known schema, written constantly and read by key and time range.

- **Structured data leans SQL.** The schema is fixed, so a relational model fits. For schema drift, a `JSONB` column in Postgres is enough without switching engines.
- **Write-heavy, time-ordered data leans toward time-partitioned storage.** The first default was Postgres with TimescaleDB, run on Kubernetes through CloudNativePG.
- **Ingestion practices matter more than the engine.** Batched writes, idempotent inserts keyed on an event ID, and acking the queue only after commit remove most apparent write bottlenecks.
- **A single Postgres primary has a ceiling.** It is roughly tens to a few hundred thousand narrow rows/sec with batching and fast disks, and failover is a window where writes stop. With writes dominating and availability preferred, that pushed us to look at engines that scale writes horizontally.

## LSM-style storage vs B-tree

For sustained heavy writes, we wanted an engine that turns writes into sequential appends instead of random in-place updates.

|  | B-tree engines (Postgres, MySQL) | LSM-style engines (Cassandra, ScyllaDB, ClickHouse) |
| --- | --- | --- |
| Write path | Update pages and indexes in place, plus WAL | Append to an in-memory structure and a log, flush as immutable files |
| Write cost | Rises with each index; random I/O | Sequential I/O; background merges do the sorting |
| Reads | Fast point lookups and range scans | Rely on sort order, sparse indexes and merging of files |
| Old data | `DELETE` leaves dead rows | Whole files or partitions expire together via TTL |

Practical consequences for this workload:

- In Postgres, a large B-tree on the timestamp gets expensive under heavy writes. BRIN or time-partitioned indexes soften this, but the primary stays the single write point.
- Cassandra and Scylla use a classic LSM design (memtable, commit log, SSTables, compaction). TimeWindowCompactionStrategy plus TTL expires whole windows without tombstone storms.
- ClickHouse's MergeTree is LSM-inspired rather than a textbook LSM tree. Each insert writes an immutable part, and background merges combine parts in sort-key order. That gives the same sequential-write benefit.
- The cost of LSM-style engines is background merge or compaction work. Monitor the merge backlog, because disk throughput is usually the first bottleneck.

## Sharding strategy

Sharding trades write distribution against read locality, and for time-range data the two pull in opposite directions.

| Shard key | Writes | Range read |
| --- | --- | --- |
| Hash of record ID | Spread evenly | Scatter-gather across every shard, merged by the coordinator |
| Time bucket | Hotspot: all current writes hit the newest shard | Hits 1-2 shards |
| Hash of GPU ID | Spread evenly, since each GPU is a trickle | Single shard, because every query carries a GPU ID |
| Time directory plus K-way hash in the current bucket | Spread over K shards | Touches the K shards of the buckets in range |

How we got to the choice:

1. **Pure hash sharding** scales writes but forces fan-out on every range query. Small queries waste round trips, and p99 latency becomes the slowest shard's.
2. **Pure time sharding** gives cheap reads, but one node takes 100% of ingest. If it can absorb that, sharding is unnecessary. If it cannot, time routing does not help. It is still useful for retention and tiering of old data.
3. **A time directory with a small hash inside the current bucket** works, but we would own the directory, bucket rollover and compaction of closed buckets. It was held in reserve.
4. **Hash by GPU ID** won once we confirmed that every read is scoped to one GPU. The API hashes the ID, routes to one shard and runs the range query there.

The queue partition key should be the same as the shard key, so each collector group writes to exactly one shard with no cross-shard traffic and batches stay large.

**Hotspots.** A time hotspot (the newest data is always hot) is harmless and even good, because the active part stays in memory. The real risk is a key hotspot, where one ID produces a disproportionate share of writes. GPU telemetry is evenly spread, so this is low. If monitoring shows a skewed shard, a composite key such as `hash(ID, time bucket)` can be applied to hot IDs only, through a routing override.

Use logical shards (for example 256 mapped onto a few physical nodes), so adding capacity moves metadata instead of re-hashing everything.

## Aggregates and range queries at scale

Range reads are the main API pattern, and dashboards will soon ask for aggregates such as average utilization per 5 minutes over 7 days.

- **Range reads** become one contiguous scan when the table is sorted by the GPU then time: built as `(gpu_uuid, ts, metric_name, streamer_id)`. The API's query `WHERE gpu_uuid = ? AND ts BETWEEN ? AND ?` uses the primary key prefix and reads very few granules. (The first proposal put `metric_name` before `ts`, which suits per-metric reads; the API reads every metric of a GPU ordered by time.)
- **Single-shard reads.** Because the shard key is the GPU, each API query reads one shard: the Distributed table's `optimize_skip_unused_shards` routes it there, and there is no merge step.
- **Aggregates** run natively in a columnar store. Cassandra and Scylla cannot do this, and would need a second system such as ClickHouse or Spark for analytics.
- **Compression helps reads too.** Metric values barely change between samples, so delta and Gorilla codecs shrink rows to a few bytes and cut read I/O.
- **Listing GPUs** should not scan the telemetry table. `GET /api/v1/gpus` is served from a small registry table, `gpus(gpu_uuid, last_seen)`, which the collectors keep current, cached for 5 s by the API.

Example aggregate query:

```sql
SELECT toStartOfInterval(ts, INTERVAL 5 MINUTE) AS bucket,
       avg(value) AS avg_util
FROM telemetry
WHERE gpu_uuid = {gpu:String}
  AND metric_name = 'DCGM_FI_DEV_GPU_UTIL'
  AND ts BETWEEN {start:DateTime} AND {end:DateTime}
GROUP BY bucket
ORDER BY bucket;
```

## Candidates compared

Storage figures are approximate estimates at 500k rows/sec (about 43 billion rows/day) and should be re-measured on real data.

| Option | Write scaling | Range reads by GPU | Aggregation | Approx. storage per day | Main drawback |
| --- | --- | --- | --- | --- | --- |
| Postgres + TimescaleDB | Single primary; vertical scaling, Citus to shard | Excellent with time-partitioned chunks | SQL, good | Not estimated | Primary is the write ceiling; failover pauses writes |
| ScyllaDB / Cassandra | Add nodes; strongest AP behavior | Good per partition; needs bucket plus shard fan-out or per-GPU partitions | None built in | 2-4 TB (RF=3) | About 10x the storage; no aggregation; heavy repair and compaction ops |
| VictoriaMetrics cluster | Handles this rate with excellent compression | Good, through its query API | PromQL, not SQL | Not estimated | Less flexible for SQL and joins; you put a REST API in front of PromQL |
| ClickHouse | Sharded inserts; one node can ingest this rate with good batching | Excellent: sorted by GPU, metric, time | Native SQL | 0.2-0.4 TB (RF=2) | Needs Keeper; async replication means brief read staleness |

Why the shortlist narrowed to ClickHouse or VictoriaMetrics: the data comes from `dcgm_exporter` through Prometheus, so a purpose-built metrics store is a natural fit. VictoriaMetrics is less work to run, but it commits us to PromQL.

Why ClickHouse over the rest:

1. **Cost.** The compression gap against Scylla is about an order of magnitude, and retention will drive cost more than write throughput.
2. **Query model.** Every read filters on one GPU, which is exactly the shape of a `(gpu, metric, ts)` sort key.
3. **SQL and aggregation.** Dashboards, joins with the GPU dimension table and ad-hoc analysis work without a second system.
4. **Availability.** Replication is asynchronous by default, so writes do not wait on a quorum. This fits the availability-over-consistency goal.

Where the others still win: Scylla is the stricter AP choice if inserts must survive a full coordination-layer failure. Timescale is simpler if the real rate turns out to be modest, at the cost of a failover window.

## Decision: ClickHouse

We store telemetry in a replicated ClickHouse table, sharded by GPU, with collectors inserting straight into their own shard.

```sql
CREATE TABLE telemetry_local ON CLUSTER '{cluster}' (
  gpu_uuid    String,
  metric_name LowCardinality(String),
  ts          DateTime CODEC(DoubleDelta, ZSTD),
  value       Float64  CODEC(Gorilla, ZSTD)
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (gpu_uuid, metric_name, ts)
TTL ts + INTERVAL 30 DAY;
```

The 30-day TTL is a placeholder until retention is confirmed.

**Schema choices**

- Drop `labels_raw` and the empty `container`, `pod` and `namespace` columns. `labels_raw` repeats every other field and would be the largest column.
- Keep a small GPU registry for `GET /api/v1/gpus`. As built it holds `gpu_uuid` and `last_seen`; host and model are not stored, since the API does not need them.
- Partition by day so retention drops whole partitions.

**Write path**

1. Producers partition the queue by `gpu_uuid`.
2. Collectors batch rows per insert: large batches at production rates (50k-200k rows), 5,000 by default in dev. Many tiny inserts is the main way to get this wrong. `async_insert=1` is the fallback if batches cannot be made large.
3. As built, each collector inserts into the Distributed table `telemetry` with `insert_distributed_sync=1`, which forwards each row to the shard chosen by `cityHash64(gpu_uuid)` and returns once it is stored there. (Inserting into each shard's local table would skip that hop, at the cost of client-side routing.)
4. The queue is acked only after the insert succeeds. Replays are collapsed by `ReplacingMergeTree` on `(gpu_uuid, ts, metric_name, streamer_id)`; until a merge, the API hides them with `LIMIT 1 BY`. (Insert-block deduplication was the alternative, but it only catches identical blocks, and replays after a rebalance or a broker failover arrive in different ones.)
5. KEDA scales collectors on queue lag, with a capped maximum so the database is not overloaded.

**Read path.** The API queries the Distributed table with the GPU id pinned; with `optimize_skip_unused_shards` (set in the API user's profile), each query touches only that GPU's shard. Verified on the dev cluster by running the gateway's queries under `force_optimize_skip_unused_shards=1`.

**Topology.** Start with 2-3 shards of 2 replicas each. One node can ingest 500k rows/sec with good batching, so shards are for storage volume, read parallelism and availability.

## Kubernetes: operator and Keeper

### Why an operator

Databases are stateful, so hand-rolling StatefulSets, config rollouts, upgrades and scaling is error-prone. An operator is a controller that runs continuously and reconciles the cluster toward a desired state declared in a custom resource. It is not just an installer.

It handles creation (StatefulSets, Services, ConfigMaps, PVCs), config and user changes, rolling version upgrades, scaling with schema propagation to new replicas, and Prometheus metrics. If the operator pod is down, the running cluster keeps serving reads and writes, because the operator is a control plane and not part of the data path.

### Which operator

There are two open-source operators, both Apache 2.0, and their manifests are not interchangeable.

|  | Altinity Kubernetes Operator | ClickHouse Inc. operator |
| --- | --- | --- |
| Maturity | First released in early 2020; widely used in production | Newer, shorter track record |
| Resources | `ClickHouseInstallation` and `ClickHouseKeeperInstallation` | Different CRDs (from memory, to be verified) |
| Direction | Community effort sponsored by Altinity | Described as the strategic upstream choice |

**Current lean: Altinity**, because a 500k rows/sec production system benefits from the longer track record and more documented failure modes. The ClickHouse Inc. operator is reasonable if we are comfortable with newer tooling. Switching later means rewriting manifests and migrating carefully, so this should be settled before building on top. Open item below.

### Why Keeper, and how to run it

ClickHouse Keeper is the coordination service behind `Replicated*` tables. It is ClickHouse's own implementation of the ZooKeeper protocol, written in C++ and based on Raft. It tracks which data parts exist, coordinates merges, stores insert-deduplication hashes, and queues `ON CLUSTER` DDL. Row data never passes through it.

We chose it over Apache ZooKeeper because it is the recommended replacement and keeps the stack within one project.

- **Run 3 nodes** (tolerates one failure) or 5 (tolerates two), spread across zones. Never 2, since losing one loses quorum.
- **Separate pods on fast disks.** Keeper fsyncs its Raft log, so it should not compete with ClickHouse merges for I/O.
- **Failure mode:** if quorum is lost, replicated tables go read-only. Queries still work, inserts fail, and the queue absorbs the backlog until Keeper recovers. This is the main availability caveat of the design.
- The Altinity operator can deploy Keeper through its Keeper resource, or Keeper can run as its own StatefulSet.

Sources consulted (search results, not fully read): [Altinity operator README](https://github.com/lesandie/clickhouse-operator), [Running ClickHouse on Kubernetes](https://pulse.support/kb/clickhouse-kubernetes), [Altinity webinar](https://altinity.com/?p=30799).

## Risks and trade-offs

- **Keeper quorum loss stops inserts.** Mitigation: 3 nodes across 3 zones, dedicated pods, and queue backpressure so no data is lost while inserts are down. If inserts must survive even that, Scylla remains the stricter AP choice.
- **"Too many parts".** Many small inserts overwhelm merges. Mitigation: large batches, `async_insert` as a fallback, and an alert on parts per partition and merge backlog.
- **Eventual consistency.** Async replication means a read right after a write may miss rows. Acceptable for telemetry.
- **No automatic rebalancing.** Adding a shard only affects new inserts. Plan shard counts with headroom, and treat moving old data as a manual job.
- **Key skew.** One very busy GPU would load its shard. Low risk here. Monitor per-shard write rate.
- **Duplicates.** Queue redelivery is handled by `ReplacingMergeTree`. Duplicates can exist until a merge; the API hides them with `LIMIT 1 BY`.
- **Backups** are not covered by either operator. Built: a `clickhouse-backup` sidecar with scheduled incremental and full backups to S3 (MinIO in dev), enabled in the production values.