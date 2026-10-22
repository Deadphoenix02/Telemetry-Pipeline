# Collector Design: Queue to ClickHouse

## TL;DR

The collector is a stateless Deployment that drains the brokers into ClickHouse. The brokers are independent queues (three in dev), so every collector joins every broker's consumer group: for each one it reads the partitions that broker's coordinator assigns, groups rows into blocks, inserts each block, and commits offsets only after the insert succeeds. It does no deduplication of its own, because the table's key does that.

- **Commit after insert.** Anything uncommitted is redelivered after a crash or rebalance, so delivery is at-least-once.
- **Every broker.** One consumer per broker, each with its own session and blocks, all sharing one ClickHouse connection pool. A broker that is down stops only its own consumer, which keeps retrying.
- **One block per flush.** 5,000 rows, one second after the first row, or 8 MB of rows, whichever comes first, across the partitions a session owns.
- **Rebalance-safe.** On a rebalance signal it finishes the in-flight insert, commits if its generation is still valid, then rejoins. Rows for partitions it no longer owns are discarded, and the new owner re-reads them.
- **Clean drain.** On SIGTERM it flushes, inserts, commits, leaves every group and exits within the 25 s shutdown grace (the pod's grace period is 30 s).
- **Scaled by KEDA on lag.** 1 to 10 replicas, never more than the 10 partitions of a broker. One lag trigger per broker.
- **Bounded memory.** When ClickHouse is slow the fetch loops pause instead of buffering without limit.
- **Skips undecodable batches** after logging and counting them, so one bad batch cannot block a partition.

## Purpose, scope and interfaces

The collector moves records from the broker's log into ClickHouse. It is the only component that writes telemetry, and it runs separately from the API so that ingestion and reads scale and fail independently.

| Owns | Does not own |
| --- | --- |
| Consuming assigned partitions and decoding batches | The log, offset storage and partition assignment (broker) |
| Grouping rows into blocks and inserting them | The table schema and migrations (migration job) |
| Committing offsets after a successful insert | Deduplicating rows (the table key) |
| Recording each GPU in the registry | Serving reads (API gateway) |
| Draining cleanly on shutdown | Autoscaling decisions (KEDA) |

| Interface | Peer | Purpose |
| --- | --- | --- |
| gRPC `Coordinator.JoinGroup`, `Heartbeat`, `LeaveGroup` | Broker | Membership and partition assignment |
| gRPC `Broker.Fetch` | Broker | Read batches from the committed offset |
| gRPC `Coordinator.CommitOffset` | Broker | Acknowledge consumed offsets |
| ClickHouse native protocol | ClickHouse | Insert into `telemetry` and `gpus` with an insert-only user |
| HTTP `/healthz`, `/readyz`, `/metrics` | kubelet, Prometheus | Probes and metrics |

**Requirements it must meet**

- At-least-once consumption, with every insert safe to repeat.
- Scale from 1 to 10 replicas without losing data, with graceful drain on scale-down.
- No unbounded memory, even when ClickHouse is slow or down.
- Stateless: all progress lives in the broker's committed offsets.

## The consume, batch, insert and commit loop

**Structure.** One coordinator goroutine joins the group and heartbeats. One fetch loop runs per assigned partition and feeds decoded rows into a shared buffer. One inserter flushes the buffer. A commit tracker remembers, per partition, the next offset to commit.

**Steps**

1. Join the group, receive the generation and the assigned partitions, and ask the broker for the committed offset of each.
2. Fetch from each partition with `max_bytes` 1 MB and `max_wait` 500 ms. The broker returns only fsynced data.
3. Decode each batch into rows: GPU UUID, timestamp, metric name, value and the `streamer_id` from the batch header. The broker has already verified the CRC.
4. Append the rows to the buffer and record the highest offset seen for each partition.
5. Flush when the block holds 5,000 rows, one second after its first row, or when its rows take 8 MB. A flush is one ClickHouse insert, into the Distributed table `telemetry` (with `insert_distributed_sync`, so a successful insert means the rows are stored on their shard), holding rows from every partition the session owns.
6. If the insert succeeds, commit the next offset for each partition in the block, tagged with the current generation.
7. If the insert fails, retry with exponential backoff from 100 ms to 5 s and do not commit. Fetching pauses while the buffer is full.

**Why one block across partitions.** It keeps the insert rate near one per second per collector, so ten collectors send about 10 inserts a second, far from the point where ClickHouse accumulates too many small parts. The cost is that one failed insert holds up every partition the collector owns.

**Insert details.** The collector writes with the ClickHouse native protocol (a Go client such as clickhouse-go, a proposal) as a single batch with an insert timeout of 10 seconds. A block that spans an hour boundary lands in two parts, because the table is partitioned by hour, and each part is atomic on its own. If an insert fails after one part landed, the retry resends the whole block and the rows already stored are duplicates, which the table key removes.

**No deduplication here.** A replayed row carries the same `(gpu_uuid, ts, metric_name, streamer_id)`, so ReplacingMergeTree collapses it and the API hides unmerged copies. The collector keeps no record of what it has inserted, which is what keeps it stateless.

**GPU registry.** The collector keeps an in-memory set of UUIDs it has seen. A new UUID is written to the `gpus` table with the next block, and `last_seen` is refreshed about once a minute for UUIDs still active.

**Poison batches.** A batch that cannot be decoded is logged with its partition and offset range, counted, and skipped with the offset committed, so one bad batch cannot block a partition forever. This is configurable. The WAL's CRC already guarantees that what decodes is what was written.

**Memory.** The buffer is capped (16 MB in dev, a proposal). At the cap the fetch loops pause, lag grows, and the broker's own backpressure eventually slows the streamers.

## Rebalance, drain and scaling

**Rebalance.** The heartbeat response signals a generation change. The collector finishes any in-flight insert, commits if its generation is still valid, then rejoins and receives a new assignment. Buffered rows for partitions it no longer owns are discarded, and the new owner re-reads them from the committed offset. Rows that were already inserted come back as duplicates and are removed by the table key. A commit with a stale generation is rejected by the broker, and the collector treats that as a signal to rejoin.

**Graceful drain on SIGTERM.** In order: stop fetching, flush and insert the buffer, commit, call `LeaveGroup`, close connections, exit. Leaving triggers an immediate rebalance, so the group does not wait for the session timeout. The pod's termination grace period must exceed the flush interval plus the insert timeout, for example 30 seconds.

**Scaling with KEDA.** Collectors are a Deployment scaled by a KEDA `ScaledObject` with one `metrics-api` trigger per broker, each reading that broker's `/lag` (`http://broker-N.broker-headless.<namespace>.svc.cluster.local:8080/lag`). The HPA follows the largest demand among the triggers, so each trigger's target is `lagTarget / brokers` (3,333 with 3 brokers): that matches the total when the lag is spread evenly, and scales up harder when it sits on one broker.

| Setting | Dev | Production | Note |
| --- | --- | --- | --- |
| `minReplicaCount` | 1 | 2 | Two collectors in production so one failure does not stop consumption |
| `maxReplicaCount` | 10 | 10 | Equal to the partition count, since extra collectors would sit idle |
| Lag target per replica | 10,000 records over all brokers | same | Split evenly across the per-broker triggers |
| Polling interval | 15 s | 15 s | |
| Scale-up | up to 2 pods per 60 s | same | HPA behavior block |
| Scale-down | 1 pod per 120 s after a 300 s stabilization window | same | Avoids repeated rebalances |
| Fallback | hold 2 replicas after 3 failed metric reads | same | Safe size if the broker endpoint is unreachable |

In the e2e `scale` case with 3 brokers (and shortened KEDA timings), collectors went from 1 to 10 under 6 × 5,000 rows/s and back to 1 about 2 minutes after the lag reached zero, with no loss.

**Balance across partitions.** With 10 partitions the load is even at 1, 2, 5 and 10 collectors. At other counts the busiest collector carries up to 1.8 times the average, at 9 collectors. We accept that rather than build a custom scaler that rounds to 1, 2, 5 or 10.

**Every scale event pauses consumption.** Rebalancing is eager, so each scale-up or scale-down stops the group for a few seconds. The scale policies and cooldowns limit how often that happens.

**Overload.** If ClickHouse is the bottleneck, adding collectors adds contention without adding throughput. Lag grows, the broker fills toward its quota, and backpressure slows the streamers. The maximum replica count bounds the contention, and an alert on insert latency shows the real cause.

**Cost of KEDA on minikube.** The operator and its metrics adapter add roughly 200-300 MB of memory (an estimate). A `keda.enabled` flag in the Helm chart lets the collectors fall back to manual scaling if that is too heavy.

## Decisions, alternatives and trade-offs

| Decision | Alternatives considered | Why | Cost accepted |
| --- | --- | --- | --- |
| Collector is a separate service from the API | One combined service | They scale on different signals (lag versus request rate), fail independently, shut down differently and use separate credentials | One more deployable |
| Commit offsets after the insert | Commit on fetch, auto-commit, per-record commits | Nothing is lost on the path | A crash between insert and commit replays rows |
| One block across partitions, at least 10,000 rows or 1 second | One insert per partition, ClickHouse async inserts | Few large inserts keep the part count low | One failed insert stalls every partition the collector owns |
| No deduplication in the collector | An in-memory or persisted cache of inserted rows | The collector stays stateless and replicas are interchangeable, and the table key already collapses replays | Duplicates are visible until a merge, and the API hides them |
| Skip and count undecodable batches | Stop the partition, or a dead-letter partition | A stuck partition would stall lag and scaling | A bad batch is lost, though it is counted and logged |
| Discard buffered rows on rebalance | Hand buffers to the new owner | No state transfer, and the new owner re-reads from the committed offset | Repeated reads and brief duplicates |
| KEDA on lag, 1 to 10 replicas (minimum 2 in production) | Fixed replicas, CPU autoscaling, a custom operator | Lag measures backlog directly | Coarse reaction time, and imbalance at some counts |
| Insert-only ClickHouse user | Shared read-write credentials | Least privilege, so the collector cannot read broadly and the API cannot write | One more credential to manage |

**What changes in production**

- ClickHouse is sharded by a hash of the GPU UUID, so the collector either inserts through a Distributed table or routes each row to its shard with the same hash. The database record settles which.
- A minimum of two collectors, anti-affinity across nodes and a PodDisruptionBudget keep consumption running during node maintenance.
- Credentials come from Secrets, and traffic to the broker and ClickHouse uses mTLS.

## Failure modes, observability and configuration

| Event | Effect | Recovery |
| --- | --- | --- |
| ClickHouse slow | Inserts take longer, the buffer fills, fetch loops pause and lag grows | Backpressure reaches the streamers; alert on insert latency |
| ClickHouse down | Inserts fail, nothing is committed, lag grows and KEDA adds replicas that cannot help | Retry with backoff and resume when it returns; the maximum replica count bounds the contention; alert on insert errors |
| Insert partly landed (block spans two hourly parts) | One part is stored and the retry resends the whole block | Duplicates collapse on the table key |
| Crash between insert and commit | The new owner re-reads from the committed offset | Duplicates collapse on the table key |
| Crash while fetching | Partitions are reassigned after the 10 s session timeout | Uncommitted rows are redelivered |
| Stale generation on commit | The broker rejects the commit | The collector discards in-flight state and rejoins |
| Broker restart | Fetch and heartbeat fail, or return `UNKNOWN_MEMBER` | Rejoin and resume from committed offsets |
| Undecodable batch | Logged, counted and skipped | Alert on any non-zero count |
| SIGKILL during a drain | Equivalent to a crash | Replay and deduplication |
| Lag endpoint unreachable | KEDA holds the fallback replica count | Alert on scaler errors |

**Metrics**

- Inserts: rows inserted, insert duration, insert errors by type, rows per block.
- Commits: results (ok, stale generation, error).
- Group: assigned partitions, rebalances seen, drain duration.
- Pipeline: buffer bytes, fetch lag per partition, undecodable batches, registry upserts.

Logs are structured JSON through `log/slog`, with partition and offset ranges on every insert and error. Endpoints are `/healthz`, `/readyz` (joined the group and ClickHouse reachable) and `/metrics`.

**Configuration** (as built; every flag is also `COLLECTOR_<NAME>`)

| Setting | Default | Notes |
| --- | --- | --- |
| `brokers` | (the chart renders `broker-N.broker-headless:7070`) | Every broker to consume from; `broker` is a one-address alternative |
| `batch_rows` | 5,000 | Insert a block at this many rows... |
| `batch_wait` | 1 s | ...this long after its first row... |
| `buffer_mb` | 8 | ...or at this much memory; fetching pauses meanwhile |
| `insert_timeout` | 10 s | Per attempt; retries back off from 100 ms to 5 s with jitter |
| `poison_policy` | `skip` | `stop` ends the process instead |
| `registry_refresh` | 60 s | How often a GPU's `last_seen` is rewritten |
| `shutdown_grace` | 25 s | Must exceed `insert_timeout`; the pod's grace period is 5 s more |
| ClickHouse credentials | From a Secret | The insert-only `collector` user; the pool holds one connection per broker plus one |
| Pod memory limit | 64 MiB | `GOMEMLIMIT` 52 MiB |

## Testing, build plan, risks and open questions

**Unit tests**

- Batching: flush at the row count, the wait and the memory cap.
- Commit ordering: with a fake ClickHouse and a fake broker, nothing is committed before a successful insert, and the committed offset is the last row included for each partition.
- Retry: a failed insert retries with backoff and never commits, and a commit follows the eventual success.
- Rebalance: a signal mid-batch discards rows for lost partitions, commits carry the right generation, and a stale-generation rejection leads to a rejoin.
- Drain: on SIGTERM the order is stop fetching, flush, insert, commit, leave.
- Poison batches: skipped and counted with the offset committed, and halted when the policy says `stop`.
- Memory: fetching pauses at the buffer cap and resumes as it drains.
- Registry: a new UUID is written once, and `last_seen` refreshes on schedule.
- Several brokers: one collector consuming from two in-process brokers at once, each with its own session; stopping one broker does not stop the other. The race detector covers the shared state.
- The sink, against a throwaway ClickHouse in Docker: inserts, replays collapsing by the table key, the registry keeping the newest `last_seen`, and the `collector` user unable to read.
- The collector's `run`, end to end against in-process brokers and a fake sink: rows from every broker, per-broker group gauges, a clean drain, and `poison_policy=stop`.
- Coverage is reported by `make coverage` (86 %).

**Open items:** see [the backlog](BACKLOG.md).

**Decided during build**

- The buffer is capped at 8 MB per session, inside a 64 MiB pod limit.
- Offsets are committed after every insert.
- Blocks are 5,000 rows. Raising `batch_rows` (and `buffer_mb`) is the cheapest throughput lever: ClickHouse's own load test reached 494,000 rows/s with blocks of about 37,000 rows.
- Inserts go through the Distributed table with `insert_distributed_sync`; routing by shard key in the client is not needed at this scale.
- With several brokers, each collector consumes from all of them (one consumer per broker), and KEDA has one lag trigger per broker, targeting `lagTarget / brokers` each.
