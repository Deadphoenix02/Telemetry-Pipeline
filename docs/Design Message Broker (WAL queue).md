# Design: Message Broker (WAL queue)

## TL;DR

The broker is one Go process. It partitions incoming telemetry by GPU, writes it to a 10-partition write-ahead log, serves consumers over gRPC, and runs the consumer-group coordinator. A producer is acknowledged only after its records are fsynced. Once the consumers have acknowledged (committed) data, it is deleted.

- **Broker-side partitioning.** Streamers keep their simple per-record gRPC stream. The broker hashes each record's GPU UUID to a partition, so a GPU's data stays in order.
- **Off-the-shelf WAL.** Each partition is a `github.com/tidwall/wal` log. The broker adds group-commit fsync, crash repair, a disk quota and truncation.
- **Durable acks.** Group commit fsyncs every 20 ms, and the producer ack follows the fsync. Consumers only ever see fsynced data.
- **At-least-once, deduplicated twice.**
  - The broker drops producer retries by `(streamer_id, epoch, partition, seq)`.
  - Collectors commit only after their ClickHouse insert.
  - The table key collapses whatever is replayed.
- **Delete after ack.** A collector's offset commit is the consumer ack. Within about a second, the consumed part of the log is deleted.
- **Several independent brokers, for write availability.** Dev runs three. Each is a whole queue (its own log, partitions and coordinator); streamers fail over between them, and collectors consume from all of them. Nothing is replicated: this keeps ingest available, not every byte durable. See [Several independent brokers](#several-independent-brokers).
- **Sized for a demo machine.** Each broker uses 128 MiB of memory. The chart's defaults are a 512 MiB PVC and a 256 MB quota; the dev values use 6 GiB and 4 GiB. Replication is described, not built.

**In scope:** one topic (`telemetry`) with a fixed frame format; the existing `Ingest` producer stream; `Fetch`; a coordinator for one consumer group with durable offsets; `/lag` for KEDA; and metrics.

**Non-goals:** multiple topics or schemas, transactions, replication or consensus, authentication or TLS in the dev profile, and exactly-once delivery.

## Context and requirements

The assignment forbids existing queues, so the queue is built here. It decouples streamers from collectors, absorbs bursts while ClickHouse is slow, and is the replay point that makes at-least-once delivery possible.

| ID | Requirement | Origin |
| --- | --- | --- |
| B1 | A custom queue delivered as a service with a client library | Assignment |
| B2 | A write-ahead log with explicit producer and consumer acknowledgements | Decided |
| B3 | gRPC between components. HTTP only for KEDA, Prometheus and probes | Decided |
| B4 | One topic, one fixed format, one consumer group | Decided |
| B5 | 10 partitions by hash of the GPU UUID, computed in the broker. Ordering holds per GPU only | Decided |
| B6 | At-least-once delivery with idempotent producers | Decided |
| B7 | Collectors scale from 1 to 10 without losing data | Assignment |
| B8 | A lag signal KEDA can read | Decided |
| B9 | Consumed data is deleted promptly; unconsumed data is never deleted | Decided |
| B10 | Fits a small demo cluster (see Resource budget) | Decided |

There are no throughput targets for the demo; settings are defaults, not goals. Measured on a laptop with two streamers at 1,000 rows/s each, the producer ack latency averaged about 12 ms. With the queue drained, the WAL held 0 bytes.

## Architecture

```
                         gRPC Ingest.Stream (Record → / ← Ack)          gRPC Fetch, Coordinator
streamer ×N (StatefulSet) ───────────────────────────► broker (1 pod) ◄──────────────────────────► collector ×1..10 ──► ClickHouse
                                                         │  ▲
                                                 HTTP /lag  └── KEDA metrics-api scales the collectors
```

Inside the broker:

- **Ingest handler**, one per producer stream:
  - checks sequence contiguity and drops retries;
  - hashes each record to a partition and packs records into one frame per partition (written every 10 ms, or at 64 KB);
  - sends a cumulative `Ack` once everything up to that sequence is fsynced.
- **Partition logs.** One `tidwall/wal` log per partition, in `/data/wal/<p>`. It runs with `NoSync`, 1 MB segments and a segment cache of 1, and allows deleting every entry.
- **Flusher.** Every 20 ms it fsyncs each partition written since the last pass. It then advances that partition's high watermark (HW) and wakes the ack loops and waiting fetches.
- **Fetch handler.** A push stream per partition that never sends past the HW.
- **Coordinator.** Group membership, eager rebalancing with generation fencing, and committed offsets in `/data/offsets.json`.
- **Truncator.** Once a second, it deletes each partition's entries below the committed offset.
- **HTTP**: `/healthz`, `/readyz`, `/metrics`, `/lag`.

**Topic and partitions.** One topic, `telemetry`, with 10 partitions.
- A record's partition is `fmix32(FNV-1a-32(uuid bytes)) % 10`.
- A hash is needed because the UUID itself is a poor number to divide by 10. Every 256^k with k ≥ 1 leaves remainder 6 mod 10, so `uuid % 10` reduces to (sum of bytes mod 5, parity of the last byte). Whenever the last byte is fixed, half the partitions are never used. Trying `uuid % 10` showed exactly that: the test UUIDs fill only the even partitions, so a second collector got nothing to do. On the 247 real UUIDs it happened to spread well, but that's sampling luck, not a property.
- The murmur3 finalizer matters too. Plain FNV-1a leaves the low bit equal to the XOR of the input bytes' low bits, the same kind of flaw. The finalizer mixes every input bit into every output bit.
- The partition count, format version and hash name are written to `topic.json` on first start. The broker refuses to start if they differ later, because remapping GPUs would break per-GPU ordering.

**Offsets.** An offset is a WAL entry index: one entry is one frame, and the first entry is 1. Each frame header carries `record_base`, the partition-wide index of its first record, so lag can be reported in records.

## Frame format (v1)

One WAL entry. It is little-endian and built entirely by the broker. The CRC covers every byte after the CRC field, so a frame is self-checking on disk and on the wire.

| Offset | Field | Size | Notes |
| --- | --- | --- | --- |
| 0 | magic | 1 | `0xDB` |
| 1 | version | 1 | `1` |
| 2 | crc32c | 4 | Castagnoli, over bytes 6.. |
| 6 | record_base | 8 | Partition-wide index of the first record |
| 14 | record_count | 4 | |
| 18 | partition | 2 | Sanity check on read |
| 20 | streamer_id | 2 | uint16 everywhere (wire, frame, ClickHouse `UInt16`) |
| 22 | epoch | 4 | Random per streamer process |
| 26 | first_seq | 8 | Producer sequence of the first record |
| 34 | last_seq | 8 | Producer sequence of the last record |
| 42 | records | | Each record is `ts_ns i64 · uuid [16] · name_len u8 · name · value f64`, about 58 bytes |

Only the GPU UUID, metric name, value and the streamer's emission timestamp are kept. Hostname, model and the other CSV labels are dropped, because no endpoint reads them.

## Storage: WAL, durability, recovery and deletion

**Why `tidwall/wal`.** It is a small, well-used segmented log with exactly the operations needed: append by index, read by index, `Sync`, and `TruncateFront` for deleting consumed entries. A spike test (`internal/plog/spike_test.go`) pins the behaviour the broker relies on:

| Library behaviour | How the broker handles it |
| --- | --- |
| A torn final entry makes `Open` fail with `ErrCorrupt` | `repairTail` cuts the last segment after its last complete entry, then reopens |
| Zero-filled bytes after power loss parse as valid empty entries | After opening, trailing entries that fail the frame CRC are removed with `TruncateBack` |
| The active segment, plus `SegmentCacheSize` cached segments, is held in memory | 1 MB segments and a cache of 1: about 2-3 MB per partition |
| `TruncateFront` rewrites the rest of the first segment | Truncation is batched to once a second per partition, so at most one 1 MB rewrite each |
| Without `AllowEmpty`, the last entry can never be deleted | `AllowEmpty` is on; the next index survives a restart |
| `Read` mutates the segment cache while holding only a read lock | All calls into a log go through one mutex per partition |
| `Sync` fsyncs only the active segment | The library syncs a segment when it rolls over. The flusher also fsyncs the directory when a segment file was created |

**Write and ack path.**
1. The handler appends a sealed frame; this assigns `record_base` and the CRC.
2. The flusher fsyncs and advances the HW.
3. The handler acks the longest prefix of its pending records whose partitions have all reached the needed HW.

A duplicate's ack waits for the HW to cover the partition's log end at the moment it was dropped, because the original may not be fsynced yet.

**Recovery.** For each partition:
1. Repair a torn tail.
2. Drop invalid trailing entries.
3. Treat the rest as durable (HW = last).
4. Rebuild `record_base` from the last entry.

The producer dedupe table is rebuilt from the headers of the last 1,024 entries of each partition. Nothing that was acknowledged is lost, because acks only follow an fsync.

**Deletion after ack.** A commit is acknowledged only after `offsets.json` is fsynced. The coordinator then asks the truncator to delete entries below the offset, never past the HW. Steady-state disk use is therefore the uncommitted backlog plus at most one segment per partition. There is no minimum retention age.

**Quota and backpressure.** At the quota (`quota_mb`: 256 MB by default, 4 GiB in dev), ingest handlers stop reading their streams. HTTP/2 flow control stalls the streamer's stream, its acks stop, and after its ack timeout (2 s) the streamer fails over to another broker. Only when every broker is at its quota (or down) does the streamer's pacer block; nothing is dropped. Writing resumes below 90 % of the quota. At demo rates (about 120 KB/s per broker), 256 MB holds about 35 minutes of backlog if ClickHouse is down.

**Corruption in a sealed segment.** If an entry can't be read, `Fetch` reports `skip_to`, the start of the next segment. The collector counts it and commits past it (`poison_policy=skip`), or stops (`stop`). Nothing is ever marked permanently unhealthy, so one bad segment cannot stall a partition.

## Several independent brokers

A broker pod is a complete queue: its own write-ahead log on its own volume, its own 10 partitions, and its own consumer-group coordinator with its own committed offsets. The StatefulSet runs 1 to 5 of them (3 in dev); nothing is shared or copied between them.

- **Producers.** Each streamer streams to one broker at a time, its home broker (`streamer_id mod brokers`), and fails over to the next one when a stream breaks or acks stop. It never sends on two brokers at once: a resend landing on a broker that already holds a later sequence from it would be dropped as a duplicate (`seq < next[...]` below). See the [streamer design](Component%20Design%20Streamer.md#failover-between-brokers).
- **Consumers.** Every collector joins every broker's consumer group, with one consumer per broker, so data is drained from wherever it landed. KEDA reads each broker's `/lag`.
- **Addressing.** Clients address each broker by its pod name through the headless Service (`broker-N.broker-headless:7070`), which publishes not-ready pods too, so a recovering broker refuses connections at once instead of timing out.
- **What it buys.** Losing one broker (a crash, a restart, a rolling update, a drained node) does not stop ingest: its streamers move to the others within milliseconds, without pausing. What it held but had not handed to collectors waits on its volume and is consumed when it returns.
- **What it does not buy.** A record is on one broker only. Losing a broker's *volume* loses the records on it that were acked but not yet consumed: normally seconds of data, since consumed data is deleted within about a second. Replication (below) closes that gap.
- **Ordering and duplicates.** A GPU's records can sit on several brokers, so per-GPU order across brokers is not kept; storage and the API order by time. A record fsynced on one broker whose ack was lost is resent to another and stored twice; ClickHouse's key collapses it.
- **Why not partition ownership** (partition `p` on broker `p mod N`)? It needs forwarding or client-side routing, and one broker down still stops its partitions. Independent brokers need no routing, and keep every partition writable while any broker is up.

## Producer protocol

Producers use the streamer's existing contract, `telemetry.v1.Ingest/Stream` (`telemetry-streamer/proto/ingest.proto`). It is a bidirectional stream with one `Record` per message and a cumulative `Ack{acked_up_to_seq}`. Two metadata headers identify the producer:
- `x-streamer-id`: the pod ordinal, 0..65535.
- `x-epoch`: random per process start.

| Situation | Broker behaviour |
| --- | --- |
| `seq` is not the previous `seq + 1` on a stream | The stream is closed with `InvalidArgument` (`broker_sequence_gaps_total`) |
| `seq < next[(streamer, epoch)][partition]` | Duplicate: dropped, and acked once the original is fsynced |
| The same producer opens a second stream | The old stream is cancelled (`Aborted`), and the new one waits until it has exited |
| Bad UUID, or a metric name over 255 bytes | Dropped and counted, but acked: resending cannot help |
| Disk quota reached | The handler stops reading (flow control); the streamer fails over after its ack timeout |
| Broker shutting down | Buffered records are written and acked, then the stream closes with `Unavailable` |

Dedupe state is kept per partition, not per producer. A resent record always hashes to the same partition, so if a crash cut one partition's tail, the resend restores exactly the lost records. A single per-producer "highest sequence" would have dropped them. A test covers this case.

## Consumer API and group

```proto
service Broker      { rpc Fetch(FetchRequest) returns (stream FetchResponse); }
service Coordinator { JoinGroup, Heartbeat, LeaveGroup, CommitOffset, FetchOffsets }
```

The full schema is `broker/proto/queue.proto`. The client library is `broker/pkg/consumer`.

**Fetch** is a push stream from an offset:
- Each response holds whole entries up to `max_bytes` (256 KB), plus the HW and first offset.
- When the consumer is caught up, the broker waits up to `max_wait` (500 ms) and then sends an empty response with the HW.
- An offset below the first entry or past HW + 1 fails with `OUT_OF_RANGE`.

**Group states:** `Empty → PreparingRebalance → Stable`.

1. `JoinGroup` blocks until the next generation forms. That happens once the join window (1 s after the last join) has passed and either every known member has rejoined or the 15 s rebalance timeout has expired; members that haven't rejoined by then are dropped. The rebalance timeout is longer than the session timeout, so a member busy with a 10 s insert can still rejoin.
2. Partitions are assigned by sorting members by ID; partition `p` goes to `members[p % n]`.
3. Members heartbeat every 3 s, with a 10 s session timeout. A heartbeat during a rebalance returns `REBALANCE_IN_PROGRESS`. The member then finishes its insert, commits and rejoins. Members blocked in `JoinGroup` don't expire.
4. **The generation advances only when the assignment is computed.** Commits from the current generation are therefore still accepted while the rebalance is being prepared. If the generation advanced when the rebalance started, that final commit would always be rejected.
5. `CommitOffset` carries every partition of the member. It is rejected for an unknown member, a stale generation, a partition the member doesn't own, or an offset above HW + 1.
6. `FetchOffsets` returns the committed offset, or the first entry for a partition that has never been committed.
7. Membership is in memory. After a broker restart, members get `UNKNOWN_MEMBER` and rejoin as new members; the offsets are durable.

**Lag (`GET /lag`).** For each partition, lag is the records appended minus the `record_base` at the committed offset. KEDA's `metrics-api` scaler reads the `lag` field:

```json
{"group":"collectors","lag":5628,"partitions":[{"partition":0,"lag":657,"committed_offset":1,"high_watermark":327}]}
```

## Operations

**Configuration.** Every flag is also `BROKER_<NAME>`, set from Helm values in `helm/broker`.

| Setting | Default | Notes |
| --- | --- | --- |
| `partitions` | 10 | Fixed once `/data` holds data |
| `segment_kb` | 1024 | The active segment of each partition is in memory |
| `flush_interval` | 20ms | Group commit; producer acks wait for it |
| `truncate_interval` | 1s | Deletion of committed entries |
| `quota_mb` | 256 | Producers pause here |
| `linger`, `max_frame_kb` | 10ms, 64 | Frame batching per producer stream |
| `fetch_max_kb`, `fetch_max_wait` | 256, 500ms | Fetch defaults |
| `session_timeout`, `heartbeat_interval` | 10s, 3s | Coordinator |
| `join_window`, `rebalance_timeout` | 1s, 15s | Coordinator |
| `drain_timeout` | 10s | Shutdown: time for producer streams to get their final acks |

**Resource budget (demo).** ClickHouse (2×2 replicas plus 3 Keepers, about 4.75 GiB at its limits) is budgeted separately; the whole stack needs about an 8 GiB minikube.

| Component | Replicas | Memory request / limit | Disk |
| --- | --- | --- | --- |
| broker | 1..5 (3 in dev) | 64 / 128 MiB each (`GOMEMLIMIT` 100 MiB) | A PVC each: 512 MiB by default, 6 GiB in dev |
| streamer | 2 by default, up to 10 | 32 / 64 MiB | |
| collector | 1..10 via KEDA | 32 / 64 MiB | |
| KEDA | operator, metrics server, webhooks | trimmed by `make deploy SERVICE=keda` | |

**Deployment.** The broker is a StatefulSet of 1 to 5 independent replicas (3 in dev), each with its own PVC at `/data`, soft anti-affinity across nodes, and a PodDisruptionBudget of one broker at a time.
- Readiness turns true only after recovery; liveness only checks that the process answers.
- On SIGTERM: readiness drops, fetches end, producer streams write and ack what they hold, gRPC stops, offsets are written, and the final fsync runs. The pod's grace period is the drain timeout plus 20 s.
- NetworkPolicy:
  - gRPC from pods in the namespace;
  - HTTP from the namespace, `keda` and `monitoring`;
  - egress for DNS only.

**Metrics** (`/metrics`):

| Area | Series |
| --- | --- |
| Produce | `broker_records_{accepted,duplicate,invalid}_total`, `broker_sequence_gaps_total`, `broker_streams_{opened,fenced}_total`, `broker_ack_latency_seconds`, `broker_quota_blocked_seconds_total` |
| Log | `broker_frames_appended_total`, `broker_fsync_duration_seconds`, `broker_truncations_total`, `broker_recovery_truncated_bytes_total`, `broker_corrupt_ranges_total`, `broker_disk_used_bytes`, `broker_partition_{high_watermark,first_offset,bytes}` |
| Group | `broker_group_lag_records{partition}`, `broker_group_members`, `broker_group_generation`, `broker_rebalances_total`, `broker_commits_{accepted,rejected}_total` |

**Failure modes**

| Failure | Effect | Recovery |
| --- | --- | --- |
| One broker crashes (SIGKILL) or restarts | Its streams break | Its streamers fail over to another broker at once and resend their unacked records there; ingest does not pause. The restart repairs the tail and rebuilds the dedupe table; collectors rejoin its group from the committed offsets and drain what it held. Streamers return home |
| A broker's node or PVC is lost | That broker's log is gone; there is one copy | Its unconsumed records (seconds of data) are lost; the other brokers keep ingesting. Replication would close this gap |
| Every broker down | No broker to write to | Streamers back off and pause until one returns |
| ClickHouse down | No commits, so lag grows and the WALs fill | At its quota a broker stops reading and streamers move to another; when all are full, streamers pause. When ClickHouse returns, collectors drain the backlogs and the WALs shrink |
| Collector crash | Its partitions move after the session timeout (10 s) | The new owner re-reads from the committed offset; the table key removes replays |
| Zombie collector | Late commits after a rebalance | Rejected by generation fencing |
| Torn or zero-filled tail | Detected at start | Cut and counted. Never acknowledged, so nothing a producer was promised is lost |
| Corrupt sealed segment | Unreadable entries | `skip_to` the next segment, counted on both sides |

## Testing

- **Unit and integration tests** run with `make test` (race detector). `make coverage` reports 85 % for the broker, excluding generated code; the broker's `run` is tested end to end (produce, consume, recover the same data directory).
  - Log: visibility up to the HW; a cut at every byte of the last entry; zero-filled tail; truncation; the quota blocking and resuming; topic mismatch; skipping a corrupt sealed segment.
  - Ingest: ack only after fsync; resend after reconnect; a gap closes the stream; fencing; invalid records; a crash that cut one partition, repaired by resend; drain.
  - Coordinator: join and assignment; a commit during `PreparingRebalance`; fencing (stale generation, wrong partition, past HW); session expiry; leave; rebalance timeout; durable offsets and deletion; lag.
  - Consumer library: consume everything with the log deleted behind the commits; partitions handed over on join and leave without loss.
  - Fuzz test on the frame decoder: `go test ./pkg/frame -fuzz FuzzDecode` (in `broker/`).
- **End to end** with `bash broker/tests/e2e.sh`: ClickHouse in Docker, the broker, 2 streamers and 2 collectors. The broker and one collector are killed with SIGKILL midway. The check is that the distinct rows in ClickHouse equal the records the streamers were told are durable, and that the WAL is deleted once drained. Last run: 47,463 acked, 47,463 distinct, WAL 0 bytes after draining.

## Alternatives considered

| Option | Why not |
| --- | --- |
| Client-side partitioning (the first draft) | Needs a producer library in every streamer, and the broker could not validate placement. Broker-side keeps the streamer's simple, already-tested stream |
| Partition ownership across brokers (`p mod N`) | Needs forwarding or client routing, and a broker down stops its partitions. Independent brokers need neither |
| One broker | A restart or crash pauses ingest for seconds. Independent brokers keep ingest running |
| Hand-written segment log (the first draft) | More code and recovery risk. `tidwall/wal` provides segments, indexes and truncation; the broker adds what it lacks (repair, group commit, quota) |
| `hashicorp/raft-wal` | Truncates whole segments without rewriting, but brings in the raft and bbolt dependencies |
| An external database behind the queue | Hides the work the assignment asks for, and adds a system to operate |
| A minimum retention age for consumed data | Spends disk on data nobody reads. Inspection can use ClickHouse |
| An offsets append-log plus snapshots | More than 10 numbers need. One file, rewritten atomically by a 50 ms group commit |
| fsync per batch, or no fsync | Too slow, or loses acknowledged data on power loss. Group commit is the compromise |

## Evolution to production

- **Multiple brokers: built** as independent brokers with producer failover (above). That gives write availability.
- **Open items:** replication, coordinator persistence with cooperative rebalancing, and security hardening are in [the backlog](BACKLOG.md).