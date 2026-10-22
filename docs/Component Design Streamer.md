# Component Design: Streamer

## Summary

The streamer simulates telemetry sources. It replays the DCGM CSV in a loop, stamps each row with the time it is emitted, and sends rows to a broker as individual records over one gRPC stream. Each streamer is an independent replay of the whole file, so scaling streamers up or down scales the load on the system.

- It runs as a StatefulSet of 0 to 10 replicas (Helm value `streamer.replicas`). The pod ordinal is the `streamer_id`.
- It keeps the GPU UUID, metric name and value from each row (plus the other DCGM label columns, which the broker ignores). The CSV's own timestamp is discarded.
- It stamps `timestamp_ns` when the record is created, in UTC nanoseconds. A resend carries the same timestamp.
- The rate is set per instance, 1,000 rows/s by default (1 to 100,000).
- It never drops rows. When no broker can take more, it pauses reading the CSV.
- **It streams to one of several independent brokers at a time and fails over between them. While at least one broker is up, it never pauses.**
- It drains on SIGTERM.

## Scope

| In scope | Out of scope |
| --- | --- |
| Reading and looping the CSV | How the broker stores data |
| Creating and timestamping records | Deduplication in the database |
| Pacing, producing, failover between brokers | Autoscaling streamers (the count is a Helm knob) |
| Backpressure, metrics, shutdown | Real exporters |

## Interfaces

| Peer | Protocol | Use |
| --- | --- | --- |
| Brokers | gRPC `Ingest.Stream`, one bidirectional stream at a time | Records out, cumulative acks back. Each broker is addressed by its own pod name (`broker-N.broker-headless:7070`) |
| CSV file | Local file | Baked into the image at `/data/telemetry.csv` |
| Configuration | Flags, or `STREAMER_<NAME>` environment variables | Set by the Helm chart |
| Kubernetes | HTTP | `/healthz`, `/readyz`, `/metrics` on `:9100` |

The wire format is `proto/ingest.proto`. The client encodes it by hand (`internal/ingest/codec.go`); the broker generates its server from the same file.

## Lifecycle

| State | What the streamer does | Next state |
| --- | --- | --- |
| Starting | Reads config, takes `streamer_id` from the pod name, picks a random epoch, loads the CSV, opens a stream to its home broker | Streaming |
| Streaming | Emits rows at the configured rate and sends them on the open stream | Failing over, or Draining |
| Failing over | The stream broke, or acks stopped. Opens a stream to the next broker without pausing intake, then resends what is unacked | Streaming, or Paused |
| Paused | Every broker failed in the last round. Backs off; the window fills and the pacer waits | Streaming |
| Draining | Entered on SIGTERM. Stops emitting, waits for acks | Stopped |
| Stopped | Logs `streamer stopped … records_acked=N unacked_records=N` and exits | None |

## CSV loading and replay

- **Columns.** The file is the DCGM export with a header line. The streamer reads `metric_name`, `gpu_id`, `device`, `uuid`, `modelName`, `Hostname`, `container`, `pod`, `namespace` and `value`, and ignores the CSV timestamp.
- **Memory or stream mode.** In `auto` mode the rows are held in memory (about 16 bytes per row plus a table of distinct series) unless that exceeds `max_memory_mb` (16 MB); then the file is re-read on every pass. Either way, the file loops forever.
- **Random start.** Each instance starts at a random row (the seed is logged, `-seed` pins it), so ten streamers do not emit the same row at the same instant.
- **Malformed rows** are counted (`streamer_malformed_rows_total`), logged at a limited rate and skipped. They never stop the stream.
- **Timestamp.** `timestamp_ns` is the current UTC time in nanoseconds, taken when the row becomes a record.

## Pacing

A token bucket at the configured rate, with a 10 ms tick (`-tick`): every tick emits the rows accrued since the last one, so the stream is smooth. The burst is capped at about 100 ms of rows, so a pause is not followed by a flood. The rate is a flag (and Helm value), changed by redeploying: `make streamers N=… RATE=…`.

## Producing

- **One record per gRPC message**, on one bidirectional stream. The broker partitions each record by its GPU UUID; the streamer does not partition.
- **Identity.** Every stream carries `x-streamer-id` (the pod ordinal) and `x-epoch` (random per process start). Sequences count records from 0 per epoch. The broker drops a resend it already holds, by `(streamer_id, epoch, partition, seq)`.
- **Acks.** The broker sends a cumulative `Ack{acked_up_to_seq}` once the records are fsynced in its log. Acknowledged records leave the window.
- **The window** holds every record sent but not yet acked, for resending: `window × rate` records (3 s of data), capped at `window_max_records` (60,000) and at least 100.
- **Sending never blocks the worker.** A sender goroutine owns the stream's `Send`, which gRPC flow control can block when a broker stops reading. The worker hands it records only when it has room. Receiving runs in its own goroutine too.
- **The queue between the pacer and the worker holds 200 ms of rows** (at least 256). The worker can be descheduled for tens of milliseconds: under the 200m CPU limit, the resend burst after a failover gets the pod CPU-throttled. With the earlier 256-record queue (about 50 ms at 5,000 rows/s), the e2e suite once measured the pacer blocked for 1.7 ms during a SIGKILL failover, and Prometheus showed that streamer, and not the other, throttled in about ten CPU periods right then. The worker also takes every row waiting before anything else in each loop.

## Failover between brokers

The brokers are independent queues: each has its own log, partitions and consumer group, and nothing is replicated between them ([broker design](Design%20Message%20Broker%20%28WAL%20queue%29.md)). A streamer is connected to one of them at a time, and every collector reads all of them, so a record is safe on whichever broker acked it.

- **Home broker.** `streamer_id mod brokers`, from the `-targets` list. This spreads load evenly by construction.
- **Immediate failover.** On a stream error, the worker tries the next broker in the list at once, with no backoff. Each attempt is bounded by `connect_timeout` (1 s), so an address that does not answer cannot stall the round. Backoff (100 ms to 5 s, with jitter) applies only after a full round in which *every* broker failed. That is the only state in which streaming pauses (`streamer_all_brokers_down`).
- **Intake never stops while connecting.** Stream opening runs in a goroutine. The worker keeps taking rows into the window, so the pacer does not notice a failover unless it outlasts the window.
- **Hangs are noticed in seconds.** If the oldest sent record is unacked for `ack_timeout` (2 s), the stream counts as dead and the worker fails over. Normal acks take 20 to 100 ms (group commit every 20 ms). This also covers a broker at its disk quota, which stops reading its producers: for a streamer, that broker is unavailable for writes. gRPC keepalive (every 10 s, 5 s timeout) is a backstop. The client's reconnect backoff is capped at 2 s instead of gRPC's 120 s, so a broker that comes back is usable within seconds.
- **Measured on the cluster** (`make e2e CASES=broker-crash`, 2 streamers × 5,000 rows/s): a graceful broker delete ends the stream cleanly, and the affected streamer kept acking with no gap beyond the half-second sampling. A force delete (SIGKILL) does not reset the TCP connection, so the ack timeout caught it: 2.1 to 2.4 s without acks. With the queue sized in time (below), the blocked-time counter did not move in either mode over repeated runs, the streamer returned home, and nothing was lost.
- **Measured across a network partition** (`make e2e CASES=broker-partition`, 2 streamers × 5,000 rows/s). Every packet to and from broker-0 was dropped for 20 s while it kept running and stayed Ready. The ack timeout caught it, the longest gap without acks was 3.4 s, and the blocked-time counter did not move. The streamer returned home once broker-0 was reachable, and nothing was lost.
- **The window covers the failover.** The defaults make the window (3 s of data) outlast the slowest failover (ack timeout plus one connect), and the config refuses a window shorter than that. Above about 20,000 rows/s per streamer the 60,000-record cap is reached first, so a *hang* failover can briefly fill the window. A crash failover, which takes milliseconds, is not affected. Measured with `PARTITION_RATE=30000 make e2e CASES=broker-partition`: the cap held 2 s of data, the failover took about 4 s without acks, and the partitioned broker's streamer was blocked for 0.88 s. Nothing was lost. Raising `window_max_records` to rate × 3 s (about 22 MB at 30,000 rows/s) closes the gap; see the [backlog](BACKLOG.md).
- **One active stream, always.** The broker drops any record whose sequence is below one it already holds from that producer on that partition, treating it as a retry. Sending on two brokers at once could therefore make a later resend look like an old retry and lose it. So every switch resends the oldest unacked record first, on exactly one new stream, after the old one is closed. A broker also requires consecutive sequences within a stream, which an in-order resend satisfies.
- **Duplicates across brokers.** A record the old broker fsynced but never acked is resent to the new one, with the same timestamp, and stored twice. ClickHouse's key `(gpu_uuid, ts, metric_name, streamer_id)` collapses the copy, and the API hides it until then.
- **Ordering.** A GPU's records can sit on several brokers. Storage and the API order by time, so this does not matter.
- **Return home.** While away from home, the worker probes its home broker every `home_probe` (10 s). Once it answers, the worker stops sending new records (they collect in the window), waits until everything sent on the current stream is acked (tens of ms), closes it, and sends the window to home in order. That keeps the one-active-stream rule, and costs no resends.

## Backpressure

When no broker accepts writes (all down, or all at their disk quota), the window fills, the worker stops taking rows, the queue in front of it fills, and the pacer blocks: it stops reading the CSV. Nothing is dropped, because the input can be replayed. `streamer_backpressure_blocked_seconds_total` counts the time blocked, including a block still in progress.

## Shutdown

On SIGTERM readiness drops, the streamer stops emitting, keeps failing over if it needs to, and waits up to `-grace` (30 s) for the outstanding acks. It then logs `streamer stopped rows_emitted=N records_acked=N unacked_records=N`, which the e2e loss check reads. A record still unacked then was never confirmed; the pod's grace period is `grace + 10 s`.

## Configuration

Every flag is also `STREAMER_<NAME>`; flags win. The chart sets them from values.

| Flag | Default | Notes |
| --- | --- | --- |
| `csv_path` | (required) | `/data/telemetry.csv` in the image |
| `rate_rows_per_sec` | 1000 | Per instance, 1 to 100,000 |
| `tick` | 10ms | Pacing interval |
| `targets` | (empty = discard) | Broker addresses, comma-separated. The chart renders `broker-N.broker-headless:7070` from `brokers.replicas` |
| `target` | | A single address; same as a one-item `targets` |
| `streamer_id` | pod ordinal | 0 to 65535; required with `targets` outside a StatefulSet |
| `window`, `window_max_records` | 3s, 60000 | Unacked data held for resend. The window must be at least `ack_timeout + connect_timeout` |
| `connect_timeout`, `ack_timeout`, `home_probe` | 1s, 2s, 10s | Failover timings |
| `mode`, `max_memory_mb` | `auto`, 16 | Rows in memory unless over budget |
| `grace` | 30s | Shutdown: wait this long for acks |
| `metrics_addr`, `stats_interval`, `log_level` | `:9100`, 5s, `info` | |

## Failure modes

| Failure | Effect | Behavior |
| --- | --- | --- |
| One broker crashes or restarts | Its streams break | Fail over to the next broker at once, resend the unacked records, return home later. No pause |
| A broker hangs, or reaches its disk quota | Acks stop | Fail over after `ack_timeout` (2 s). No pause while the window lasts |
| A broker address does not answer | The connect attempt hangs | Abandoned after `connect_timeout` (1 s); the next broker is tried |
| Every broker unavailable | No stream | Back off 100 ms to 5 s per round; the window fills and the pacer pauses. Resumes when any broker answers |
| Streamer crash | Unacked records in memory are lost; they were never confirmed | The new process picks a new epoch; the broker treats it as a new producer |
| Malformed CSV row | One row skipped | Counted and logged at a limited rate |
| CSV missing or empty | Nothing to stream | Fails at start with the error logged |
| Scale-down | The highest ordinal gets SIGTERM | It drains and exits |

## Metrics and health

- Emission: `streamer_rows_emitted_total`, `streamer_malformed_rows_total`, `streamer_backpressure_blocked_seconds_total`, `streamer_buffer_fullness`.
- Producing: `streamer_records_sent_total` (includes resends), `streamer_records_acked_total`, `streamer_retries_total`, `streamer_stream_errors_total`, `streamer_send_latency_seconds` (send to ack).
- Failover: `streamer_failovers_total{reason="error|ack_timeout|home"}`, `streamer_connected_broker` (index, -1 while none), `streamer_home_broker`, `streamer_all_brokers_down`.
- Endpoints: `/healthz` (the process is up), `/readyz` (CSV loaded and a stream open to some broker; 503 on SIGTERM), `/metrics`.

## Testing

- **Unit tests** (`go test -race`):
  - The worker, against fake brokers that can be down, hang on connect, stop acking, or be killed mid-stream. They check:
    - a broker dying under load moves the stream to the next broker with the pacer never blocked;
    - unacked records are resent in order on exactly one stream;
    - an ack stall triggers an `ack_timeout` failover, and a hung address costs only the connect timeout;
    - the return home waits for acks, and no two streams ever send at once;
    - backoff happens only after every broker has failed.
  - Plus the pacer, the CSV source, the codec, the config and the metrics.
- **The streamer's `run`**, against two in-process ingest servers: it fails over when its home server stops, reports it in `/metrics`, and flushes on cancellation.
- **The cluster e2e suite** (`make e2e`): `broker-crash` deletes broker-0 under load. It checks that the streamers' blocked time does not move, that they return home, and that nothing acknowledged is lost. `happy` checks that each streamer starts on its home broker.

## Deployment

- A StatefulSet with parallel pod management, so each pod has a stable ordinal for its `streamer_id`, and scaling does not wait for pods one at a time. Replicas 0 to 10.
- No volume: the CSV is baked into the image.
- The broker list comes from the chart's `brokers` block (`replicas`, `statefulSet`, `headlessService`, `port`). `brokers.replicas` must match the broker chart's replicas.
- Memory limit 64 MiB, `GOMEMLIMIT` 52 MiB; the 60,000-record window cap is about 15 MB.
- Termination grace period `grace + 10 s`.

## Trade-offs accepted

- Every streamer replays the full file. That is simple and needs no coordination, but total data scales with the streamer count.
- Pausing instead of dropping, when no broker accepts writes, keeps the data complete and only slows the simulation.
- Failing over to independent brokers gives write availability without replication. What a broker held but had not yet handed to collectors waits on its volume until it returns, and is lost only if that volume is lost.
- One record per gRPC message limits throughput per streamer, which is enough at the rates this exercise uses.

## Changed during build

| Proposed | Built | Why |
| --- | --- | --- |
| A `Metadata` RPC, streamer-side partitioning, per-partition batches with a 50 ms linger | One record per message; the broker partitions | Simpler streamers; partitioning in one place |
| A single broker behind a Service | A list of independent brokers, a home broker per streamer, immediate failover | Write availability: streaming continues while any broker is up |
| A window of 0.5 s of data, reconnect with backoff through the Service | A 3 s window, failover with no backoff until every broker has failed, an ack timeout, a connect timeout | So a failover completes before the window fills, and the pacer never pauses while a broker is up |
| A rate file re-read every 5 s, up to 5,000 rows/s | A flag set by Helm, up to 100,000 rows/s | Runtime changes go through `make streamers`; load tests need higher rates |
| `streamer_paused_seconds_total` | `streamer_backpressure_blocked_seconds_total` | Includes a block still in progress |
