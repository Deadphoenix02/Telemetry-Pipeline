# Backlog

Open work for the pipeline. The first section is what the dev machine (one minikube node, 4 CPUs, 8 GiB) is too small to exercise. The rest is by topic.

## Needs more resources than the dev machine

### KEDA with 3 brokers

Only the e2e `scale` case has run: 6 × 5,000 rows/s, then it checks that the collectors went up by at least 2 and came back to 1. Still to test:
- every broker's consumer Group following each scale event (members equal to the replicas, all 30 partitions assigned);
- lag on one broker only, where the HPA follows that broker's trigger and over-scales (see [KEDA scaling on the total lag](#keda-scaling-on-the-total-lag));
- one broker's `/lag` unreachable, so the `fallback` (after 3 failed reads, hold 2 replicas) engages;
- a scale-down while a broker is unreachable, with every collector leaving within its grace period;
- the 10-replica cap, with 10 collectors × 3 broker sessions in memory;
- eager-rebalance pauses ([G21](#in-memory-membership-and-eager-rebalancing-g21)) showing as lag spikes while scaling.

### Throughput

- The 500,000 rows/s target end to end: ingest alone is CPU-bound at 100,000–160,000 rows/s here, and reads under load were measured only up to that (V5, under [Results checked on the dev cluster](#results-checked-on-the-dev-cluster)).
- The target itself still has to be checked against the real GPU count and scrape interval.
- Per-streamer rates above 20,000 rows/s (V11, [G20](#one-grpc-message-per-record-g20)).

### ClickHouse under sustained, real load

The load test ran 90-second bursts with synthetic values and large batches (up to 125,000 rows per insert, 37,000 on average), and its generator shared the machine's 16 threads. Not exercised:
- soak runs of hours, where merges and TTL drops show;
- real DCGM values; compression (6.2 bytes per row) was measured on synthetic data;
- small batches, which create more parts per second;
- memory headroom: peak cgroup memory reached about 950 MiB of the 1 GiB limit at 500,000 rows/s;
- replica catch-up after an outage under load, and different rows per batch.

### The API at production scale

- The API's capacity is set by the `api` user's per-minute ClickHouse quota, not by the gateway (V12). The right quota and replica count for real clients is unmeasured.
- `GET /gpus` at about 190,000 GPUs was measured only against a fake store (V6).

### More than one node

- Anti-affinity and zone spread for the brokers, ClickHouse and Keeper.
- A real node loss, or a network partition between nodes (V10 simulated one on a single node).
- Production sizing and shard count.

## Other items

### Quota 429s say `Retry-After: 1`

- The `api` user's quota counts per minute, so a client told to retry in 1 s can keep hitting it for up to a minute. Each failed attempt also counts against the quota's 200 errors a minute.
- Fix: for ClickHouse error 201 (quota exceeded), set `Retry-After` to the end of the quota interval, which the error message carries ("Interval will end at …"), with 1 s as the fallback.

### Broker retry detection is bounded

- The retry table covers the last 1,024 entries per partition, and producers idle for less than an hour.
- An older retry is stored twice. ClickHouse's key `(gpu_uuid, ts, metric_name, streamer_id)` collapses it, so the API never shows it.
- Fix, if needed: keep more retry entries per partition, at the cost of broker memory.

### One gRPC message per record

- Every record is its own message on the stream, with a cumulative ack, which caps a streamer's throughput. In the e2e `load` case at 200,000 rows/s offered (10 streamers), the streamers were the limit.
- Fix: batch records per message (the design's original per-partition batches), keeping the per-record sequence numbers.

### In-memory membership and eager rebalancing

- After a broker restart every collector rejoins its group; committed offsets survive, on disk.
- Every scale event pauses consumption on every partition of every broker for the length of the rebalance.
- Fix: persist or replicate membership, and move to cooperative rebalancing, so that only the moved partitions pause.

### Results checked on the dev cluster

These were checked on the dev cluster (minikube, 4 CPUs, 8 GiB) on 21 October 2026.

- **V4. Can the GPU list show GPUs whose telemetry has expired?** Yes, and the opposite too, for minutes to hours.
  - Rows that are already expired when inserted are dropped at once. So the test inserted a GPU 30 s before its TTL and watched it cross.
- **V5. Does the 500,000 rows/s target hold with reads running?**
  - Not on this machine, and not because of reads: ingest alone is CPU-bound at 100,000–160,000 rows/s on the 4-CPU minikube (the e2e `load` ceiling).
  - Measured with `LOAD_READERS=4 make e2e CASES=load` (4 API readers through each step), against the same steps without readers:
    - **10,000 rows/s:** ingest unchanged (9,984 rows/s inserted, ack p95 79 ms against 77 ms). Readers saw p50 13 ms and p95 61 ms, and 63 % of their requests got 429 from the quota.
    - **100,000 rows/s, minikube's CPU saturated:**
      - reads and ingest compete: insert p95 0.10 → 0.40 s, ack p95 0.14 → 0.42 s, inserted 97,000 → 72,000 rows/s over the same 60 s window, with lag not growing;
      - the readers got 576 answers in 55 s, p95 1.2 s, with no errors.
  - Whether 500,000 rows/s holds with reads on production-sized hardware stays open. It needs that hardware.
- **V11. Is the window large enough above about 20,000 rows/s per streamer?** No, for a hang: a 0.88 s pause at 30,000 rows/s.
- **V12. How many API readers does one gateway pod serve?** The gateway is not what limits the API; the `api` user's ClickHouse quota is.
  - Measured with 1, 2, 4, 8 and 16 readers sending back to back for 45 s, on 1 replica and on 2 (4 query slots each), during the dev ingest:
    - the gateway's query slots turned nothing away (`gateway_rejected_total` stayed 0);
    - every 429 came from the quota, and ClickHouse finished about 2,000–2,700 initial queries a minute in every run;
    - p95 stayed between 24 and 102 ms.
  - From 2 readers up, most requests got 429, on either replica count. Even one reader used most of the quota.
  - To serve more, raise `quota.queriesPerMinute` for the `api` user; replicas and `maxInFlight` matter only after that.

### Broker replication

Several independent brokers give write availability: streamers fail over without pausing (verified by `make e2e CASES="broker-crash broker-partition"`). A record still lives only on the broker that acked it, so losing a broker's *volume* (not a restart) loses its unconsumed records, normally seconds of data. A leader-follower design would close this with no change for the streamers:

1. **Each broker's log gets followers.** With 3 brokers and a replication factor of 3, broker-0 leads log 0 and follows logs 1 and 2, and so on. Streamers keep writing to their home broker, which leads that log. Collectors keep consuming each log from its leader.
2. **Followers pull, like a collector does.** A follower runs the existing `Fetch` stream against the leader, from its own log end. It appends what it receives to its copy of the WAL, fsyncs it, and reports its new log end on its next fetch. Partitioning, frames and offsets are unchanged, so a follower's copy is byte-for-byte the leader's.
3. **The high watermark (HW) moves to the slowest in-sync copy.** Today the HW is the leader's last fsynced offset. With replication it becomes the minimum log end over the in-sync replicas (ISR): the leader plus every follower that has caught up within, for example, 2 s. Two existing rules then give `acks=all` for free:
   - a streamer is acked only once the HW covers its record;
   - `Fetch` never serves past the HW.

   An acked record is therefore on every in-sync copy, and a collector never reads a record that a failover could lose.
4. **A minimum ISR keeps writes safe.** With `min_insync = 2`, a leader left alone refuses writes. Its streamers fail over to another broker, as they do today when a broker goes away, so writes never pause while some log is still writable.
5. **Leader election uses a Kubernetes Lease per log.** The leader renews its Lease. When the Lease expires, the most caught-up member of the ISR takes it and becomes leader. The Lease carries a leader epoch: a stale leader that comes back has its writes rejected, truncates its log to the HW, and rejoins as a follower. Unclean election (an out-of-sync replica taking over) is disabled, so an acked record is never lost. Raft would replace the Lease if the brokers had to run outside Kubernetes.
6. **The consumer group follows the leader.** Committed offsets (`offsets.json`) are replicated with the log, as a small control record in each partition or in the Lease's annotations. A new leader therefore resumes the group exactly where it stood. Collectors find the leader through the Lease and simply reconnect.

**The costs.**
- Three times the disk and the replication traffic.
- One more round trip plus a follower fsync on every ack: about 20–40 ms on top of today's 20 ms group commit.
- More failure states to test: lagging followers, a leader epoch changing mid-fetch, ISR shrink and expand.

**What stays the same:**
- The streamers' failover and the one-active-stream rule.
- The collectors' commit-after-insert.
- ClickHouse's deduplication, which still absorbs a record resent after a lost ack.

### OpenAPI 3

The served document is Swagger 2.0, because stable swag (v1.16.6) only produces 2.0; OpenAPI 3 needs swag v2, still a release candidate. Redocly's recommended rules flag `security-defined`, because swag v1 can't declare "no authentication".

### KEDA scaling on the total lag

The collectors' HPA takes the largest of its per-broker triggers. Each trigger targets `lagTarget / brokers`, which matches the total when lag is spread evenly and over-scales when it sits on one broker. Scaling on the sum would need a Prometheus trigger (`sum(broker_group_lag_records)`), making scaling depend on Prometheus.

### Host and GPU-model metadata

Not required by the assignment: the API lists GPU ids and their telemetry, and nothing queries by host or model.

### Production values for the app charts

The charts have production-shaped defaults and a dev profile; production numbers would be guesses without real requirements.

### Authentication, rate limiting and per-request caps for the API

Not required by the assignment. The optional `limit` is there as a safety valve for clients.

### A "latest N" query

Not required by the assignment. Telemetry comes earliest first.

### Proxies in front of the API

An Ingress and its timeouts for long streams.

### Production retention

Dev keeps 4 hours. Production retention depends on requirements the assignment doesn't give.

### Every broker unavailable at once

Not tested. In that state the streamers' windows fill and their pacers pause, so nothing is dropped. The unit tests cover the backoff path; no e2e case takes every broker down.

### Security hardening

- ClickHouse TLS beyond what the chart enables: the operator over TLS, Keeper's secure port and ClickHouse-to-Keeper TLS, Distributed queries over the secure port, and closing the plaintext ports. That needs a test on a running cluster first.
- TLS certificates rotate only when the `-tls` Secret is deleted and the chart upgraded, and nothing alerts before they expire.
- gRPC between the streamers, brokers and collectors is plaintext; mTLS between components, and NetworkPolicies narrowed to specific pods.
- The ClickHouse test helpers pass passwords as `kubectl exec` arguments.
- Secrets are only as safe as the cluster's Secret storage; production would use `auth.existingSecret` with a secret manager.

### Production operations

- Alertmanager and notification routing (dev reads alerts in its own Prometheus), and log shipping (only ClickHouse's system log tables, with a 1-day TTL in dev).
- The Grafana dashboard has not been loaded into a real Grafana; the backup alert metric names are unverified.
- The ClickHouse restore drill has never been run; there is no point-in-time recovery; `schema_migrations` is not replicated or backed up.
- Runbooks (disk full, too many parts, a lost replica), the operator and CRD upgrade path, rebalancing existing data when a shard is added, merge-backlog tuning and capacity planning.

### Rollup tables for long ranges

For example 1-minute and 1-hour averages through materialized views. No requirement queries aggregates; the API returns raw entries.
