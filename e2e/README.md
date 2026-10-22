# e2e

These are end-to-end tests against the minikube deployment: streamers → 3 brokers → collectors → ClickHouse → API gateway. The suite deploys its own streamers, drives load, breaks things on purpose, and checks that nothing a streamer was told is stored goes missing. It also checks, live, that the API's answers equal what ClickHouse holds. It records metrics and latency as it goes.

On exit, including Ctrl-C, it removes everything it created: its streamer release and the rows it wrote. It also restores everything it changed: the collector's KEDA settings, the brokers' quota, the gateway's chunk size, any ClickHouse shard it took down, and the dev streamer release, which it pauses for the run (the loss check counts every row stored since a case began).

```bash
make e2e                                          # all cases (about 40 min)
make e2e CASES="happy scale"                      # a subset
LOAD_STEPS="2:5000 4:10000" STEP_SECONDS=60 make e2e CASES=load
LOAD_READERS=4 make e2e CASES=load                  # the same steps, with 4 API readers during each
PARTITION_RATE=30000 make e2e CASES=broker-partition   # the partition case at 30,000 rows/s per streamer
make e2e-clean                                    # after a run killed with SIGKILL
```

Each run writes `e2e/results/<timestamp>/report.md`, which holds the load table, latencies and loss checks, along with the logs of every streamer, port-forward and helm call. That directory is gitignored.

## Prerequisites

All of these are checked before the run starts:
- The kube context is `minikube`.
- These releases are installed:
  - `broker`, `collector`, `api-gateway` and `telemetry-store` in namespace `telemetry`;
  - `keda` in `keda`;
  - `prometheus` in `monitoring`.

  Install all of them with `make deploy`.
- `telemetry-streamer:dev` is loaded into minikube: `make docker`.
- The API answers on its NodePort (`http://$(minikube ip):30080`).
- No release `e2e-streamer` is left over from an earlier run. Remove one with `make e2e-clean`.

## Cases

| Case | What it does | Passes when |
| --- | --- | --- |
| `limits` | Renders the streamer chart with −1, 0, 10 and 11 replicas | The chart and the suite accept 0..10 and reject the rest |
| `happy` | One streamer per broker (3 × 1,000 rows/s) for 75 s | Each streamer is on its home broker and every broker accepts records; every acked record is in ClickHouse once; producer ack p95 < 100 ms; a row is queryable within 5 s; Prometheus scrapes every component |
| `load` | Ramps `LOAD_STEPS`, streamers × rows/s each (default up to 10 × 50,000), and stops at the first step that can't be sustained. With `LOAD_READERS=N`, N API readers run through each step | A ceiling was measured with no loss. Hitting a limit is a result, not a failure. With readers, their latency and status counts are added to each step's verdict, and a 5xx or connection error fails the step |
| `scale` | Faster KEDA settings, 6 × 5,000 rows/s for 2 min, then stops | Collectors scale up by at least 2, then back to 1 within 5 min. Every replica change is printed and logged |
| `broker-crash` | Deletes `broker-0` under 2 × 5,000 rows/s: once gracefully, once with SIGKILL | Its streamer fails over to another broker **without pausing** (its blocked-time counter does not move) and returns home afterwards; broker-0 rebuilds its retry table; collectors rejoin its group and drain what it held; no loss. The outage and each streamer's longest gap without acks are measured |
| `broker-partition` | Cuts `broker-0` off the network for 20 s under 2 × `PARTITION_RATE` rows/s (default 5,000). It keeps running and stays Ready, as behind a lost node or a network split: packets to and from it are dropped at the node, so its streams stall instead of breaking | Its streamer fails over on the 2 s ack timeout **without pausing**, streams to another broker meanwhile, and returns home once broker-0 is reachable; broker-0 is not restarted; no loss. Each streamer's longest gap without acks is measured |
| `collector-crash` | Force-deletes a collector under load | The group rebalances; no loss |
| `db-down` | Takes ClickHouse shard 1 down for 60 s under load | Nothing is committed while it's down (lag grows) and insert errors are retried; afterwards the backlog drains with no loss |
| `backpressure` | Broker quota 16 MB plus ClickHouse down | A full broker pauses its producers at the quota and the streamers move to another broker; after ClickHouse returns they resume; nothing is dropped. |
| `api` | 2 × 1,000 rows/s for 60 s, then drain | Live against ClickHouse: `/gpus` lists exactly the registry; per shard, a GPU's full history equals ClickHouse's distinct rows, ordered and without duplicates; an inclusive window built from stored timestamps equals ClickHouse; `limit`/`truncated`; 404 and 400 cases; the served Swagger file equals the committed one |
| `api-chunks` | The gateway set to 500-row chunks | A busy GPU's full history, read across many chunk boundaries, equals ClickHouse |
| `api-load` | 8 readers for 60 s during 2 × 1,000 rows/s | No 5xx or connection errors; p50/p95 latency and 429s reported; no loss in the ingest |
| `api-rollout` | `kubectl rollout restart` of the gateway under 4 readers | No failed request |
| `api-db-down` | One ClickHouse replica down, then a whole shard down. It waits for a fresh minute of the `api` user's ClickHouse quota first, since `api-load` and `api-rollout` use it up on purpose | With a replica gone, a full history read is complete and right; with a shard gone, its GPUs get a JSON 503/504 while the other shard's are served; once it is back, everything is served again |

**How the API checks work.** `e2e/api_checks.py` calls the API through the NodePort and queries ClickHouse in its pods, and prints a PASS or FAIL line per check, which the suite counts.

**How the loss check works.** Before each case's streamers start, the suite follows their logs. Scaling them to 0 makes each pod log `streamer stopped … records_acked=N` before it exits. The sum of those counts must equal the number of distinct `(gpu_uuid, ts, metric_name, streamer_id)` rows in ClickHouse since the case started, after the queue drains and the replicas are synced.

**Memory in the load case.**
- **Load generators:** the streamers get 500m CPU and 128 Mi during `load`, so the test measures the pipeline, not its own load generator.
- **System under test:** the broker, collectors and ClickHouse keep their dev limits.
- **Pre-check:** a step doesn't start if minikube would pass 90 % memory.
- **Stopping:** the ramp stops on any restart or OOM kill, or if minikube's memory passes 85 %.
- **Verdicts:** each step is labelled sustained, generator-limited, pipeline-limited (producer or consumer path) or stopped, and names the component closest to its CPU limit.