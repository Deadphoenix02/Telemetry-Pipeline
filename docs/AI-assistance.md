# How AI was used

The pipeline was built with Claude Code, an AI coding agent running in the terminal and in VS Code, mostly on Claude Opus. The author owned the requirements, the design direction, the decisions at every fork along with the plan and Claude did the coding and tests.

This page covers:
- [the workflow](#workflow);
- [who did what](#who-did-what), AI against manual;
- [the prompts](#prompts) that bootstrapped each part;
- [where a prompt fell short](#where-a-prompt-fell-short) and needed manual intervention.

The prompts are quoted from the local Claude Code session transcripts, lightly trimmed. They are not edited for spelling.

## Workflow

The same loop ran for every component:

1. **Design first, by the author.** A short design doc, or a pasted requirement, went in as context. Examples: the ClickHouse design, the WAL broker design, and the API spec from the assignment.
2. **Gap review and plan by the agent.** AI read the doc and the existing code, listed gaps and discrepancies, and proposed a plan. For larger changes this happened in plan mode, so nothing was edited before the author approved.
3. **Questions back to the author.** Where the plan forked, the agent asked:
   - single or multiple brokers;
   - cursor paging or complete results;
   - how coverage treats generated code.

   The author also redirected plans that were heading the wrong way.
4. **Implementation, tests and a live run by the agent.**
   - Code, unit tests, Helm charts, Dockerfiles and Makefile targets.
   - Then `make ci` (vet, race-enabled tests, swagger check, helm lint).
   - Then a deploy to the author's minikube and the cluster end-to-end suite. Failures were debugged from pod logs and Prometheus, and rerun.
5. **Docs updated to what was built**, with the measured numbers from the run.
6. **Commit by the author**, after review. The agent never committed.

`CLAUDE.md`-style standing rules weren't used. Each session started from the repo and its docs.

## Who did what

| Area                       | AI (Claude Code)                                                                                                                   | Author                                                                                                                                                                                                                                                                      |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Requirements and design    | Gap analysis against the requirements PDF and the design docs; trade-off write-ups; the as-built design docs                       | The starting design docs (ClickHouse store, WAL broker, pipeline), brought in before the recorded sessions; every design decision, such as independent brokers with failover rather than replicas, streamed complete results rather than cursors, and the 0–10 streamer cap |
| telemetry-streamer         | Producer identity, StatefulSet chart, the multi-broker failover worker and its tests                                               | The first version (CSV replay, pacing, gRPC client, fake server), written before the recorded sessions                                                                                                                                                                      |
| broker                     | All the code from the design: WAL on `tidwall/wal`, partitioning, group commit, consumer groups, quota, `/lag`, chart and tests    | Choosing an off-the-shelf WAL; the HTTP lag endpoint for KEDA; the gRPC consumer protocol                                                                                                                                                                                   |
| collector, KEDA            | Code, chart, KEDA ScaledObject, multi-broker consumption, tests                                                                    | Batching and freshness requirements; the scaling bounds                                                                                                                                                                                                                     |
| ClickHouse                 | Review of the chart and configs; fixes for the startup crash loops; migrations; users; `cluster-test` and `load-test`              | The schema design; the shape of the dev cluster (more shards and replicas, small volumes, short TTL); no backups or MinIO in dev                                                                                                                                            |
| api-gateway                | The whole service, swag annotations, chart, NodePort, client guidance for broken streams                                           | The endpoints; 1 replica; NodePort over port-forward; swag over a custom generator; no default limit                                                                                                                                                                        |
| Build and test environment | Makefile consolidation (7 Makefiles down to 2), `make coverage` with Docker ClickHouse, the e2e suite, monitoring values, diagrams | Which targets to keep; the e2e scope (live cluster, API included)                                                                                                                                                                                                           |
| Operations                 | Every deploy, debug and rerun on minikube that needed no root                                                                      | Moving minikube's storage to a bigger partition (needed `sudo`); restarting minikube; rearranging the repo layout by hand; reviewing and committing                                                                                                                         |

## Prompts

### Bootstrapping the repo and the database

- > This is a database designed for storing telemetry data. Go through the design "Database Design ClickHouse Telemetry Store.md" and see if the implementation and its configs are right.
- > the idea is to replicate a production like environment but within a miniature laptop. Lets shrink the dev size per replica and shrink down the TTL to 3-4 hours if possible. I would like to have more replicas more shards than sheer size per volume. We need backup and restores drill to be done by the operator. We need tests for replica mode and failure modes

### The broker and the collector

- > Take a look at these documents for a custom queue with a single topic, that uses WAL to write to a collector then from collector to our clickhouse db. Find the gaps, undefined part, discrepancies in the design. Write a plan to implement this feature
- > I want the connection between the broker to collecter/consumer to be grpc as well. lets use http for keda. This is just the demo, so we are going to have to keep our resource constraint in our mind. Design it accordingly. Please use a off the shelf WAL if we have one. Redo the plan.
- > try deploying the collector, keda and the broker without the streamer.

### Tests and the build environment

- > I want an e2e test cases for the streamer, broker, collector, clickhouse db againt the minikube env, which deletes at the end of the test. This is primarily for the metrics, latency, load, e2e testing of happy and other paths.
- > The max number of streamer should be 0-10 lets make sure we have a checker for that.
- > I want you to fix the make files. I want clearly defined vet,fmt altogether for go services. Builder docker, helm-lint. Crunch it down as much as possible. create make for e2e test againt the cluster.
- > Lets change the e2e tests case to test the api gateway as well, But lets do it against a live cluster primarily

### The API gateway

- > I want the api to be port forwarded to local. I want a swagger file from the api gateway
- > why cant we use swaggo or swag to generate the openapi spec instead of changing the whole service

### Broker availability

- > Can we design the system such a way that, when the broker 0 goes down, all the streamers writes to broker 1 and 2?
- > Make sure that the streamers dont pause until they have all the brokers unavailable. Make sure to update the documentation and backlogs after the change in implementation
