# ==============================================================================
# ClickHouse tests and operations. Deploying is in the root Makefile (make deploy SERVICE=clickhouse);
# this file holds what only the database needs: a local container, its SQL and alert tests, and the
# tests that run against the deployed release.
# ==============================================================================

CH_IMAGE     ?= altinity/clickhouse-server:25.8.16.10002.altinitystable
CH_NAME      ?= ch-telemetry
PROM_IMAGE   ?= prom/prometheus:v2.55.1
NAMESPACE    ?= telemetry
RELEASE      ?= telemetry-store
FULLNAME     ?= $(RELEASE)
ROTATE_USER  ?= collector
CONFIRM_WIPE ?=

HERE       := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
CHART      := $(abspath $(HERE)../helm/clickhouse)
ALERTS_DIR := $(HERE).alerts-test
CLUSTER_ENV := NS=$(NAMESPACE) FULLNAME=$(FULLNAME)

.DEFAULT_GOAL := help
.PHONY: help run shell down test sql-test alerts-test cluster-test security-test backup-drill load-test \
        rotate-credentials disk-check

help: ## Show the targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---- local container (Docker) ---------------------------------------------------------------

run: ## Start one local ClickHouse with the dev config and users (shell, down)
	docker run -d --rm --name $(CH_NAME) -p 127.0.0.1:8123:8123 -p 127.0.0.1:9000:9000 \
	  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
	  -v $(HERE)config/dev-config.xml:/etc/clickhouse-server/config.d/dev-config.xml:ro \
	  -v $(HERE)config/dev-users.xml:/etc/clickhouse-server/users.d/dev-users.xml:ro \
	  $(CH_IMAGE) >/dev/null
	@for i in $$(seq 1 60); do docker exec $(CH_NAME) clickhouse-client --query "SELECT 1" >/dev/null 2>&1 && exit 0; sleep 1; done; \
	  echo "ClickHouse did not start" >&2; exit 1

shell: ## clickhouse-client in the local container
	docker exec -it $(CH_NAME) clickhouse-client

down: ## Remove the local container
	-docker rm -f $(CH_NAME) >/dev/null 2>&1

# ---- offline tests (Docker) -----------------------------------------------------------------

test: sql-test alerts-test ## SQL suite and alert-rule tests (offline, needs Docker)

sql-test: down run ## SQL tests in a fresh local container (removed afterwards)
	CLICKHOUSE_CLIENT="docker exec -i $(CH_NAME) clickhouse-client" bash $(HERE)tests/run.sh; \
	  rc=$$?; docker rm -f $(CH_NAME) >/dev/null 2>&1; exit $$rc

# Renders the alert rules exactly as the dev chart ships them, then runs promtool's syntax check
# and the unit tests in tests/alerts_test.yaml.
alerts-test: ## promtool check and unit tests of the rendered alert rules
	rm -rf $(ALERTS_DIR) && mkdir -p $(ALERTS_DIR)
	helm template $(RELEASE) $(CHART) -n $(NAMESPACE) -f $(CHART)/values-dev.yaml -s templates/prometheus.yaml \
	  | python3 -c "import sys,yaml; d=[x for x in yaml.safe_load_all(sys.stdin) if x and x['kind']=='ConfigMap'][0]['data']; open('$(ALERTS_DIR)/alerts.yaml','w').write(d['alerts.yaml']); open('$(ALERTS_DIR)/prometheus.yml','w').write(d['prometheus.yml'].replace('/etc/prometheus/alerts.yaml','alerts.yaml'))"
	cp $(HERE)tests/alerts_test.yaml $(ALERTS_DIR)/
	docker run --rm -v $(ALERTS_DIR):/w -w /w --entrypoint promtool $(PROM_IMAGE) check config prometheus.yml
	docker run --rm -v $(ALERTS_DIR):/w -w /w --entrypoint promtool $(PROM_IMAGE) test rules alerts_test.yaml
	rm -rf $(ALERTS_DIR)

# ---- against the deployed release (make deploy SERVICE=clickhouse first) ---------------------

cluster-test: ## Replica, shard, Keeper and rolling-restart tests (with alert checks)
	$(CLUSTER_ENV) bash $(HERE)tests/cluster-test.sh

security-test: ## Network policy, source-network and credential-rotation tests (needs Calico)
	$(CLUSTER_ENV) bash $(HERE)tests/security-test.sh

backup-drill: ## Back up, destroy and restore the tables (needs backup.enabled; mutates the release)
	$(CLUSTER_ENV) bash $(HERE)tests/backup-drill.sh

load-test: ## Ramp the insert rate straight into ClickHouse (truncates telemetry: CONFIRM_WIPE=yes)
	$(CLUSTER_ENV) CONFIRM_WIPE=$(CONFIRM_WIPE) bash $(HERE)tests/load-test.sh

# Writes a new random value into the Secret, then upgrades so the pods roll and pick up the hash.
rotate-credentials: ## Rotate one user's password (ROTATE_USER=collector|api|migrator|backup)
	kubectl -n $(NAMESPACE) patch secret $(FULLNAME)-credentials --type merge \
	  -p "{\"data\":{\"$(ROTATE_USER)-password\":\"$$(head -c 24 /dev/urandom | base64 | tr -dc A-Za-z0-9 | head -c 32 | base64 -w0)\"}}"
	helm upgrade $(RELEASE) $(CHART) -n $(NAMESPACE) -f $(CHART)/values-dev.yaml --wait --timeout 15m

disk-check: ## Disk used by the minikube node and its volumes
	@docker system df -v 2>/dev/null | awk '$$1=="minikube" && NF==3 {print "minikube node volume: " $$3}'
	@minikube ssh -- "sudo du -sh /tmp/hostpath-provisioner 2>/dev/null" | awk '{print "  of which PVCs:        " $$1}'
