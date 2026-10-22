# ==============================================================================
# GPU telemetry pipeline: one Makefile for the whole repo.
#
#   Go services:  broker, collector, api-gateway, telemetry-streamer (fmt, vet, test, build act on all)
#   Charts:       helm/<name>, deployed to minikube with their values-dev.yaml
#   Database:     ClickHouse's own tests live in clickhouse/Makefile (make -C clickhouse help)
# ==============================================================================

SHELL := /bin/bash

GO_SERVICES := broker collector api-gateway telemetry-streamer
IMAGES      := broker collector api-gateway telemetry-streamer
DEPLOY_ALL  := clickhouse keda monitoring broker collector api-gateway telemetry-streamer
NS          := telemetry
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# Optional variables: SERVICE (one service for docker, deploy, undeploy), N and RATE (streamers),
# CASES (e2e). Everything else is fixed on purpose.
SERVICE ?=
N       ?=
RATE    ?=
CASES   ?=

HELM     ?= helm
DOCKER   ?= docker
MINIKUBE ?= minikube

CH_OPERATOR_VERSION := 0.26.3
KEDA_VERSION        := 2.20.2
PROMETHEUS_VERSION  := 29.35.0
SWAG_VERSION        := v1.16.6

.DEFAULT_GOAL := help
.PHONY: help fmt vet test coverage build ci swagger swagger-check docker helm-lint deploy undeploy streamers prometheus api-port api-port-stop \
        e2e e2e-clean \
        deploy-clickhouse deploy-keda deploy-monitoring deploy-broker deploy-collector deploy-api-gateway \
        deploy-telemetry-streamer

help: ## Show the targets
	@echo "Usage: make <target> [SERVICE=<name>]"
	@echo ""
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Go services: $(GO_SERVICES)    deployable: $(DEPLOY_ALL)"
	@echo "ClickHouse tests (SQL, alerts, cluster, security, backup, load): make -C clickhouse help"

# ---- Go: every target runs in every module and reports every failure -----------------------

# $(call each_go,<label>,<shell command run inside the module>)
define each_go
@fail=""; for s in $(GO_SERVICES); do \
	echo "==> $(1): $$s"; \
	( cd $$s && $(2) ) || fail="$$fail $$s"; \
done; \
if [ -n "$$fail" ]; then echo "==> $(1) failed for:$$fail"; exit 1; fi
endef

fmt: ## Format the Go code and tidy go.mod (rewrites files)
	$(call each_go,fmt,gofmt -w . && go mod tidy)

vet: ## go vet, plus fail if code is unformatted or go.mod is not tidy
	$(call each_go,vet,go vet ./... && \
		out="$$(gofmt -l .)" && { [ -z "$$out" ] || { echo "gofmt needed: $$out"; false; }; } && \
		{ go mod tidy -diff >/dev/null || { echo "go.mod not tidy: run make fmt"; false; }; })

test: ## Unit and integration tests with the race detector, with coverage per package
	$(call each_go,test,set -o pipefail; go test -race -cover ./... | grep -v "no test files")

# Unit tests, plus the ClickHouse-backed ones (the gateway's store, the collector's sink) against a
# throwaway ClickHouse in Docker (clickhouse/tests/test-container.sh); COVER_CH=0 skips those.
# Generated protobuf code and test-support packages are not production code, so they are left out of
# every figure (COVER_EXCLUDE). -coverpkg=./... credits code to any test of its module that runs it
# (the cmd/ tests drive the internal packages end to end); a block then appears once per test binary,
# so the total counts each block once, as covered if any test ran it. Fails when a service is under
# COVER_MIN percent.
COVER_DIR     := coverage
COVER_MIN     ?= 80
COVER_CH      ?= 1
COVER_EXCLUDE := \.pb\.go:|/pkg/brokertest/|/cmd/fakeserver/

coverage: ## Coverage per service and in total, HTML in coverage/; fails under COVER_MIN (80) percent
	@rm -rf $(COVER_DIR) && mkdir -p $(COVER_DIR)
	@addr=""; \
	if [ "$(COVER_CH)" = 1 ]; then \
		echo "==> coverage: starting a throwaway ClickHouse (COVER_CH=0 skips the database tests)"; \
		addr="$$(bash clickhouse/tests/test-container.sh up)" || exit 1; \
		trap 'bash clickhouse/tests/test-container.sh down' EXIT; \
	fi; \
	fail=""; for s in $(GO_SERVICES); do \
		echo "==> coverage: $$s"; \
		( cd $$s && GATEWAY_TEST_CH_ADDR=$$addr COLLECTOR_TEST_CH_ADDR=$$addr \
		  go test -count=1 -covermode=atomic -coverpkg=./... -coverprofile=../$(COVER_DIR)/$$s.raw ./... >../$(COVER_DIR)/$$s.log 2>&1 && \
		  grep -Ev '$(COVER_EXCLUDE)' ../$(COVER_DIR)/$$s.raw >../$(COVER_DIR)/$$s.out && \
		  go tool cover -html=../$(COVER_DIR)/$$s.out -o ../$(COVER_DIR)/$$s.html && \
		  go tool cover -func=../$(COVER_DIR)/$$s.out >../$(COVER_DIR)/$$s.func ) \
		|| { fail="$$fail $$s"; tail -20 $(COVER_DIR)/$$s.log; }; \
	done; \
	if [ -n "$$fail" ]; then echo "==> coverage: tests failed for:$$fail (logs in $(COVER_DIR)/)"; exit 1; fi
	@{ printf '%-20s %8s\n' service coverage; \
	for s in $(GO_SERVICES); do printf '%-20s %8s\n' $$s "$$(tail -1 $(COVER_DIR)/$$s.func | awk '{print $$NF}')"; done; \
	cat $(COVER_DIR)/*.out | awk '!/^mode:/ { n[$$1] = $$2; if ($$3 > 0) hit[$$1] = 1 } \
		END { for (b in n) { total += n[b]; if (b in hit) covered += n[b] } printf "%-20s %7.1f%%\n", "total", 100 * covered / total }'; } \
		| tee $(COVER_DIR)/summary.txt
	@echo "==> excluded from the figures: generated protobuf code, broker/pkg/brokertest, telemetry-streamer/cmd/fakeserver"
	@echo "==> HTML reports: $(COVER_DIR)/<service>.html   per function: $(COVER_DIR)/<service>.func"
	@below=""; for s in $(GO_SERVICES); do \
		pct="$$(tail -1 $(COVER_DIR)/$$s.func | awk '{sub("%","",$$NF); print $$NF}')"; \
		awk -v p="$$pct" -v m="$(COVER_MIN)" 'BEGIN { exit !(p + 0 < m + 0) }' && below="$$below $$s($$pct%)"; \
	done; \
	if [ -n "$$below" ]; then echo "==> coverage: under $(COVER_MIN)% for:$$below"; exit 1; fi; \

build: ## Compile the binaries into <service>/bin
	$(call each_go,build,CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/ ./cmd/...)

ci: vet test swagger-check helm-lint ## vet, test, swagger-check and helm-lint: the gate before a commit

# swag reads the annotations on the gateway's handlers and types (general info on main) and writes
# Swagger 2.0 next to the package that embeds it. The gateway's tests hold the document to the code;
# swagger-check (part of ci) fails when the committed files are not what swag writes now.
SWAG := go run github.com/swaggo/swag/cmd/swag@$(SWAG_VERSION) init --quiet \
	--generalInfo cmd/api-gateway/main.go --dir ./ --parseInternal --outputTypes json,yaml

swagger: ## Generate api-gateway/openapi/swagger.{json,yaml} with swag from the gateway's annotations
	@cd api-gateway && $(SWAG) --output openapi
	@echo "==> wrote api-gateway/openapi/swagger.json and swagger.yaml (swag $(SWAG_VERSION))"

swagger-check: ## Fail if api-gateway/openapi is not what make swagger generates
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	cd api-gateway && $(SWAG) --output "$$tmp" && \
	cmp -s "$$tmp/swagger.json" openapi/swagger.json && cmp -s "$$tmp/swagger.yaml" openapi/swagger.yaml \
		&& echo "==> swagger-check: up to date" \
		|| { echo "==> swagger-check: api-gateway/openapi is out of date: run make swagger"; exit 1; }

# ---- images and charts ---------------------------------------------------------------------

# The image ID of <name>:dev in the local Docker, empty when it isn't built.
image_id = $(shell $(DOCKER) image inspect -f '{{.Id}}' $(1):dev 2>/dev/null)

# `minikube image load <name>:dev` can keep the old image when the tag is in use by a pod, so the
# images go in as tarballs and the load is checked: minikube must now hold the ID just built.
docker: ## Build the images (tag dev) and load them into minikube when it is running (SERVICE= for one)
	@for s in $(or $(SERVICE),$(IMAGES)); do \
		ctx=$$s; [ "$$s" = collector ] && ctx=.; \
		echo "==> docker build $$s:dev (context $$ctx)"; \
		$(DOCKER) build -q -f dockerfiles/$$s.Dockerfile --build-arg VERSION=$(VERSION) -t $$s:dev $$ctx >/dev/null || exit 1; \
	done
	@if ! $(MINIKUBE) status >/dev/null 2>&1; then echo "==> minikube is not running: images built but not loaded"; exit 0; fi; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	for s in $(or $(SERVICE),$(IMAGES)); do \
		want="$$($(DOCKER) image inspect -f '{{.Id}}' $$s:dev)"; \
		echo "==> minikube image load $$s:dev ($${want:7:12})"; \
		$(DOCKER) save $$s:dev -o "$$tmp/$$s.tar" && $(MINIKUBE) image load "$$tmp/$$s.tar" || exit 1; \
		got="$$($(MINIKUBE) ssh -- docker image inspect -f "'{{.Id}}'" $$s:dev 2>/dev/null | tr -d '\r')"; \
		[ "$$got" = "$$want" ] || { echo "error: minikube holds $$s:dev as $${got:-nothing}, expected $$want"; exit 1; }; \
	done

helm-lint: ## Lint and render every chart with each of its values-*.yaml
	@for c in helm/*/; do \
		for v in $$c/values-*.yaml; do \
			echo "==> helm lint $$c -f $$(basename $$v)"; \
			$(HELM) lint $$c -f $$v --quiet >/dev/null || { $(HELM) lint $$c -f $$v; exit 1; }; \
			$(HELM) template x $$c -f $$v -n $(NS) >/dev/null || exit 1; \
		done; \
	done

# ---- minikube --------------------------------------------------------------------------------

deploy: ## Install or upgrade on the current kube context (SERVICE= for one; default all, in order)
	@echo "==> kube context: $$(kubectl config current-context)"
	@for s in $(or $(SERVICE),$(DEPLOY_ALL)); do \
		case " $(DEPLOY_ALL) " in *" $$s "*) ;; *) echo "error: SERVICE must be one of: $(DEPLOY_ALL)"; exit 1;; esac; \
		$(MAKE) --no-print-directory deploy-$$s || exit 1; \
	done

# The app charts: release = chart name, namespace telemetry, dev values, rolled back on failure.
# The tag is always dev, so the pods carry the image ID as an annotation: a rebuilt image changes
# the pod template and rolls the pods, and an unchanged one leaves them alone.
deploy-broker deploy-collector deploy-api-gateway deploy-telemetry-streamer: deploy-%:
	@echo "==> deploy $*"
	@$(HELM) upgrade --install $* helm/$* -n $(NS) --create-namespace -f helm/$*/values-dev.yaml \
		$(if $(call image_id,$*),--set-string 'podAnnotations.checksum/image=$(call image_id,$*)') \
		--rollback-on-failure --timeout 5m $(EXTRA_SET) >/dev/null && echo "    $* deployed"

# The operator first: its CRDs must exist before the ClickHouseInstallation.
deploy-clickhouse:
	@echo "==> deploy clickhouse (operator $(CH_OPERATOR_VERSION), then release telemetry-store)"
	@$(HELM) repo add altinity https://helm.altinity.com --force-update >/dev/null
	@$(HELM) upgrade --install clickhouse-operator altinity/altinity-clickhouse-operator \
		--version $(CH_OPERATOR_VERSION) -n $(NS) --create-namespace --wait >/dev/null
	@$(HELM) upgrade --install telemetry-store helm/clickhouse -n $(NS) -f helm/clickhouse/values-dev.yaml \
		--rollback-on-failure --timeout 15m >/dev/null && echo "    clickhouse deployed"

# Trimmed for the demo: operator and metrics server well below their 1000Mi defaults, no admission
# webhooks (they only validate ScaledObjects), and the operator's metrics on for Prometheus.
deploy-keda:
	@echo "==> deploy keda $(KEDA_VERSION)"
	@$(HELM) repo add kedacore https://kedacore.github.io/charts --force-update >/dev/null
	@$(HELM) upgrade --install keda kedacore/keda --version $(KEDA_VERSION) -n keda --create-namespace \
		--wait --timeout 5m \
		--set webhooks.enabled=false --set prometheus.operator.enabled=true \
		--set resources.operator.requests.cpu=50m --set resources.operator.limits.cpu=500m \
		--set resources.operator.requests.memory=64Mi --set resources.operator.limits.memory=192Mi \
		--set resources.metricServer.requests.cpu=25m --set resources.metricServer.limits.cpu=500m \
		--set resources.metricServer.requests.memory=48Mi --set resources.metricServer.limits.memory=128Mi \
		>/dev/null && echo "    keda deployed"

# metrics-server feeds kubectl top; Prometheus feeds OpenLens (Helm 14.x, monitoring/prometheus-server:80).
deploy-monitoring:
	@echo "==> deploy monitoring (metrics-server, prometheus $(PROMETHEUS_VERSION))"
	@$(MINIKUBE) addons enable metrics-server >/dev/null
	@$(HELM) repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update >/dev/null
	@$(HELM) upgrade --install prometheus prometheus-community/prometheus --version $(PROMETHEUS_VERSION) \
		-n monitoring --create-namespace -f monitoring/prometheus-values.yaml --wait --timeout 5m >/dev/null \
		&& echo "    monitoring deployed"

undeploy: ## Remove one release (SERVICE= required; volumes follow each chart's retention setting)
	@case "$(SERVICE)" in \
		broker|collector|api-gateway|telemetry-streamer) $(HELM) uninstall $(SERVICE) -n $(NS) ;; \
		clickhouse) $(HELM) uninstall telemetry-store -n $(NS) ;; \
		keda) $(HELM) uninstall keda -n keda ;; \
		monitoring) $(HELM) uninstall prometheus -n monitoring ;; \
		*) echo "error: SERVICE must be one of: $(DEPLOY_ALL)"; exit 1 ;; \
	esac

# The streamer count is a knob, not autoscaled. Without RATE the release keeps its current rate.
streamers: ## Set the streamer count (N=0..10, optional RATE=rows/s per streamer)
	@if ! printf '%s' "$(N)" | grep -Eq '^([0-9]|10)$$'; then \
		echo "error: N must be 0..10, e.g. make streamers N=5 RATE=2000"; exit 1; fi
	@if [ -n "$(RATE)" ] && ! { printf '%s' "$(RATE)" | grep -Eq '^[1-9][0-9]*$$' && [ "$(RATE)" -le 100000 ]; }; then \
		echo "error: RATE must be 1..100000 rows/s per streamer"; exit 1; fi
	@rate="$(RATE)"; \
	[ -n "$$rate" ] || rate="$$($(HELM) get values telemetry-streamer -n $(NS) -a -o json 2>/dev/null | jq -r '.streamer.rateRowsPerSec // empty')"; \
	set="--set streamer.replicas=$(N)"; [ -z "$$rate" ] || set="$$set --set streamer.rateRowsPerSec=$$rate"; \
	echo "==> streamers: $(N) x $${rate:-1000} rows/s"; \
	$(MAKE) --no-print-directory deploy-telemetry-streamer EXTRA_SET="$$set"

# The Prometheus UI from the monitoring namespace, on localhost; it runs until Ctrl-C.
prometheus: ## Open the Prometheus UI at http://localhost:9090 (port-forward; Ctrl-C stops it)
	@kubectl -n monitoring get svc prometheus-server >/dev/null 2>&1 || \
		{ echo "error: no Prometheus in namespace monitoring: make deploy SERVICE=monitoring"; exit 1; }
	@if curl -s -m 2 -o /dev/null localhost:9090/-/ready; then \
		echo "==> Prometheus UI is already at http://localhost:9090 (another port-forward holds the port)"; exit 0; fi; \
	echo "==> Prometheus UI: http://localhost:9090  (Ctrl-C to stop)"; \
	kubectl -n monitoring port-forward svc/prometheus-server 9090:80

# The API on localhost:8080 through a background port-forward that reconnects when the gateway pod
# changes (rollouts, restarts). Its log and process-group id are kept in .api-port/.
api-port: ## Serve the API at http://localhost:8080 in the background (make api-port-stop ends it)
	@if [ -f .api-port/pid ] && kill -0 -- -"$$(cat .api-port/pid)" 2>/dev/null; then \
		echo "==> already running at http://localhost:8080 (make api-port-stop ends it)"; exit 0; fi; \
	kubectl -n $(NS) get svc api-gateway >/dev/null 2>&1 || \
		{ echo "error: no api-gateway Service in namespace $(NS): make deploy SERVICE=api-gateway"; exit 1; }; \
	if (exec 3<>/dev/tcp/127.0.0.1/8080) 2>/dev/null; then \
		echo "error: localhost:8080 is already in use by another process"; exit 1; fi; \
	mkdir -p .api-port; rm -f .api-port/pid; \
	setsid sh -c 'echo $$$$ > .api-port/pid; \
		while :; do kubectl -n $(NS) port-forward --address 127.0.0.1 svc/api-gateway 8080:8080; sleep 1; done' \
		>.api-port/log 2>&1 </dev/null & \
	for _ in $$(seq 1 20); do \
		code="$$(curl -s -m 2 -o /dev/null -w '%{http_code}' 'http://localhost:8080/api/v1/gpus?limit=1')"; \
		[ "$$code" != "000" ] && break; sleep 0.5; done; \
	if [ "$$code" = "000" ]; then \
		[ -f .api-port/pid ] && kill -- -"$$(cat .api-port/pid)" 2>/dev/null; rm -f .api-port/pid; \
		echo "error: the port-forward did not come up; last lines of .api-port/log:"; tail -5 .api-port/log; exit 1; fi; \
	echo "==> API:        http://localhost:8080/api/v1/gpus"; \
	echo "==> Swagger UI: http://localhost:8080/api/v1/docs"; \
	echo "    running in the background; make api-port-stop ends it"

api-port-stop: ## Stop the background port-forward started by make api-port
	@if [ -f .api-port/pid ] && kill -- -"$$(cat .api-port/pid)" 2>/dev/null; then \
		echo "==> stopped the API port-forward"; else echo "==> not running"; fi; \
	rm -f .api-port/pid

# ---- end-to-end tests against the cluster ----------------------------------------------------

e2e: ## Cluster e2e suite: load, scaling, broker failover, crashes, the API live, no-loss checks; cleans up (CASES=)
	@CASES="$(CASES)" bash e2e/cluster-e2e.sh

e2e-clean: ## Clean up after an interrupted e2e run (streamers, overrides, test rows)
	@cd e2e && bash -c 'source ./lib.sh; COLLECTOR_CHANGED=1; BROKER_CHANGED=1; GATEWAY_CHANGED=1; \
		since="$$(cat results/last-T0 2>/dev/null)"; pf_prom; cleanup "$$since"; echo "e2e-clean: $$pass passed, $$fail failed"'
