#!/usr/bin/env bash
# Security tests against a deployed release: network policies (needs a CNI that enforces them,
# for example minikube start --cni=calico), the users' source-network restriction, and
# credential rotation. Starts short-lived busybox pods; rotation rolls the ClickHouse pods.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/lib.sh"

load_creds
BUSYBOX="${BUSYBOX:-busybox:1.36.1}"
TARGET="$(pod 0 0 | sed 's/-0$//')"      # the per-pod service, e.g. chi-<fn>-<cluster>-0-0
SKIP_ROTATION="${SKIP_ROTATION:-0}"

# probe <name> <extra kubectl run args...> -- <shell command>: runs a throwaway pod, prints its output.
probe() {
  local name="$1"; shift
  local args=()
  while [[ "$1" != "--" ]]; do args+=("$1"); shift; done; shift
  kubectl -n "$NS" run "$name-$RANDOM" --image="$BUSYBOX" --restart=Never --rm -i --quiet \
    "${args[@]}" --command -- sh -c "$*" 2>&1
}
reachable() { # reachable <probe args...>: prints open or closed for TARGET:9000
  probe "$@" -- "nc -z -w 3 $TARGET 9000 && echo open || echo closed" | tail -1
}

# ---- 1. ingress policy ----
note "1. ingress: only labelled clients reach ClickHouse"
check "unlabelled pod cannot reach 9000" "closed" "$(reachable np-unlabelled)"
check "pod labelled telemetry-store/client=true can reach 9000" "open" \
  "$(reachable np-client --labels=telemetry-store/client=true)"

# ---- 2. egress policy ----
note "2. egress: ClickHouse pods reach DNS and peers, nothing else"
check "ClickHouse pod resolves cluster DNS" "yes" \
  "$(kubectl -n "$NS" exec "$(pod 0 0)" -c clickhouse -- sh -c 'getent hosts kubernetes.default.svc.cluster.local >/dev/null && echo yes || echo no')"
check "ClickHouse pod reaches its replica" "yes" \
  "$(kubectl -n "$NS" exec "$(pod 0 0)" -c clickhouse -- bash -c "timeout 5 bash -c '</dev/tcp/$(pod 0 1 | sed 's/-0$//')/9000' && echo yes || echo no")"
check "ClickHouse pod cannot reach the internet" "no" \
  "$(kubectl -n "$NS" exec "$(pod 0 0)" -c clickhouse -- bash -c "timeout 5 bash -c '</dev/tcp/1.1.1.1/443' 2>/dev/null && echo yes || echo no")"

# ---- 3. source-network restriction on the users ----
# A host-network pod connects from the node IP, which is outside security.allowedNetworks.
note "3. users only accept their allowed source networks"
from_node="$(probe np-hostnet --labels=telemetry-store/client=true \
  --overrides='{"spec":{"hostNetwork":true}}' -- \
  "wget -q -T 5 -O- --header 'X-ClickHouse-User: collector' --header 'X-ClickHouse-Key: $COLLECTOR_PW' 'http://$TARGET.$NS.svc.cluster.local:8123/?query=SELECT%201' 2>&1 || true")"
if grep -qiE 'not allowed|IP_ADDRESS_NOT_ALLOWED|AUTHENTICATION_FAILED|403|516' <<<"$from_node"; then
  ok "collector is refused from the node address"
elif grep -qiE 'timed out|bad address|Connection refused|can.t connect' <<<"$from_node"; then
  note "SKIP  source-network check: the network policy blocked the node before ClickHouse saw it ($from_node)"
else
  bad "collector is refused from the node address" "got [$from_node]"
fi
from_pod="$(probe np-allowed --labels=telemetry-store/client=true -- \
  "wget -q -T 5 -O- --header 'X-ClickHouse-User: collector' --header 'X-ClickHouse-Key: $COLLECTOR_PW' 'http://$TARGET:8123/?query=SELECT%201'")"
check "collector is accepted from the pod network" "1" "$(tail -1 <<<"$from_pod")"

# ---- 4. credential rotation ----
if [[ "$SKIP_ROTATION" != "1" ]]; then
  note "4. credential rotation: new password works, old one stops, no manual restart"
  OLD="$COLLECTOR_PW"
  make -s -C "$HERE/.." rotate-credentials ROTATE_USER=collector NAMESPACE="$NS" FULLNAME="$FN" >/dev/null 2>&1 \
    || bad "rotate-credentials ran" "make failed"
  NEW="$(secret_val collector-password)"
  check "the Secret holds a new password" "1" "$([[ -n "$NEW" && "$NEW" != "$OLD" ]] && echo 1 || echo 0)"
  # The operator rolls one pod at a time; wait until every pod accepts the new password.
  deadline=$(( $(date +%s) + 900 )); rolled=0
  while (( $(date +%s) < deadline )); do
    n=0
    for s in $(seq 0 $((SHARDS-1))); do for r in $(seq 0 $((REPLICAS-1))); do
      as collector "$NEW" "$s" "$r" "SELECT 1" >/dev/null 2>&1 && n=$((n+1))
    done; done
    [[ $n -eq $((SHARDS*REPLICAS)) ]] && { rolled=1; break; }
    sleep 10
  done
  check "every pod accepts the new password" "1" "$rolled"
  check "the old password is refused" "yes" \
    "$(rejects 'AUTHENTICATION_FAILED|password is incorrect|Authentication failed' as collector "$OLD" 0 0 "SELECT 1")"
fi

summary
