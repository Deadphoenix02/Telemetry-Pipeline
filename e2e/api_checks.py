#!/usr/bin/env python3
"""Live checks of the API gateway for the cluster e2e suite (e2e/cluster-e2e.sh).

Every check compares what the API answers, through the dev NodePort, with what
ClickHouse holds, queried directly in its pods. Output is one line per result,
tab-separated, which lib.sh's api_checks turns into the suite's PASS/FAIL count:
    PASS <name> | FAIL <name> <detail> | REPORT <text>
"""
import argparse
import json
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def out(kind, name, detail=""):
    print(f"{kind}\t{name}\t{detail}", flush=True)


def check(name, cond, detail=""):
    out("PASS" if cond else "FAIL", name, "" if cond else detail)
    return cond


class API:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def get(self, path, params=None, timeout=120):
        """(status, parsed JSON or None, error). error is set when the transfer broke."""
        url = self.base + path + ("?" + urllib.parse.urlencode(params) if params else "")
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                body = r.read()
                return r.status, json.loads(body), None
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read()), None
            except Exception:
                return e.code, None, None
        except Exception as e:  # connection refused, reset, incomplete chunked body, bad JSON
            return 0, None, repr(e)

    def raw(self, path):
        try:
            with urllib.request.urlopen(self.base + path, timeout=30) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, b""
        except Exception:
            return 0, b""


class CH:
    def __init__(self, ns):
        self.ns = ns

    def q(self, pod, query):
        res = subprocess.run(["kubectl", "-n", self.ns, "exec", pod, "-c", "clickhouse", "--",
                              "clickhouse-client", "--query", query],
                             capture_output=True, text=True, timeout=120)
        if res.returncode != 0:
            raise RuntimeError(f"{pod}: {res.stderr.strip()[:300]}")
        return res.stdout.strip()


def ns(ts):
    """RFC 3339 UTC with up to nanoseconds -> integer nanoseconds."""
    date, _, rest = ts.partition("T")
    hms, _, frac = rest.rstrip("Z").partition(".")
    import calendar
    secs = calendar.timegm(time.strptime(date + "T" + hms, "%Y-%m-%dT%H:%M:%S"))
    return secs * 1_000_000_000 + int((frac + "000000000")[:9])


def key(e):
    return (ns(e["timestamp"]), e["metric"], e["streamer_id"])


def busiest_gpu(ch, pod, since_ns=None):
    """The GPU with the most rows on this pod's shard (local table)."""
    where = f"WHERE ts >= fromUnixTimestamp64Nano(toInt64({since_ns}))" if since_ns else ""
    return ch.q(pod, f"SELECT toString(gpu_uuid) FROM telemetry_local {where} GROUP BY gpu_uuid ORDER BY count() DESC LIMIT 1")


def ch_count(ch, pod, gpu, lo=None, hi=None):
    where = f"gpu_uuid = toUUID('{gpu}')"
    if lo is not None:
        where += f" AND ts >= fromUnixTimestamp64Nano(toInt64({lo})) AND ts <= fromUnixTimestamp64Nano(toInt64({hi}))"
    return int(ch.q(pod, f"SELECT count() FROM telemetry FINAL WHERE {where}"))


def full_history_matches(api, ch, pod, gpu, label):
    """The full history equals ClickHouse's distinct rows, ordered, without duplicates."""
    t0 = time.time()
    code, d, err = api.get(f"/gpus/GPU-{gpu}/telemetry")
    took = time.time() - t0
    if not check(f"{label}: full history of GPU-{gpu[:8]} returned", code == 200 and d is not None,
                 f"status {code} {err or ''}"):
        return None
    want = ch_count(ch, pod, gpu)
    entries = d["telemetry"]
    check(f"{label}: full history equals ClickHouse ({len(entries)} entries)",
          len(entries) == want == d["count"] and not d["truncated"],
          f"API {len(entries)} (count {d['count']}, truncated {d['truncated']}), ClickHouse {want}")
    keys = [key(e) for e in entries]
    check(f"{label}: ordered by time, metric, streamer", keys == sorted(keys), "out of order")
    check(f"{label}: no duplicate entries", len(set(keys)) == len(keys), f"{len(keys) - len(set(keys))} duplicates")
    out("REPORT", f"{label}: {len(entries)} entries of GPU-{gpu[:8]} in {took:.2f}s")
    return entries


def correctness(a, api, ch, pods):
    # GPU list = the registry.
    code, d, err = api.get("/gpus")
    if check("GET /gpus returned", code == 200 and d is not None, f"status {code} {err or ''}"):
        want = set(ch.q(pods[0], "SELECT concat('GPU-', toString(gpu_uuid)) FROM gpus FINAL").split())
        got = {g["id"] for g in d["gpus"]}
        check(f"GET /gpus lists exactly the registry ({len(got)} GPUs)", got == want and d["count"] == len(got) and not d["truncated"],
              f"API {len(got)}, ClickHouse {len(want)}, missing {len(want - got)}, extra {len(got - want)}")

    for s, pod in enumerate(pods):
        gpu = busiest_gpu(ch, pod)
        if not gpu:
            out("FAIL", f"shard {s}: has telemetry", "no rows")
            continue
        entries = full_history_matches(api, ch, pods[0], gpu, f"shard {s}")
        if not entries or len(entries) < 20:
            continue
        # An inclusive window with exact stored timestamps as both bounds.
        lo, hi = entries[len(entries) // 4]["timestamp"], entries[3 * len(entries) // 4]["timestamp"]
        lo_ns, hi_ns = ns(lo), ns(hi)
        code, w, err = api.get(f"/gpus/GPU-{gpu}/telemetry", {"start_time": lo, "end_time": hi})
        local = [e for e in entries if lo_ns <= ns(e["timestamp"]) <= hi_ns]
        want = ch_count(ch, pods[0], gpu, lo_ns, hi_ns)
        if check(f"shard {s}: window returned", code == 200 and w is not None, f"status {code} {err or ''}"):
            check(f"shard {s}: inclusive window equals ClickHouse ({len(w['telemetry'])} entries)",
                  w["telemetry"] == local and len(local) == want and ns(w["telemetry"][0]["timestamp"]) == lo_ns
                  and ns(w["telemetry"][-1]["timestamp"]) == hi_ns,
                  f"API {len(w['telemetry'])}, from the full history {len(local)}, ClickHouse {want}")
        # limit takes the earliest entries; truncated says whether more matched.
        code, l5, _ = api.get(f"/gpus/GPU-{gpu}/telemetry", {"limit": 5})
        check(f"shard {s}: limit=5 gives the earliest 5, truncated",
              code == 200 and l5["telemetry"] == entries[:5] and l5["truncated"], f"status {code}")
        code, la, _ = api.get(f"/gpus/GPU-{gpu}/telemetry", {"limit": len(entries)})
        check(f"shard {s}: limit equal to the total is not truncated",
              code == 200 and la["count"] == len(entries) and not la["truncated"], f"status {code}")

    for name, path, params, want in [
        ("unknown GPU is 404", "/gpus/GPU-00000000-0000-0000-0000-000000000000/telemetry", None, 404),
        ("bad id is 400", "/gpus/not-a-gpu/telemetry", None, 400),
        ("bad time is 400", "/gpus/GPU-00000000-0000-0000-0000-000000000000/telemetry", {"start_time": "yesterday"}, 400),
        ("limit=0 is 400", "/gpus", {"limit": 0}, 400),
        ("unknown parameter is 400", "/gpus", {"cursor": "x"}, 400),
    ]:
        code, d, _ = api.get(path, params)
        check(name, code == want and d is not None and "error" in d, f"status {code}")

    code, body = api.raw("/swagger.json")
    with open(a.swagger, "rb") as f:
        check("swagger.json served equals the committed file", code == 200 and body == f.read(), f"status {code}")
    code, _ = api.raw("/docs")
    check("Swagger UI served", code == 200, f"status {code}")


def chunks(a, api, ch, pods):
    best, gpu = 0, None
    for pod in pods:
        g = busiest_gpu(ch, pod)
        n = ch_count(ch, pods[0], g) if g else 0
        if n > best:
            best, gpu = n, g
    if not check(f"a GPU with more than {a.min_rows} rows exists", best > a.min_rows, f"largest has {best}"):
        return
    full_history_matches(api, ch, pods[0], gpu, f"chunks of {a.chunk_rows}")


class Readers:
    """Concurrent API readers: half GPU lists, half recent telemetry windows."""

    def __init__(self, api, threads):
        self.api, self.threads = api, threads
        self.lat, self.codes, self.errors = [], {}, []
        self.mu, self.stop = threading.Lock(), threading.Event()
        code, d, _ = api.get("/gpus")
        self.gpus = [g["id"] for g in d["gpus"]] if code == 200 else []

    def run(self):
        rnd = random.Random()
        while not self.stop.is_set():
            if rnd.random() < 0.5 or not self.gpus:
                path, params = "/gpus", None
            else:
                start = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 60))
                path, params = f"/gpus/{rnd.choice(self.gpus)}/telemetry", {"start_time": start}
            t0 = time.time()
            code, _, err = self.api.get(path, params, timeout=30)
            with self.mu:
                self.lat.append(time.time() - t0)
                self.codes[code] = self.codes.get(code, 0) + 1
                if err or code >= 500 or code == 0:
                    self.errors.append(f"{code} {err or ''}"[:120])

    def start(self):
        self.ts = [threading.Thread(target=self.run, daemon=True) for _ in range(self.threads)]
        for t in self.ts:
            t.start()

    def finish(self, label):
        self.stop.set()
        for t in self.ts:
            t.join(60)
        lat = sorted(self.lat)
        p = lambda q: lat[min(len(lat) - 1, int(q * len(lat)))] * 1000 if lat else 0
        out("REPORT", f"{label}: {len(lat)} requests by {self.threads} readers; p50 {p(0.5):.0f} ms, p95 {p(0.95):.0f} ms; "
                      f"status counts {dict(sorted(self.codes.items()))}")
        return self.errors


def load(a, api, ch, pods):
    r = Readers(api, a.threads)
    r.start()
    time.sleep(a.seconds)
    errors = r.finish("load")
    check("no 5xx or connection errors under load", not errors, f"{len(errors)}: {errors[:3]}")
    check("readers were served", r.codes.get(200, 0) > 0, f"status counts {r.codes}")


def rollout(a, api, ch, pods):
    r = Readers(api, a.threads)
    r.start()
    time.sleep(5)
    subprocess.run(["kubectl", "-n", a.ns, "rollout", "restart", "deploy/api-gateway"], capture_output=True, timeout=30)
    done = subprocess.run(["kubectl", "-n", a.ns, "rollout", "status", "deploy/api-gateway", "--timeout=180s"],
                          capture_output=True, timeout=200)
    time.sleep(10)  # through the old pod's drain
    errors = r.finish("rollout")
    check("the rollout completed", done.returncode == 0, done.stderr.decode()[:200])
    check("no failed request during the gateway's rolling restart", not errors, f"{len(errors)}: {errors[:3]}")


def eventually_full(api, ch, count_pod, gpu, seconds):
    """Retries a full-history read until it is complete and right.
    Returns (ok, attempts, seconds, failures), failures counting each failed attempt by its status
    (0 for a broken transfer, "short" for a 200 that didn't match ClickHouse)."""
    want, t0, attempts, failures = ch_count(ch, count_pod, gpu), time.time(), 0, {}
    while time.time() - t0 < seconds:
        attempts += 1
        code, d, err = api.get(f"/gpus/GPU-{gpu}/telemetry")
        if code == 200 and d and d["count"] == len(d["telemetry"]) == want:
            return True, attempts, time.time() - t0, failures
        k = "short" if code == 200 else code
        failures[k] = failures.get(k, 0) + 1
        time.sleep(1)
    return False, attempts, time.time() - t0, failures


def replica(a, api, ch, pods):
    """One replica of the GPU's shard is gone: reads go to the other replica."""
    ok, attempts, took, failures = eventually_full(api, ch, a.count_pod, a.gpu, 60)
    check(f"with {a.killed} gone, a full history read is complete and equals ClickHouse", ok,
          f"{attempts} attempts in {took:.0f}s, failed attempts by status {failures}")
    out("REPORT", f"replica down: a correct full read after {attempts} attempt(s), {took:.1f}s; "
                  f"failed attempts by status {failures or 'none'}")


def shard_down(a, api, ch, pods):
    """Both replicas of one shard are down: its GPUs get a JSON 5xx, the other shard's are served."""
    code, d, err = api.get(f"/gpus/GPU-{a.gpu_up}/telemetry", {"limit": 10})
    check("a GPU on the healthy shard is still served", code == 200, f"status {code} {err or ''}")
    t0, code, d = time.time(), 200, None
    while time.time() - t0 < 60:
        code, d, err = api.get(f"/gpus/GPU-{a.gpu_down}/telemetry", {"limit": 10})
        if code != 200:
            break
        time.sleep(2)
    check("a GPU on the downed shard gets a JSON 503 or 504",
          code in (503, 504) and d is not None and "error" in d and "request_id" in d, f"status {code} body {d}")


def shard_up(a, api, ch, pods):
    t0 = time.time()
    code = 0
    while time.time() - t0 < 180:
        code, d, err = api.get(f"/gpus/GPU-{a.gpu_down}/telemetry", {"limit": 10})
        if code == 200:
            break
        time.sleep(3)
    check("the shard's GPUs are served again once it is back", code == 200, f"status {code} after {time.time() - t0:.0f}s")
    out("REPORT", f"shard back: served again after {time.time() - t0:.0f}s")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("check", choices=["correctness", "chunks", "load", "rollout", "replica", "shard-down", "shard-up"])
    p.add_argument("--api", required=True)
    p.add_argument("--ns", required=True)
    p.add_argument("--ch-pods", required=True, help="one ClickHouse pod per shard, comma-separated")
    p.add_argument("--swagger", required=True)
    p.add_argument("--min-rows", type=int, default=500)
    p.add_argument("--chunk-rows", type=int, default=500)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--gpu", default="")
    p.add_argument("--gpu-up", default="")
    p.add_argument("--gpu-down", default="")
    p.add_argument("--killed", default="")
    p.add_argument("--count-pod", default="")
    a = p.parse_args()
    if not a.api:
        out("FAIL", "the API is reachable", "no api-gateway-nodeport Service (make deploy SERVICE=api-gateway)")
        return 1
    pods = [x for x in a.ch_pods.split(",") if x]
    fn = {"correctness": correctness, "chunks": chunks, "load": load, "rollout": rollout,
          "replica": replica, "shard-down": shard_down, "shard-up": shard_up}[a.check]
    fn(a, API(a.api), CH(a.ns), pods)
    return 0


if __name__ == "__main__":
    sys.exit(main())
