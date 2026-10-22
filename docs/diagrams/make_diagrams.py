#!/usr/bin/env python3
"""Draws the README diagrams as SVG and renders them to PNG with rsvg-convert.

    python3 docs/diagrams/make_diagrams.py        # writes docs/diagrams/*.png

Style matches the earlier design diagrams: warm grey for components outside the
broker, green for the broker, blue for ClickHouse, amber for warnings and
states. Solid arrows carry data, dashed arrows are control or acks.
"""
import os
import subprocess
from xml.sax.saxutils import escape

HERE = os.path.dirname(os.path.abspath(__file__))
FONT = "Liberation Sans, Arial, Helvetica, sans-serif"

KINDS = {  # fill, stroke, text
    "ext": ("#F1EFEA", "#6B6B66", "#3D3D3A"),
    "core": ("#E1F3EC", "#0F6E56", "#0B5A44"),
    "db": ("#E6EEF8", "#2F5E9E", "#1F3F6E"),
    "warn": ("#FBF0DC", "#A86B12", "#6E460B"),
    "plain": ("#FFFFFF", "#9A9A94", "#3D3D3A"),
    "muted": ("#F6F6F4", "#C4C4BF", "#7A7A75"),
}
ARROW = "#8A8A85"


class Svg:
    def __init__(self, w, h):
        self.w, self.h, self.parts = w, h, []

    def add(self, s):
        self.parts.append(s)

    def text(self, x, y, s, size=18, color="#3D3D3A", anchor="middle", weight="normal", italic=False):
        style = ' font-style="italic"' if italic else ""
        for i, line in enumerate(s.split("\n")):
            self.add(f'<text x="{x}" y="{y + i * size * 1.3}" font-family="{FONT}" font-size="{size}" '
                     f'fill="{color}" text-anchor="{anchor}" font-weight="{weight}"{style}>{escape(line)}</text>')

    def box(self, x, y, w, h, label, sub=None, kind="ext", size=20, subsize=15, r=12):
        fill, stroke, tc = KINDS[kind]
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="1.6"/>')
        lines = label.count("\n") + 1
        sublines = sub.count("\n") + 1 if sub else 0
        block = lines * size * 1.25 + (sublines * subsize * 1.3 + 6 if sub else 0)
        ty = y + (h - block) / 2 + size * 0.95
        self.text(x + w / 2, ty, label, size, tc)
        if sub:
            self.text(x + w / 2, ty + lines * size * 1.25 + 4, sub, subsize, tc)

    def group(self, x, y, w, h, label, size=20):
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="18" fill="none" stroke="#C8C8C4" '
                 f'stroke-width="1.6" stroke-dasharray="8 6"/>')
        self.text(x + w / 2, y + 32, label, size, "#3D3D3A")

    def path(self, pts, label=None, lx=None, ly=None, dashed=False, color=ARROW, size=15, anchor="middle", head=True, width=2.4):
        d = "M" + " L".join(f"{px},{py}" for px, py in pts)
        dash = ' stroke-dasharray="9 7"' if dashed else ""
        marker = ' marker-end="url(#ah)"' if head else ""
        self.add(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}"{dash}{marker}/>')
        if label:
            if lx is None:
                (x1, y1), (x2, y2) = pts[0], pts[1]
                lx, ly = (x1 + x2) / 2, min(y1, y2) - 10
            self.text(lx, ly, label, size, "#55554F", anchor)

    def arrow(self, x1, y1, x2, y2, label=None, **kw):
        self.path([(x1, y1), (x2, y2)], label, **kw)

    def save(self, name):
        svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" viewBox="0 0 {self.w} {self.h}">'
               f'<defs><marker id="ah" viewBox="0 0 12 12" refX="10" refY="6" markerWidth="12" markerHeight="12" '
               f'markerUnits="userSpaceOnUse" orient="auto"><path d="M1,1 L10,6 L1,11" fill="none" stroke="{ARROW}" '
               f'stroke-width="2.2"/></marker></defs><rect width="100%" height="100%" fill="#FFFFFF"/>'
               + "".join(self.parts) + "</svg>")
        svg_path = os.path.join(HERE, name + ".svg")
        png_path = os.path.join(HERE, name + ".png")
        with open(svg_path, "w") as f:
            f.write(svg)
        subprocess.run(["rsvg-convert", "-w", str(self.w * 2), "-o", png_path, svg_path], check=True)
        os.remove(svg_path)
        print("wrote", os.path.relpath(png_path))


def legend(s, y, items=(("solid", "data"), ("dashed", "control, acks"))):
    x = 60
    for style, text in items:
        s.arrow(x, y, x + 50, y, dashed=(style == "dashed"))
        s.text(x + 62, y + 5, text, 15, "#55554F", "start")
        x += 220


def sequence(s, parts, top, bottom, msgs, kinds=None):
    """parts: [(x, label)], msgs: [(y, i_from, i_to, label, dashed)] or (y, i, 'note', text)."""
    for i, (x, label) in enumerate(parts):
        kind = (kinds or {}).get(i, "ext")
        s.box(x - 105, top, 210, 64, label, kind=kind, size=18)
        s.add(f'<line x1="{x}" y1="{top + 64}" x2="{x}" y2="{bottom}" stroke="#C8C8C4" stroke-width="1.4" stroke-dasharray="6 6"/>')
    for m in msgs:
        y = m[0]
        if m[2] == "note":
            x = parts[m[1]][0]
            s.text(x + 14, y, m[3], 14, "#7A7A75", "start", italic=True)
            continue
        _, a, b, label, dashed = m
        xa, xb = parts[a][0], parts[b][0]
        off = 6 if xb > xa else -6
        s.arrow(xa + off, y, xb - off, y, label, dashed=dashed, lx=(xa + xb) / 2, ly=y - 10)


# ---------------------------------------------------------------- diagrams


def pipeline_overview():
    s = Svg(1500, 720)
    s.text(750, 46, "Telemetry pipeline: who talks to whom", 24, weight="bold")
    s.box(40, 150, 230, 110, "Streamers ×0–10", "StatefulSet\nreplay the DCGM CSV", "ext")
    s.text(155, 286, "one broker at a time;\nfail over to the next", 13, "#7A7A75")
    for off in (16, 8):  # the other brokers: independent copies of the one drawn
        s.add(f'<rect x="{330 + off}" y="{90 + off}" width="400" height="400" rx="18" fill="#FAFAF8" '
              f'stroke="#D8D8D4" stroke-width="1.4" stroke-dasharray="8 6"/>')
    s.add('<rect x="330" y="90" width="400" height="400" rx="18" fill="#FFFFFF" stroke="none"/>')
    s.group(330, 90, 400, 400, "Brokers ×3: independent, a PVC each")
    s.box(360, 140, 340, 64, "Ingest: partition, dedupe", kind="core", size=17)
    s.box(360, 220, 340, 84, "Write-ahead log", "10 partitions · tidwall/wal\nfsync every 20 ms", "core", size=18)
    s.box(360, 320, 160, 64, "Coordinator", kind="core", size=17)
    s.box(540, 320, 160, 64, "Fetch", kind="core", size=17)
    s.box(360, 398, 340, 40, "HTTP :8080  /lag  /metrics", kind="muted", size=15)
    s.box(820, 150, 230, 110, "Collectors ×1–10", "Deployment, read every broker\nbatch, insert, commit", "ext")
    s.box(1150, 130, 310, 150, "ClickHouse", "2 shards × 2 replicas\n3 Keepers\ntables telemetry, gpus", "db")
    s.arrow(270, 190, 358, 172, "records")
    s.arrow(358, 238, 272, 238, "acks\nafter fsync", dashed=True, lx=315, ly=258)
    s.path([(702, 352), (760, 352), (760, 200), (818, 200)], "frames ≤ HW", lx=768, ly=190)
    s.path([(818, 245), (790, 245), (790, 466), (345, 466), (345, 352), (358, 352)], dashed=True)
    s.text(568, 484, "join · heartbeat · commit", 15, "#55554F")
    s.arrow(1052, 205, 1148, 205, "insert", lx=1100, ly=195)
    s.text(1100, 228, "insert-only\nuser", 13, "#7A7A75")
    s.box(1150, 330, 230, 90, "API gateway", "REST /api/v1 · Swagger UI\nread-only user", "ext", size=19, subsize=14)
    s.arrow(1265, 328, 1265, 284, "queries", lx=1312, ly=312)
    s.arrow(1265, 478, 1265, 424, "REST clients", lx=1265, ly=496)
    s.box(820, 520, 230, 80, "KEDA", "metrics-api scaler", "ext")
    s.path([(702, 418), (760, 418), (760, 560), (818, 560)], "reads each /lag", dashed=True, lx=712, ly=545)
    s.arrow(935, 518, 935, 262, "scales 1–10", dashed=True, lx=990, ly=470)
    s.box(1150, 520, 310, 80, "Prometheus", "monitoring namespace", "ext")
    s.path([(1440, 518), (1440, 282)], dashed=True)
    s.text(1448, 452, "scrapes", 15, "#55554F", "start")
    s.text(1448, 471, "all", 15, "#55554F", "start")
    legend(s, 680)
    s.save("pipeline-overview")


def broker_internals():
    s = Svg(1500, 860)
    s.text(750, 46, "Inside the broker", 24, weight="bold")
    s.box(30, 170, 200, 100, "Streamers", "Ingest.Stream\none record/message", "ext")
    s.box(1270, 170, 200, 100, "Collectors", "Fetch, JoinGroup,\nHeartbeat, Commit", "ext")
    s.group(270, 80, 960, 640, "broker-0 (Go process)")
    s.box(300, 130, 300, 190, "Ingest handler", "one per producer stream\n\n1  seq must be prev + 1\n2  drop seq < next[p]\n3  p = hash(uuid) % 10\n4  frame per partition\n    (10 ms or 64 KB)", "core", size=19, subsize=15)
    s.box(650, 130, 260, 270, "Partition logs", "P0 … P9\none tidwall/wal each\n1 MB segments\nNoSync (flusher syncs)\n\nAppend: seal frame\n(record_base, CRC32C)", "core", size=19, subsize=15)
    s.box(300, 360, 300, 110, "Flusher", "every 20 ms: fsync dirty logs\n→ advance high watermark\n→ wake acks and fetches", "core", size=19, subsize=15)
    s.box(960, 130, 240, 110, "Fetch", "push frames ≤ HW\n256 KB per response", "core", size=19, subsize=15)
    s.box(960, 270, 240, 130, "Coordinator", "group, generations\nfenced commits\noffsets.json", "core", size=19, subsize=15)
    s.box(650, 440, 260, 100, "Truncator", "every 1 s: delete\nentries < committed", "core", size=19, subsize=15)
    s.box(960, 440, 240, 100, "Quota", "≥ 4 GiB on disk:\nstop reading streams", "warn", size=19, subsize=15)
    s.box(300, 500, 300, 60, "HTTP :8080  /healthz /readyz /metrics /lag", kind="muted", size=14)
    s.arrow(232, 205, 298, 205, "records")
    s.arrow(298, 250, 232, 250, "Ack{seq}", dashed=True, lx=265, ly=290)
    s.arrow(600, 225, 648, 225)
    s.arrow(602, 385, 648, 385, "fsync", dashed=True, lx=625, ly=376)
    s.arrow(450, 358, 450, 322, "HW", dashed=True, lx=480, ly=345)
    s.arrow(910, 185, 958, 185)
    s.arrow(1202, 185, 1268, 185, "frames")
    s.arrow(1268, 245, 1202, 300, "commit", dashed=True, lx=1250, ly=300)
    s.path([(958, 380), (935, 380), (935, 490), (912, 490)], dashed=True)
    s.text(1080, 428, "committed offsets", 14, "#55554F")
    s.arrow(780, 438, 780, 402, dashed=True)
    s.group(300, 600, 900, 100, "PVC /data", size=17)
    s.text(750, 670, "topic.json (partitions, hash)  ·  wal/0 … wal/9 (segment files)  ·  offsets.json (committed offsets)", 16, "#55554F")
    legend(s, 790)
    s.save("broker-internals")


def wal_offsets():
    s = Svg(1500, 640)
    s.text(750, 46, "One partition's log: what each offset means", 24, weight="bold")
    x0, y0, h = 80, 200, 70
    regions = [  # width, fill, label
        (230, "#F6F6F4", "deleted\n(truncated)"),
        (330, "#E7E7E3", "consumed, committed\nwaiting for the truncator"),
        (330, "#E1F3EC", "durable, fetched or\nwaiting for a collector"),
        (250, "#FBF0DC", "written, not fsynced\n(never acked)"),
    ]
    x = x0
    for w, fill, label in regions:
        dash = ' stroke-dasharray="7 6"' if fill == "#F6F6F4" else ""
        s.add(f'<rect x="{x}" y="{y0}" width="{w}" height="{h}" fill="{fill}" stroke="#9A9A94" stroke-width="1.4"{dash}/>')
        s.text(x + w / 2, y0 + 30, label, 15, "#3D3D3A")
        x += w
    marks = [(x0 + 230, "first offset", "oldest entry on disk"),
             (x0 + 560, "committed", "next offset the group reads"),
             (x0 + 890, "high watermark (HW)", "last fsynced entry"),
             (x0 + 1140, "last", "last written entry")]
    for i, (mx, name, desc) in enumerate(marks):
        s.add(f'<line x1="{mx}" y1="{y0 - 50}" x2="{mx}" y2="{y0 + h + 10}" stroke="#0F6E56" stroke-width="2.4"/>')
        s.text(mx, y0 - 78, name, 17, "#0B5A44", weight="bold")
        s.text(mx, y0 - 58, desc, 14, "#55554F")
    s.arrow(x0 + 1140, y0 + h + 40, x0 + 1220, y0 + h + 40, "appends", lx=x0 + 1180, ly=y0 + h + 30)
    notes = [
        "• Producers are acked only for records at or below HW (the flusher fsyncs every 20 ms, then acks).",
        "• Fetch never returns anything above HW, so a consumer never sees data a crash could remove.",
        "• A collector commits after its ClickHouse insert; within ~1 s the truncator deletes everything below committed.",
        "• On restart, a torn or zero-filled tail above HW is cut: it was never acked, so the producer resends it.",
        "• Offsets are WAL entry indexes (one entry = one frame); each frame header carries record_base for lag in records.",
    ]
    for i, n in enumerate(notes):
        s.text(80, 400 + i * 32, n, 16, "#3D3D3A", "start")
    s.text(750, 600, "segment files: wal/<p>/00000000000000000001 … (1 MB each; the active segment is held in memory)", 15, "#7A7A75", italic=True)
    s.save("wal-offsets")


def produce_ack_sequence():
    s = Svg(1560, 940)
    s.text(780, 40, "From a streamer's record to deleted WAL space", 24, weight="bold")
    parts = [(120, "Streamer"), (340, "Ingest handler"), (560, "Partition log"), (780, "Flusher"),
             (1000, "Collector"), (1220, "ClickHouse"), (1440, "Coordinator")]
    kinds = {1: "core", 2: "core", 3: "core", 5: "db", 6: "core"}
    msgs = [
        (185, 0, 1, "Record seq=101 (gpu A)", False),
        (215, 1, "note", "seq contiguous · not a retry\nhash(A) % 10 = 3 → buffer P3"),
        (275, 1, 2, "Write(frame) after 10 ms", False),
        (310, 2, "note", "page cache, not durable yet"),
        (365, 3, 2, "fsync", True),
        (420, 3, 1, "high watermark advanced", True),
        (475, 1, 0, "Ack{101}", True),
        (510, 0, "note", "frees its window"),
        (575, 4, 2, "Fetch(P3, offset)", False),
        (630, 2, 4, "frames ≤ HW", True),
        (690, 4, 5, "insert block (5,000 rows or 1 s)", False),
        (745, 5, 4, "ok", True),
        (805, 4, 6, "CommitOffset(P3, next) after the insert", False),
        (860, 6, 2, "truncate below committed (≤ 1 s)", True),
    ]
    sequence(s, parts, 80, 900, msgs, kinds)
    s.save("produce-ack-sequence")


def coordinator_states():
    s = Svg(1500, 500)
    s.text(750, 46, "Consumer group states (one group: collectors)", 24, weight="bold")
    s.box(80, 120, 220, 90, "Empty", "no members", "ext")
    s.box(560, 120, 330, 90, "PreparingRebalance", "members must rejoin", "warn")
    s.box(1150, 120, 240, 90, "Stable", "partitions assigned", "core")
    s.arrow(302, 150, 558, 150, "first JoinGroup")
    s.arrow(558, 190, 302, 190, "last member leaves", dashed=True, lx=430, ly=220)
    s.arrow(892, 150, 1148, 150, "join window over and all rejoined", lx=1020, ly=140)
    s.text(1020, 182, "(or 15 s rebalance timeout)", 14, "#7A7A75")
    s.path([(1270, 212), (1270, 280), (725, 280), (725, 214)], dashed=True)
    s.text(1000, 304, "a member joins, leaves (SIGTERM) or misses heartbeats for 10 s", 15, "#55554F")
    notes = [
        "• Entering Stable: generation + 1, members sorted, partition p → members[p % n].",
        "• While preparing, heartbeats return REBALANCE_IN_PROGRESS; members finish their insert, commit, and rejoin.",
        "• Commits from the current generation are still accepted while preparing; older generations are fenced.",
        "• Membership is in memory: after a broker restart members get UNKNOWN_MEMBER and join again.",
    ]
    for i, n in enumerate(notes):
        s.text(80, 370 + i * 28, n, 16, "#3D3D3A", "start")
    s.save("coordinator-states")


def crash_recovery():
    s = Svg(1500, 620)
    s.text(750, 46, "Broker crash: what recovery does, and how clients resume", 24, weight="bold")
    steps = [
        ("Open each\npartition log", "core"), ("Cut a torn\nlast entry", "warn"), ("Drop trailing\nentries that fail\nthe frame CRC", "warn"),
        ("HW = last entry\nrecord_base from\nits header", "core"), ("Rebuild dedupe\ntable from last\n1,024 entries", "core"), ("Load\noffsets.json\n→ Ready", "core"),
    ]
    x = 40
    for i, (label, kind) in enumerate(steps):
        s.box(x, 110, 210, 120, label, kind=kind, size=17)
        if i < len(steps) - 1:
            s.arrow(x + 212, 170, x + 238, 170)
        x += 240
    s.box(40, 320, 680, 130, "Streamers", "stream breaks → reconnect with backoff (100 ms – 5 s)\nresend every unacked record, same seq and timestamps\nretries already on disk are dropped (seq < next[partition])", "ext", size=19, subsize=15)
    s.box(780, 320, 680, 130, "Collectors", "heartbeat says UNKNOWN_MEMBER → join as new members\nresume from committed offsets in offsets.json\nre-read uncommitted rows; the ClickHouse key removes repeats", "ext", size=19, subsize=15)
    s.path([(1345, 232), (1345, 280), (380, 280), (380, 318)], dashed=True)
    s.arrow(1120, 280, 1120, 318, dashed=True)
    s.text(750, 272, "once Ready, clients resume", 15, "#55554F")
    s.text(750, 510, "Nothing a producer was acked for is lost: acks only follow an fsync.  Measured in minikube: back to accepting", 16, "#3D3D3A")
    s.text(750, 536, "records 7 s after SIGKILL and 12 s after a graceful delete, with acked = stored for every row.", 16, "#3D3D3A")
    s.text(750, 580, "Not covered: losing the PVC or node (single copy).", 15, "#A86B12", italic=True)
    s.save("crash-recovery")


def collector_flow():
    s = Svg(1500, 760)
    s.text(750, 46, "Inside a collector", 24, weight="bold")
    s.box(40, 150, 220, 110, "Broker", "Fetch streams\nCoordinator", "core")
    s.group(300, 90, 860, 560, "collector pod")
    s.box(330, 140, 230, 140, "Fetch loops", "one per owned\npartition, from the\ncommitted offset", "ext", size=19, subsize=15)
    s.box(600, 140, 230, 140, "Decode", "frame CRC32C\nrecords → rows\n+ streamer_id", "ext", size=19, subsize=15)
    s.box(870, 140, 260, 140, "Block", "5,000 rows, or 1 s,\nor 8 MB\n(fetching pauses\nwhile inserting)", "ext", size=19, subsize=15)
    s.box(870, 330, 260, 130, "Insert", "one native batch\nretry 100 ms – 5 s\nuntil it succeeds", "ext", size=19, subsize=15)
    s.box(600, 330, 230, 130, "Commit", "next offset per\npartition, only\nafter the insert", "ext", size=19, subsize=15)
    s.box(870, 510, 260, 110, "Registry", "new GPUs, and\nlast_seen every 60 s", "muted", size=19, subsize=15)
    s.box(1210, 330, 250, 130, "ClickHouse", "telemetry\ngpus", "db")
    s.arrow(262, 205, 328, 205, "frames")
    s.arrow(562, 210, 598, 210)
    s.arrow(832, 210, 868, 210)
    s.arrow(1000, 282, 1000, 328)
    s.arrow(1132, 395, 1208, 395, "rows")
    s.arrow(868, 395, 832, 395, "ok", dashed=True, lx=850, ly=385)
    s.path([(598, 395), (150, 395), (150, 262)], "CommitOffset", dashed=True, lx=420, ly=385)
    s.arrow(1000, 462, 1000, 508, "GPUs seen", dashed=True, lx=1050, ly=490)
    s.path([(1132, 565), (1335, 565), (1335, 462)], "INSERT INTO gpus", lx=1235, ly=555)
    notes = "Rebalance or SIGTERM: stop fetching, insert and commit what is buffered, then rejoin (or leave). Undecodable batch: skip and count (or stop)."
    s.text(750, 710, notes, 15, "#55554F")
    s.save("collector-flow")


def keda_scaling():
    s = Svg(1500, 640)
    s.text(750, 46, "How collectors scale on queue lag", 24, weight="bold")
    s.box(60, 140, 260, 120, "Each broker's /lag", "records not committed\nsum over its partitions", "core")
    s.box(420, 140, 280, 120, "KEDA", "one trigger per broker\npolls every 15 s\n(webhooks off)", "ext")
    s.box(800, 140, 300, 120, "HPA", "largest of ⌈lag_b / (10,000 / brokers)⌉\nbetween 1 and 10", "ext", subsize=14)
    s.box(1180, 140, 260, 120, "collector\nDeployment", kind="ext")
    s.box(1180, 380, 260, 130, "New / removed pod", "JoinGroup or\nLeaveGroup", "ext")
    s.box(800, 380, 300, 130, "Each broker's coordinator", "rebalance:\npartition p → members[p % n]", "core", size=18)
    s.box(420, 380, 280, 130, "Lag drains", "more members fetch\nmore partitions\nin parallel", "core")
    s.arrow(322, 200, 418, 200, "lag (JSON)")
    s.arrow(702, 200, 798, 200, "metric")
    s.arrow(1102, 200, 1178, 200, "replicas")
    s.arrow(1310, 262, 1310, 378)
    s.arrow(1178, 445, 1102, 445)
    s.arrow(798, 445, 702, 445)
    s.path([(418, 445), (190, 445), (190, 262)], dashed=True)
    rules = ("Scale up ≤ 2 pods / 60 s  ·  scale down 1 pod / 120 s after a 300 s window  ·  3 failed reads of /lag → hold 2 replicas\n"
             "Each change causes a short rebalance pause, which is why scale-down is slow. The e2e 'scale' case speeds it up temporarily.")
    s.text(750, 570, rules, 16, "#3D3D3A")
    s.save("keda-scaling")


def streamer_flow():
    s = Svg(1500, 620)
    s.text(750, 46, "Inside a streamer", 24, weight="bold")
    s.box(40, 150, 210, 120, "CSV", "baked into the image\nrandom start row", "ext")
    s.box(300, 150, 230, 120, "Pacer", "every 10 ms emit the\nrows accrued at the rate\nstamp ts, seq += 1", "ext", size=19, subsize=15)
    s.box(580, 150, 230, 120, "Unacked window", "3 s of rows\n(≤ 60,000 records)", "ext", size=19, subsize=15)
    s.box(860, 150, 230, 120, "gRPC stream", "Ingest.Stream\nx-streamer-id, x-epoch", "ext", size=19, subsize=15)
    s.box(1150, 150, 300, 120, "Home broker", "streamer_id mod brokers\nacks after fsync", "core")
    s.arrow(252, 210, 298, 210)
    s.arrow(532, 210, 578, 210)
    s.arrow(812, 210, 858, 210)
    s.arrow(1092, 195, 1148, 195, "records", lx=1120, ly=185)
    s.path([(1148, 240), (1120, 240), (1120, 300), (695, 300), (695, 272)], "Ack{seq}: free the window", dashed=True, lx=910, ly=325)
    s.box(300, 380, 510, 110, "Backpressure", "only when no broker takes writes: window full → the\npacer blocks (streamer_backpressure_blocked_seconds_total)", "warn", size=19, subsize=15)
    s.box(860, 380, 590, 110, "Broker lost (error, or no ack for 2 s)", "fail over to the next broker at once, without pausing, and resend\nevery unacked record in order; back off only if every broker failed", "warn", size=19, subsize=15)
    s.text(750, 560, "Identity: streamer_id = pod ordinal (0–10), epoch = random per start. A broker drops retries by (streamer_id, epoch, partition, seq). One active stream at a time.", 15, "#55554F")
    s.save("streamer-flow")


def monitoring_map():
    s = Svg(1500, 700)
    s.text(750, 46, "Where the metrics come from", 24, weight="bold")
    s.box(560, 290, 380, 120, "Prometheus", "namespace monitoring\nscrape every 30 s · 24 h retention", "ext")
    sources = [
        (60, 110, "kubelet / cAdvisor", "pod CPU and memory\n(recording rules add\ncontainer=\"pod\")"),
        (60, 290, "kube-state-metrics", "replicas, HPA,\nrestarts, OOM kills"),
        (60, 470, "node-exporter", "DaemonSet, one per node\nnode CPU, memory, disk"),
        (1100, 70, "brokers :8080", "acks, fsync, lag,\ndisk, group"),
        (1100, 180, "collectors :9090", "inserts, commits,\nbuffer"),
        (1100, 290, "streamers :9100", "emitted, acked, failovers,\nblocked time"),
        (1100, 400, "API gateway :9090", "requests, query latency,\n429s, stream aborts"),
        (1100, 510, "KEDA :8080", "the lag value it read"),
    ]
    for x, y, label, sub in sources:
        h = 110 if x < 500 else 96
        s.box(x, y, 340, h, label, sub, "ext", size=18, subsize=14)
        if x < 500:
            s.arrow(x + 342, y + h / 2, 558, 350, dashed=True)
        else:
            s.arrow(x - 2, y + h / 2, 942, 350, dashed=True)
    s.box(560, 80, 380, 110, "ClickHouse :9363, Keeper :7000", "via their metrics Services\nrows stored per shard", "db", size=18, subsize=14)
    s.arrow(750, 192, 750, 288, dashed=True)
    s.box(420, 520, 220, 80, "OpenLens", "pod and node charts", "plain", size=18, subsize=14)
    s.box(660, 520, 220, 80, "Prometheus UI", "PromQL", "plain", size=18, subsize=14)
    s.box(900, 520, 160, 80, "e2e suite", "reports", "plain", size=18, subsize=14)
    s.arrow(700, 412, 560, 518)
    s.arrow(750, 412, 770, 518)
    s.arrow(800, 412, 960, 518)
    s.text(750, 660, "metrics-server (minikube addon) is separate: it feeds kubectl top and HPA CPU, not Prometheus.", 15, "#7A7A75", italic=True)
    s.save("monitoring-map")


def clickhouse_topology():
    s = Svg(1500, 860)
    s.text(750, 46, "ClickHouse in the dev profile: 2 shards × 2 replicas", 24, weight="bold")
    s.box(40, 150, 230, 100, "Collectors", "user collector\n(INSERT only)", "ext")
    s.box(40, 300, 230, 100, "API (reads)", "user api\n(read-only, capped)", "ext")
    s.box(340, 210, 300, 120, "Service", "clickhouse-telemetry-store\n:9000 native · :8123 HTTP", "db", size=19, subsize=15)
    s.arrow(272, 200, 338, 250)
    s.arrow(272, 350, 338, 300)
    s.box(700, 190, 260, 160, "Distributed table", "telemetry, gpus\nshard = cityHash64(gpu_uuid) % 2\ninsert_distributed_sync = 1", "db", size=19, subsize=14)
    s.arrow(642, 270, 698, 270, "any pod")
    s.group(1020, 90, 440, 250, "shard 0", size=18)
    s.box(1040, 140, 190, 180, "replica 0", "telemetry_local\nReplicated\nReplacingMergeTree", "db", size=17, subsize=13)
    s.box(1250, 140, 190, 180, "replica 1", "same parts\n(fetched from\nreplica 0)", "db", size=17, subsize=13)
    s.group(1020, 370, 440, 250, "shard 1", size=18)
    s.box(1040, 420, 190, 180, "replica 0", "telemetry_local\nReplicated\nReplacingMergeTree", "db", size=17, subsize=13)
    s.box(1250, 420, 190, 180, "replica 1", "same parts\n(fetched from\nreplica 0)", "db", size=17, subsize=13)
    s.arrow(962, 240, 1038, 230)
    s.arrow(962, 300, 1038, 510)
    s.box(1020, 660, 440, 80, "Keeper ×3", "replication log · quorum: 2 of 3 needed for writes", "warn", size=19, subsize=14)
    s.arrow(1240, 658, 1240, 622, dashed=True)
    s.path([(1462, 700), (1485, 700), (1485, 215), (1462, 215)], dashed=True)
    notes = ("Sort key (gpu_uuid, ts, metric_name, streamer_id) is also the dedupe key: replays collapse on merge, reads use LIMIT 1 BY.\n"
             "Partitioned by hour, TTL 4 hours in dev (7 days in prod). Inserts wait for the owning shard (insert_distributed_sync), so a collector commits only stored rows.")
    s.text(750, 790, notes, 15, "#3D3D3A")
    s.save("clickhouse-topology")


if __name__ == "__main__":
    for fn in (pipeline_overview, broker_internals, wal_offsets, produce_ack_sequence, coordinator_states,
               crash_recovery, collector_flow, keda_scaling, streamer_flow, monitoring_map, clickhouse_topology):
        fn()
