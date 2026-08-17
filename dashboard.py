#!/usr/bin/env python3
"""
dashboard.py - Live status + topology dashboard for an IronMesh cluster.

Run this on any laptop that's on the same LAN as the mesh (it doesn't need
to be one of the nodes), then open the dashboard in a browser.

    python3 dashboard.py
    python3 dashboard.py --node 192.168.1.20:8080
    python3 dashboard.py --port 9000 --node localhost:8080

Then visit:  http://localhost:9000

How this works
---------------------------------------------------------------------
Rather than trust the "state" and "alive" fields nodes gossip about each
other (node.py never actually fills "state" into the heartbeat, which is
why it always shows "unknown"), this dashboard crawls the mesh itself:
starting from --node, it asks that node's /metrics for its peer list,
then directly asks each of *those* nodes for their own /health + /metrics
too (one hop out, breadth-first), and keeps going until it runs out of
newly-discovered peers or hits the hop/node cap. Every node's real state
comes straight from that node's own /health, not from gossip.

Nodes are tracked by host:port, not by node_id. node_id gets a fresh
random suffix every time node.py restarts (see identity.py), so keying
on it makes a reconnecting laptop look like a brand new node. host:port
is the same laptop's actual network identity across restarts, so a node
that goes down and comes back updates the *same* row/graph point instead
of spawning a duplicate.

A browser page also can't fetch a different origin (like localhost:8080)
without CORS headers, which the node's server doesn't send - so this
script does all the fetching in Python and hands the browser a
same-origin "/api/topology" endpoint to poll instead.

Stdlib only, matching the rest of this project - no installs needed.
"""

from __future__ import annotations

import argparse
import json
import socket
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DEFAULT_NODE = "localhost:8080"
DEFAULT_PORT = 9000
REQUEST_TIMEOUT_S = 1.5
DEFAULT_HOPS = 2
MAX_NODES = 40

_resolve_cache: dict = {}


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _resolve_host(host: str) -> str:
    """Resolve a hostname to an IP so 'localhost' and '127.0.0.1' (or a
    machine's hostname and its LAN IP) collapse to the same graph key."""
    if host in _resolve_cache:
        return _resolve_cache[host]
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host
    _resolve_cache[host] = ip
    return ip


def parse_hostport(addr: str):
    a = addr.strip()
    if a.startswith("http://"):
        a = a[7:]
    elif a.startswith("https://"):
        a = a[8:]
    a = a.rstrip("/")
    if ":" in a:
        host, port = a.rsplit(":", 1)
    else:
        host, port = a, "80"
    return host, port


def normalize_addr(host: str, port) -> str:
    return "{}:{}".format(_resolve_host(str(host)), port)


def probe(addr: str) -> dict:
    """Directly ask one node (host:port) for its own health + metrics."""
    base = addr.strip()
    if not base.startswith("http://") and not base.startswith("https://"):
        base = "http://" + base
    base = base.rstrip("/")

    out: dict = {"ok": False, "addr": addr}
    try:
        out["health"] = fetch_json(base + "/health")
    except Exception as exc:
        out["health_error"] = str(exc)
    try:
        out["metrics"] = fetch_json(base + "/metrics")
        out["ok"] = True
    except Exception as exc:
        out["metrics_error"] = str(exc)
    try:
        out["connections"] = fetch_json(base + "/connections")
    except Exception as exc:
        out["connections_error"] = str(exc)
    return out


def crawl(root_addr: str, hops: int = DEFAULT_HOPS, max_nodes: int = MAX_NODES) -> dict:
    """Breadth-first crawl of the mesh starting at root_addr.

    Returns {"root": addr, "visited": {addr: probe_result}, "edges": [...]}.
    Each edge is one node's own report of a peer it knows about, carrying
    whatever that node's peer table said (alive, rtt_ms, headroom, state,
    fail_count) - this is the freshest first-hand signal available for
    any node we couldn't reach directly ourselves.
    """
    visited: dict = {}
    edges: list = []
    root_key = normalize_addr(*parse_hostport(root_addr))
    frontier = [root_key]
    hop = 0

    while frontier and hop <= hops and len(visited) < max_nodes:
        frontier = [a for a in frontier if a not in visited][: max(0, max_nodes - len(visited))]
        if not frontier:
            break
        with ThreadPoolExecutor(max_workers=min(8, len(frontier))) as ex:
            results = list(ex.map(probe, frontier))

        next_frontier: list = []
        for addr, result in zip(frontier, results):
            visited[addr] = result
            metrics = result.get("metrics") or {}
            for p in metrics.get("peers", []) or []:
                host, port = p.get("host"), p.get("port")
                if not host or not port:
                    continue
                pkey = normalize_addr(host, port)
                edges.append({"a": addr, "b": pkey, "peer": p})
                if pkey not in visited and pkey not in next_frontier:
                    next_frontier.append(pkey)
        frontier = next_frontier
        hop += 1

    return {"root": root_key, "visited": visited, "edges": edges}


def device_name(node_id: str) -> str:
    import re
    if not node_id:
        return ""
    m = re.match(r"^node-(.+)-[0-9a-f]{12}$", node_id, re.IGNORECASE)
    return m.group(1) if m else node_id


def compose_graph(crawl_result: dict) -> dict:
    """Turn a crawl into a flat {nodes, edges, root} graph keyed by host:port."""
    visited = crawl_result["visited"]
    root = crawl_result["root"]
    nodes: dict = {}

    def ensure(addr: str) -> dict:
        if addr not in nodes:
            nodes[addr] = {
                "addr": addr,
                "node_id": "",
                "device_name": "",
                "reachable": False,
                "state": "unknown",
                "headroom": None,
                "load": None,
                "rtt_ms": None,
                "fail_count": None,
                "queue_depth": None,
                "uptime_s": None,
                "connections": [],
            }
        return nodes[addr]

    # Directly-probed nodes: ground truth, straight from their own /health.
    for addr, result in visited.items():
        n = ensure(addr)
        health = result.get("health") or {}
        metrics = result.get("metrics") or {}
        snap = metrics.get("snapshot") or {}
        node_id = metrics.get("node_id") or health.get("node_id") or ""
        n["node_id"] = node_id
        n["device_name"] = device_name(node_id)
        n["reachable"] = bool(result.get("ok"))
        if n["reachable"]:
            n["state"] = health.get("state", "unknown")
            n["headroom"] = snap.get("headroom")
            n["load"] = health.get("load", snap.get("load_score"))
            n["queue_depth"] = health.get("queue_depth", snap.get("queue_depth"))
            n["uptime_s"] = health.get("uptime_s")
            conns = result.get("connections") or {}
            n["connections"] = conns.get("connections") or []

    # Fill in anything only known second-hand (a peer's report of a node
    # we couldn't reach ourselves) - best available signal, clearly weaker.
    for e in crawl_result["edges"]:
        p = e["peer"]
        pkey = e["b"]
        n = ensure(pkey)
        if not n["node_id"] and p.get("node_id"):
            n["node_id"] = p["node_id"]
            n["device_name"] = device_name(p["node_id"])
        if not n["reachable"]:
            if n["headroom"] is None:
                n["headroom"] = p.get("headroom")
            if n["state"] == "unknown" and p.get("state") and p.get("state") != "unknown":
                n["state"] = p.get("state")
            n["reported_alive"] = p.get("alive")
        if n["rtt_ms"] is None and p.get("rtt_ms") is not None:
            n["rtt_ms"] = p.get("rtt_ms")
        if n["fail_count"] is None and p.get("fail_count") is not None:
            n["fail_count"] = p.get("fail_count")

    # Some nodes get discovered under more than one host:port - e.g. two
    # node.py processes running on the same physical laptop (different
    # ports) each mint their own random node_id (see identity.py), but
    # they share the same hostname/device_name. From a "which laptops are
    # on the mesh" point of view that's one machine, so collapse any addrs
    # that share a device_name down to one canonical entry, preferring the
    # root's own addr, then any directly-reachable addr. Nodes with no
    # device_name (couldn't be identified at all) are left alone.
    def _dkey(name: str) -> str:
        return name.strip().casefold()

    canonical_for_device: dict = {}
    for addr, n in nodes.items():
        dname = n.get("device_name")
        if not dname:
            continue
        key = _dkey(dname)
        current = canonical_for_device.get(key)
        if current is None:
            canonical_for_device[key] = addr
        elif addr == root:
            canonical_for_device[key] = addr
        elif n.get("reachable") and not nodes[current].get("reachable") and current != root:
            canonical_for_device[key] = addr

    addr_to_canonical: dict = {}
    for addr, n in nodes.items():
        dname = n.get("device_name")
        addr_to_canonical[addr] = canonical_for_device.get(_dkey(dname), addr) if dname else addr

    merged_nodes: dict = {}
    for addr, n in nodes.items():
        cadd = addr_to_canonical[addr]
        if cadd not in merged_nodes:
            dst = dict(n)
            dst["addr"] = cadd
            merged_nodes[cadd] = dst
            continue
        dst = merged_nodes[cadd]
        if n.get("reachable") and not dst.get("reachable"):
            for k in ("state", "headroom", "load", "queue_depth", "uptime_s", "connections"):
                dst[k] = n.get(k)
            dst["reachable"] = True
        for k in ("rtt_ms", "fail_count", "reported_alive"):
            if dst.get(k) is None and n.get(k) is not None:
                dst[k] = n.get(k)
        if not dst.get("device_name") and n.get("device_name"):
            dst["device_name"] = n["device_name"]

    root = addr_to_canonical.get(root, root)

    # Collapse duplicate edges between the same pair of addrs into one,
    # OR-ing "alive" (if anyone can see it up, treat the link as up), and
    # remap endpoints through the same canonical-address mapping so a
    # merged node doesn't leave behind a stray extra dot in the graph.
    merged_edges: dict = {}
    for e in crawl_result["edges"]:
        a = addr_to_canonical.get(e["a"], e["a"])
        b = addr_to_canonical.get(e["b"], e["b"])
        if a == b:
            continue
        key = tuple(sorted((a, b)))
        alive = bool(e["peer"].get("alive"))
        cur = merged_edges.get(key)
        if cur is None:
            merged_edges[key] = {"a": key[0], "b": key[1], "alive": alive}
        else:
            cur["alive"] = cur["alive"] or alive

    return {
        "root": root,
        "nodes": list(merged_nodes.values()),
        "edges": list(merged_edges.values()),
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # keep the console quiet
        pass

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/":
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return

        if parsed.path == "/api/topology":
            qs = parse_qs(parsed.query)
            node_addr = (qs.get("node") or [self.server.default_node])[0]  # type: ignore[attr-defined]
            try:
                hops = int((qs.get("hops") or [DEFAULT_HOPS])[0])
            except ValueError:
                hops = DEFAULT_HOPS
            result = crawl(node_addr, hops=hops)
            graph = compose_graph(result)
            body = json.dumps(graph).encode("utf-8")
            self._send(200, body, "application/json")
            return

        self._send(404, b"not found", "text/plain")


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>IronMesh - Live Dashboard</title>
<style>
  :root {
    --bg: #14121f;
    --panel: #1c1930;
    --panel-2: #211d3a;
    --border: #34304f;
    --text: #e7e4f5;
    --muted: #8d87ab;
    --accent: #9b7bf0;
    --accent-2: #5eead4;
    --good: #3ddc84;
    --warn: #f5b942;
    --bad: #f0546b;
    --mono: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
    --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: radial-gradient(1200px 600px at 10% -10%, #241f42 0%, var(--bg) 55%);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    padding: 28px 20px 60px;
  }
  .wrap { max-width: 1040px; margin: 0 auto; }

  header {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 10px;
    margin-bottom: 22px;
  }
  .brand { display: flex; align-items: baseline; gap: 10px; }
  .brand .dot {
    width: 9px; height: 9px; border-radius: 50%;
    background: var(--accent-2);
    box-shadow: 0 0 10px var(--accent-2);
    display: inline-block;
    animation: pulse 2s infinite ease-in-out;
  }
  h1 { font-size: 19px; margin: 0; font-weight: 650; letter-spacing: 0.2px; }
  .sub { color: var(--muted); font-size: 12.5px; }

  .controls { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .controls input {
    background: var(--panel);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 12.5px;
    padding: 7px 10px;
    border-radius: 8px;
    width: 190px;
  }
  .controls input:focus { outline: 2px solid var(--accent); outline-offset: 1px; }
  .controls button {
    background: var(--accent);
    color: #17122b;
    border: none;
    font-weight: 650;
    font-size: 12.5px;
    padding: 8px 14px;
    border-radius: 8px;
    cursor: pointer;
  }
  .controls button:hover { filter: brightness(1.08); }
  .meta { color: var(--muted); font-size: 11.5px; font-family: var(--mono); }

  .banner {
    display: none;
    background: rgba(240,84,107,0.12);
    border: 1px solid rgba(240,84,107,0.4);
    color: #ffb3c0;
    padding: 10px 14px;
    border-radius: 10px;
    font-size: 13px;
    margin-bottom: 18px;
  }
  .banner.show { display: block; }

  .card {
    background: linear-gradient(180deg, var(--panel), var(--panel-2));
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 18px 20px;
    margin-bottom: 18px;
  }
  .card h2 {
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    color: var(--muted);
    margin: 0 0 14px 0;
    font-weight: 650;
  }
  .card h2 .hint { text-transform: none; letter-spacing: 0; color: var(--muted); font-weight: 500; font-size: 11px; }

  .self-top { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 10px; }
  .device-name { font-size: 22px; font-weight: 700; }
  .node-id { color: var(--muted); font-family: var(--mono); font-size: 11.5px; }

  .badge {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 5px 11px; border-radius: 999px;
    font-size: 12px; font-weight: 650; font-family: var(--mono);
    border: 1px solid transparent;
  }
  .badge .d { width: 7px; height: 7px; border-radius: 50%; }
  .badge.healthy { color: var(--good); background: rgba(61,220,132,0.12); border-color: rgba(61,220,132,0.35); }
  .badge.healthy .d { background: var(--good); box-shadow: 0 0 6px var(--good); }
  .badge.busy { color: var(--warn); background: rgba(245,185,66,0.12); border-color: rgba(245,185,66,0.35); }
  .badge.busy .d { background: var(--warn); box-shadow: 0 0 6px var(--warn); }
  .badge.jammed, .badge.recovering { color: var(--bad); background: rgba(240,84,107,0.12); border-color: rgba(240,84,107,0.35); }
  .badge.jammed .d, .badge.recovering .d { background: var(--bad); box-shadow: 0 0 6px var(--bad); }
  .badge.unknown, .badge.offline { color: var(--muted); background: rgba(141,135,171,0.12); border-color: rgba(141,135,171,0.3); }
  .badge.unknown .d, .badge.offline .d { background: var(--muted); }

  .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; margin-top: 16px; }
  .stat { background: rgba(255,255,255,0.02); border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; }
  .stat .label { color: var(--muted); font-size: 10.5px; text-transform: uppercase; letter-spacing: 0.6px; }
  .stat .value { font-family: var(--mono); font-size: 17px; margin-top: 4px; font-weight: 650; }

  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead th {
    text-align: left; color: var(--muted); font-size: 10.5px; text-transform: uppercase;
    letter-spacing: 0.6px; font-weight: 650; padding: 8px 10px; border-bottom: 1px solid var(--border);
  }
  tbody td { padding: 10px; border-bottom: 1px solid rgba(255,255,255,0.04); font-family: var(--mono); }
  tbody tr:last-child td { border-bottom: none; }
  tbody tr:hover { background: rgba(255,255,255,0.02); }
  tbody tr.indirect { opacity: 0.62; }
  .pname { font-family: var(--sans); font-weight: 600; font-size: 13px; }
  .pname .you { color: var(--accent-2); font-family: var(--sans); font-weight: 600; font-size: 10.5px; margin-left: 6px; }
  .phost { color: var(--muted); font-size: 11.5px; }
  .indirect-tag { font-family: var(--sans); font-size: 10px; color: var(--muted); }

  .alive-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 7px; }
  .alive-dot.up { background: var(--good); box-shadow: 0 0 6px var(--good); }
  .alive-dot.down { background: var(--bad); }

  .empty { color: var(--muted); font-size: 13px; padding: 10px 2px; }
  footer { color: var(--muted); font-size: 11px; text-align: center; margin-top: 24px; }

  #graphWrap { width: 100%; overflow: hidden; border-radius: 10px; }
  #graphWrap svg { width: 100%; height: auto; display: block; }
  .legend { display: flex; gap: 16px; flex-wrap: wrap; margin-top: 12px; font-size: 11px; color: var(--muted); }
  .legend .item { display: flex; align-items: center; gap: 6px; }
  .legend .sw { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }

  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.35; } }
</style>
</head>
<body>
<div class="wrap">

  <header>
    <div class="brand">
      <span class="dot"></span>
      <div>
        <h1>IronMesh</h1>
        <div class="sub">live cluster status</div>
      </div>
    </div>
    <div class="controls">
      <input id="nodeInput" type="text" value="localhost:8080" spellcheck="false" />
      <button id="applyBtn">Watch</button>
      <span class="meta" id="updatedMeta">-</span>
    </div>
  </header>

  <div class="banner" id="banner"></div>

  <div class="card" id="selfCard">
    <h2>This node</h2>
    <div class="self-top">
      <div>
        <div class="device-name" id="selfName">-</div>
        <div class="node-id" id="selfId">-</div>
      </div>
      <span class="badge unknown" id="selfState"><span class="d"></span>unknown</span>
    </div>
    <div class="stat-grid" id="selfStats"></div>
  </div>

  <div class="card">
    <h2>Topology <span class="hint">- as discovered by crawling the mesh from this node</span></h2>
    <div id="graphWrap"><svg id="graphSvg" viewBox="0 0 760 440" xmlns="http://www.w3.org/2000/svg"></svg></div>
    <div class="legend">
      <span class="item"><span class="sw" style="background:var(--good)"></span>healthy</span>
      <span class="item"><span class="sw" style="background:var(--warn)"></span>busy</span>
      <span class="item"><span class="sw" style="background:var(--bad)"></span>jammed / recovering</span>
      <span class="item"><span class="sw" style="background:var(--muted)"></span>unknown / unreachable</span>
      <span class="item">solid line = link reported alive &middot; dashed = reported dead</span>
      <span class="item">dashed ring = not reached directly, only known via a peer's report</span>
    </div>
  </div>

  <div class="card">
    <h2>Connections <span id="connCount" style="color:var(--text)"></span> <span class="hint">- link history reported by this node's own /connections</span></h2>
    <table>
      <thead>
        <tr>
          <th>Peer</th>
          <th>Host</th>
          <th>Status</th>
          <th>RTT</th>
        </tr>
      </thead>
      <tbody id="connRows"></tbody>
    </table>
    <div class="empty" id="connEmpty" style="display:none;">No connection history yet.</div>
  </div>

  <div class="card">
    <h2>Peers <span id="peerCount" style="color:var(--text)"></span></h2>
    <table>
      <thead>
        <tr>
          <th>Device</th>
          <th>Host</th>
          <th>State</th>
          <th>Headroom</th>
          <th>RTT</th>
          <th>Misses</th>
        </tr>
      </thead>
      <tbody id="peerRows"></tbody>
    </table>
    <div class="empty" id="peerEmpty" style="display:none;">No peers known yet.</div>
  </div>

  <footer>polling <span id="pollTarget">localhost:8080</span> every 3s (crawls one hop out) &middot; served locally by dashboard.py</footer>
</div>

<script>
const REFRESH_MS = 3000;
let timer = null;

function fmtUptime(s) {
  if (s === undefined || s === null) return "-";
  s = Math.max(0, Math.floor(s));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return h + "h " + m + "m";
  if (m > 0) return m + "m " + sec + "s";
  return sec + "s";
}
function fmtNum(v, digits, suffix) {
  if (v === undefined || v === null) return "-";
  return Number(v).toFixed(digits !== undefined ? digits : 0) + (suffix || "");
}
function stat(label, value) {
  return '<div class="stat"><div class="label">' + label + '</div><div class="value">' + value + '</div></div>';
}
const KNOWN_STATES = ["healthy", "busy", "jammed", "recovering"];
function stateClass(state) {
  const s = (state || "unknown").toLowerCase();
  return KNOWN_STATES.includes(s) ? s : "unknown";
}
function stateColor(state) {
  const c = stateClass(state);
  if (c === "healthy") return "var(--good)";
  if (c === "busy") return "var(--warn)";
  if (c === "jammed" || c === "recovering") return "var(--bad)";
  return "var(--muted)";
}
function stateBadge(state, extraClass) {
  const cls = stateClass(state);
  return '<span class="badge ' + cls + (extraClass ? " " + extraClass : "") + '"><span class="d"></span>' + (state || "unknown") + '</span>';
}
function setBanner(msg) {
  const b = document.getElementById("banner");
  if (!msg) { b.classList.remove("show"); b.textContent = ""; return; }
  b.textContent = msg;
  b.classList.add("show");
}

function renderSelf(rootAddr, nodes) {
  const self = nodes.find(n => n.addr === rootAddr) || nodes[0];
  if (!self) return;
  document.getElementById("selfName").textContent = self.device_name || self.addr;
  document.getElementById("selfId").textContent = self.node_id || self.addr;

  const stateEl = document.getElementById("selfState");
  stateEl.className = "badge " + stateClass(self.state);
  stateEl.innerHTML = '<span class="d"></span>' + (self.state || "unknown");

  const stats = [
    stat("Load", fmtNum(self.load, 0, "%")),
    stat("Headroom", fmtNum(self.headroom, 0, "%")),
    stat("Queue depth", fmtNum(self.queue_depth, 0)),
    stat("Uptime", fmtUptime(self.uptime_s)),
    stat("Active links", (self.connections || []).filter(c => (c.status || "").toUpperCase() === "CONNECTED").length),
  ];
  document.getElementById("selfStats").innerHTML = stats.join("");
}

function renderConnections(rootAddr, nodes) {
  const self = nodes.find(n => n.addr === rootAddr) || nodes[0];
  const conns = (self && self.connections) || [];
  document.getElementById("connCount").textContent = "(" + conns.length + ")";
  const rows = conns.map(c => {
    const status = (c.status || "unknown").toUpperCase();
    const up = status === "CONNECTED";
    return '<tr>' +
      '<td><span class="alive-dot ' + (up ? "up" : "down") + '"></span>' +
        (c.peer_id || "-") +
      '</td>' +
      '<td class="phost">' + (c.host || "-") + (c.port ? (":" + c.port) : "") + '</td>' +
      '<td>' + status + '</td>' +
      '<td>' + fmtNum(c.rtt_ms, 1, "ms") + '</td>' +
    '</tr>';
  });
  document.getElementById("connRows").innerHTML = rows.join("");
  document.getElementById("connEmpty").style.display = conns.length ? "none" : "block";
}

function renderTable(rootAddr, nodes) {
  const peers = nodes.filter(n => n.addr !== rootAddr).slice();
  peers.sort((a, b) => (b.reachable === true) - (a.reachable === true));
  document.getElementById("peerCount").textContent = "(" + peers.length + ")";

  const rows = peers.map(p => {
    const up = p.reachable || p.reported_alive === true;
    const indirect = !p.reachable;
    return '<tr class="' + (indirect ? "indirect" : "") + '">' +
      '<td><span class="alive-dot ' + (up ? "up" : "down") + '"></span>' +
        '<span class="pname">' + (p.device_name || p.addr) + '</span>' +
        (indirect ? ' <span class="indirect-tag">(reported)</span>' : '') +
      '</td>' +
      '<td class="phost">' + p.addr + '</td>' +
      '<td>' + stateBadge(p.state) + '</td>' +
      '<td>' + fmtNum(p.headroom, 0, "%") + '</td>' +
      '<td>' + fmtNum(p.rtt_ms, 1, "ms") + '</td>' +
      '<td>' + (p.fail_count !== undefined && p.fail_count !== null ? p.fail_count : "-") + '</td>' +
    '</tr>';
  });
  document.getElementById("peerRows").innerHTML = rows.join("");
  document.getElementById("peerEmpty").style.display = peers.length ? "none" : "block";
}

function layout(rootAddr, nodes) {
  const W = 760, H = 440, cx = W / 2, cy = H / 2;
  const pos = {};
  const others = nodes.filter(n => n.addr !== rootAddr);
  const rootNode = nodes.find(n => n.addr === rootAddr);

  if (rootNode) {
    pos[rootAddr] = { x: cx, y: cy };
    const R = Math.min(W, H) / 2 - 70;
    others.forEach((n, i) => {
      const angle = (2 * Math.PI * i) / Math.max(1, others.length) - Math.PI / 2;
      pos[n.addr] = { x: cx + R * Math.cos(angle), y: cy + R * Math.sin(angle) };
    });
  } else {
    const R = Math.min(W, H) / 2 - 70;
    nodes.forEach((n, i) => {
      const angle = (2 * Math.PI * i) / Math.max(1, nodes.length) - Math.PI / 2;
      pos[n.addr] = { x: cx + R * Math.cos(angle), y: cy + R * Math.sin(angle) };
    });
  }
  return pos;
}

function renderGraph(rootAddr, nodes, edges) {
  const W = 760, H = 440;
  const pos = layout(rootAddr, nodes);
  let svg = '';

  edges.forEach(e => {
    const a = pos[e.a], b = pos[e.b];
    if (!a || !b) return;
    const stroke = e.alive ? "rgba(94,234,212,0.55)" : "rgba(141,135,171,0.28)";
    const dash = e.alive ? '' : ' stroke-dasharray="5,5"';
    svg += '<line x1="' + a.x + '" y1="' + a.y + '" x2="' + b.x + '" y2="' + b.y +
      '" stroke="' + stroke + '" stroke-width="1.6"' + dash + ' />';
  });

  nodes.forEach(n => {
    const p = pos[n.addr];
    if (!p) return;
    const isRoot = n.addr === rootAddr;
    const r = isRoot ? 24 : 18;
    const fill = stateColor(n.state);
    const ringDash = n.reachable ? '' : ' stroke-dasharray="3,3"';
    const label = n.device_name || n.addr;

    svg += '<circle cx="' + p.x + '" cy="' + p.y + '" r="' + r +
      '" fill="' + fill + '" fill-opacity="0.9" stroke="' +
      (isRoot ? "var(--accent)" : "rgba(255,255,255,0.35)") + '" stroke-width="' + (isRoot ? 3 : 1.6) + '"' + ringDash + ' />';

    svg += '<text x="' + p.x + '" y="' + (p.y + r + 16) + '" text-anchor="middle" font-size="11" fill="var(--text)" font-family="var(--sans)">' +
      (label.length > 16 ? label.slice(0, 15) + '\\u2026' : label) + '</text>';

    if (isRoot) {
      svg += '<text x="' + p.x + '" y="' + (p.y + 4) + '" text-anchor="middle" font-size="10" font-weight="700" fill="#0f0d1c" font-family="var(--sans)">you</text>';
    }
  });

  document.getElementById("graphSvg").innerHTML = svg;
}

async function tick() {
  const node = document.getElementById("nodeInput").value.trim() || "localhost:8080";
  document.getElementById("pollTarget").textContent = node;
  try {
    const res = await fetch("/api/topology?node=" + encodeURIComponent(node) + "&hops=2", { cache: "no-store" });
    const graph = await res.json();
    const nodes = graph.nodes || [];
    const rootReachable = nodes.some(n => n.addr === graph.root && n.reachable);

    if (!rootReachable) {
      setBanner("Can't reach " + node + " - is the node running and is the address correct?");
    } else {
      setBanner(null);
      renderSelf(graph.root, nodes);
      renderConnections(graph.root, nodes);
      renderTable(graph.root, nodes);
      renderGraph(graph.root, nodes, graph.edges || []);
    }
    document.getElementById("updatedMeta").textContent = "updated " + new Date().toLocaleTimeString();
  } catch (err) {
    setBanner("Dashboard server error: " + err);
  }
}

function restart() {
  if (timer) clearInterval(timer);
  tick();
  timer = setInterval(tick, REFRESH_MS);
}

document.getElementById("applyBtn").addEventListener("click", restart);
document.getElementById("nodeInput").addEventListener("keydown", (e) => {
  if (e.key === "Enter") restart();
});

restart();
</script>
</body>
</html>
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="IronMesh live status + topology dashboard")
    ap.add_argument("--node", default=DEFAULT_NODE, help="node address to start crawling from, e.g. localhost:8080")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT, help="port to serve the dashboard on")
    args = ap.parse_args()

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.default_node = args.node  # type: ignore[attr-defined]

    print("\nIronMesh dashboard running")
    print("  crawling from : {}".format(args.node))
    print("  open          : http://localhost:{}\n".format(args.port))
    print("  press Ctrl-C to stop\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()