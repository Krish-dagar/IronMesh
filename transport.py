"""Transport layer: UDP discovery, HTTP data plane, heartbeat gossip.

Three cooperating parts:

1. ``UDPDiscovery``
   Broadcasts ``announce`` frames on the control port every few seconds and
   listens for peer announcements. Receiving a peer's announce registers it
   in the ``PeerTable``; we also reply with our own announce (unicast) so a
   node is known even when switches disable directed broadcast.

2. ``NodeHttpServer``
   A threaded ``http.server`` instance exposing the data/control endpoints:
   ``POST /submit``, ``POST /route``, ``POST /heartbeat``, ``POST /trigger-hop``,
   ``POST /jam``, ``GET /health``, ``GET /peers``, ``GET /metrics``, 
   ``GET /connections``, ``GET /messages``, ``GET /spectrum``. All request logic 
   is delegated to the injected ``app`` (see node.py / pipeline.py); this module 
   stays I/O-only.

3. ``GossipLoop``
   Periodically POSTs our metrics snapshot to every known peer (and any
   unconfirmed static seed endpoint), measuring round-trip time, updating
   the peer table, and triggering failure backoff on errors.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional, Tuple

log = logging.getLogger("node.transport")

# Fallback-only heuristic used by the /connections handler when a PeerTable
# doesn't expose .summary() (e.g. a test double). A peer we've heard from
# more recently than this is treated as connected. This is NOT the real
# liveness timeout - that lives on PeerTable itself (peer_timeout_s) and is
# what .summary() and .live() actually use.
_LIVENESS_FALLBACK_S = 15.0


def local_ip() -> str:
    """Best guess at this host's routable IP on the primary interface."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        return "127.0.0.1"


# --------------------------------------------------------------------------
# HTTP client (urllib based, stdlib only)
# --------------------------------------------------------------------------

class HTTPClient:
    """Tiny JSON HTTP client with bounded timeouts."""

    def __init__(self, timeout_s: float = 3.0, logger=None):
        self._timeout = timeout_s
        self._log = logger or log

    def post_json(self, url: str, payload: dict) -> Tuple[Optional[dict], float, bool]:
        """POST JSON, return ``(response_json, elapsed_s, ok)``."""
        start = time.time()
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                return json.loads(body), time.time() - start, True
        except Exception as exc:
            self._log.debug("http post failed %s: %s", url, exc)
            return None, time.time() - start, False


# --------------------------------------------------------------------------
# UDP discovery
# --------------------------------------------------------------------------

class UDPDiscovery(threading.Thread):
    """Broadcast + listen for peer announces on the control port."""

    def __init__(
        self,
        node_id: str,
        advertised_host: str,
        http_port: int,
        udp_port: int,
        broadcast_addr: str,
        announce_interval_s: float,
        on_peer: Callable[[str, str, int], None],
        logger=None,
    ):
        super().__init__(name="udp-discovery", daemon=True)
        self._node_id = node_id
        self._host = advertised_host
        self._http_port = http_port
        self._udp_port = int(udp_port)
        self._broadcast = broadcast_addr
        self._interval = max(1.0, float(announce_interval_s))
        self._on_peer = on_peer
        self._log = logger or log
        self._stop = threading.Event()

        self._rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._rx.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        except OSError:
            pass
        self._rx.bind(("", self._udp_port))
        self._rx.settimeout(0.5)

        self._tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._tx.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def _frame(self) -> bytes:
        return json.dumps(
            {
                "type": "announce",
                "node_id": self._node_id,
                "host": self._host,
                "http_port": self._http_port,
            }
        ).encode("utf-8")

    def _send_to(self, addr: Tuple[str, int]) -> None:
        try:
            self._tx.sendto(self._frame(), addr)
        except OSError as exc:
            self._log.debug("announce send failed %s: %s", addr, exc)

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                data, addr = self._rx.recvfrom(65535)
                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception:
                    continue
                if msg.get("type") == "announce":
                    nid = str(msg.get("node_id", ""))
                    host = str(msg.get("host", addr[0]))
                    port = int(msg.get("http_port", 0))
                    if nid and nid != self._node_id and port > 0:
                        self._on_peer(nid, host, port)
                        self._send_to((addr[0], self._udp_port))
            except socket.timeout:
                pass
            except OSError:
                time.sleep(0.2)

    def announce_loop(self) -> None:
        """Separate sender loop that broadcasts on a cadence."""
        while not self._stop.is_set():
            self._send_to((self._broadcast, self._udp_port))
            self._stop.wait(self._interval)

    def start_senders(self) -> None:
        self.start()
        t = threading.Thread(name="udp-announcer", target=self.announce_loop, daemon=True)
        t.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._rx.close()
            self._tx.close()
        except OSError:
            pass


# --------------------------------------------------------------------------
# HTTP server
# --------------------------------------------------------------------------

class _NodeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        log.debug("http %s %s", self.command, self.path)

    def _json(self, status: int, obj: dict, extra_headers: Optional[dict] = None) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> Optional[dict]:
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            return None

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        app = self.server.app

        if path == "/health":
            self._json(200, app.on_health())
        elif path == "/peers":
            self._json(200, app.on_peers())
        elif path == "/metrics":
            self._json(200, app.on_metrics())
        elif path == "/spectrum":
            self._json(200, {
                "node_id": getattr(app, "node_id", "unknown_node"),
                "current_channel": getattr(app, "current_channel", "2.412 GHz (Ch 1)"),
                "is_jammed": getattr(app, "is_jammed", False),
                "jamming_events": getattr(app, "jamming_events", [])
            })
        elif path == "/connections":
            try:
                peers_data: list = []
                if hasattr(app, "peers"):
                    table = app.peers
                    # Preferred path: PeerTable already knows how to build
                    # this correctly (uses its own liveness timeout, real
                    # rtt_ms/headroom/fail_count, and "alive" as a real bool).
                    # The old code reached for a nonexistent `.peers`
                    # attribute (the real field is the private `_peers`),
                    # which silently fell back to the PeerTable object
                    # itself - neither a dict nor a list - so peers_data was
                    # always empty and /connections always reported zero
                    # links even when the mesh was fully connected.
                    if hasattr(table, "summary") and callable(table.summary):
                        peers_data = table.summary()
                    else:
                        raw_peers = getattr(table, "peers", None)
                        if isinstance(raw_peers, dict):
                            peers_data = list(raw_peers.values())
                        elif isinstance(raw_peers, list):
                            peers_data = raw_peers

                conn_history = getattr(app, "connection_logs", [])
                if not conn_history and peers_data:
                    conn_history = []
                    now = time.time()
                    for p in peers_data:
                        if isinstance(p, dict):
                            conn_history.append({
                                "peer_id": p.get("node_id", "unknown"),
                                "host": p.get("host"),
                                "port": p.get("port"),
                                "status": "CONNECTED" if p.get("alive") else "DISCONNECTED",
                                "rtt_ms": p.get("rtt_ms")
                            })
                        else:
                            # Fallback for raw Peer-like objects. Peer.alive
                            # is a *method* (alive(timeout_s, now) -> bool),
                            # so getattr(p, "alive", True) used to return the
                            # bound method itself, which is always truthy in
                            # Python - every peer showed as CONNECTED
                            # regardless of real liveness. We don't have
                            # access to the table's private timeout here, so
                            # use a conservative recency heuristic instead.
                            last_seen = getattr(p, "last_seen", None)
                            is_alive = (
                                last_seen is not None
                                and (now - last_seen) <= _LIVENESS_FALLBACK_S
                            )
                            conn_history.append({
                                "peer_id": getattr(p, "node_id", "unknown"),
                                "host": getattr(p, "host", None),
                                "port": getattr(p, "port", None),
                                "status": "CONNECTED" if is_alive else "DISCONNECTED",
                                "rtt_ms": round(getattr(p, "rtt_s", 0.0) * 1000, 1)
                                if getattr(p, "rtt_s", None) is not None else None,
                            })

                self._json(200, {
                    "node_id": getattr(app, "node_id", "unknown_node"),
                    "active_peers_count": len(peers_data),
                    "connections": list(conn_history) if isinstance(conn_history, (list, tuple)) else []
                })
            except Exception as e:
                print(f"[TRANSPORT ERROR] Failed processing /connections: {e}")
                self._json(500, {
                    "status": "error",
                    "message": f"Internal node error: {str(e)}"
                })
        elif path == "/messages":
            try:
                if hasattr(app, "on_messages"):
                    self._json(200, app.on_messages())
                else:
                    msg_logs = getattr(app, "message_logs", [])
                    self._json(200, {
                        "node_id": getattr(app, "node_id", "unknown_node"),
                        "total_messages": len(msg_logs) if isinstance(msg_logs, (list, tuple)) else 0,
                        "messages": list(msg_logs) if isinstance(msg_logs, (list, tuple)) else []
                    })
            except Exception as e:
                print(f"[TRANSPORT ERROR] Failed processing /messages: {e}")
                self._json(500, {
                    "status": "error",
                    "message": f"Internal node error: {str(e)}"
                })
        elif path == "/":
            self._json(200, app.on_banner())
        else:
            self._json(404, {"status": "error", "error": "not found"})

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        app = self.server.app
        body = self._read_json_body()
        if body is None:
            self._json(400, {"status": "error", "error": "invalid json body"})
            return
        if parsed.path == "/submit":
            resp = app.on_submit(body, query_string=parsed.query)
            self._respond(resp)
        elif parsed.path == "/route":
            resp = app.on_route(body)
            self._respond(resp)
        elif parsed.path == "/heartbeat":
            resp = app.on_heartbeat(body)
            self._json(200, resp)
        elif parsed.path == "/trigger-hop":
            if hasattr(app, "trigger_frequency_hop"):
                event = app.trigger_frequency_hop(reason="Dashboard Manual Command")
                self._json(200, {"status": "SUCCESS", "event": event})
            else:
                self._json(500, {"status": "error", "message": "Frequency hopper not initialized on node"})
        elif parsed.path in ("/jam", "/simulate-jam"):
            event = None
            if hasattr(app, "trigger_jamming"):
                event = app.trigger_jamming()
            elif hasattr(app, "simulate_jamming"):
                event = app.simulate_jamming()
            else:
                # Direct attribute fallbacks
                app.is_jammed = True
                event = {
                    "timestamp": time.time(),
                    "channel": getattr(app, "current_channel", "Ch 1"),
                    "reason": "Simulated jamming payload injected via HTTP"
                }
                if hasattr(app, "jamming_events") and isinstance(app.jamming_events, list):
                    app.jamming_events.append(event)
                else:
                    app.jamming_events = [event]
            self._json(200, {"status": "SUCCESS", "jammed": True, "event": event})
        else:
            self._json(404, {"status": "error", "error": "not found"})

    def _respond(self, resp: dict) -> None:
        """Emit a pipeline response, mapping app-level codes to HTTP status
        and surfacing Retry-After for backpressure / rate limiting."""
        status = int(resp.get("_http_status", 200))
        headers = {}
        if status in (429, 503) and "retry_after" in resp:
            headers["Retry-After"] = str(resp["retry_after"])
        body = {k: v for k, v in resp.items() if not k.startswith("_")}
        self._json(status, body, extra_headers=headers)


class NodeHttpServer:
    def __init__(self, app, host: str, port: int, logger=None):
        self._log = logger or log
        self._server = ThreadingHTTPServer((host, port), _NodeHandler)
        self._server.app = app  # type: ignore[attr-defined]
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]

    def serve(self) -> None:
        self._server.serve_forever(poll_interval=0.5)

    def stop(self) -> None:
        try:
            self._server.shutdown()
            self._server.server_close()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Heartbeat gossip loop
# --------------------------------------------------------------------------

class GossipLoop(threading.Thread):
    """Periodically gossip our snapshot to peers and confirm seed endpoints."""

    def __init__(
        self,
        node_id: str,
        advertised_host: str,
        http_port: int,
        peer_table,
        snapshot_provider: Callable[[], dict],
        seed_endpoints: List[Tuple[str, int]],
        heartbeat_interval_s: float,
        timeout_s: float,
        logger=None,
    ):
        super().__init__(name="gossip-loop", daemon=True)
        self._node_id = node_id
        self._host = advertised_host
        self._http_port = http_port
        self._table = peer_table
        self._snapshot = snapshot_provider
        self._seeds = [
            (h, p)
            for (h, p) in (seed_endpoints or [])
            if not (p == self._http_port and h in ("127.0.0.1", "localhost"))
        ]
        self._interval = max(0.5, float(heartbeat_interval_s))
        self._timeout = float(timeout_s)
        self._log = logger or log
        self._client = HTTPClient(timeout_s=max(1.0, self._interval * 2), logger=logger)
        self._stop = threading.Event()

    def _covered(self, host: str, port: int) -> bool:
        for p in self._table.live():
            if p.port == port and (p.host == host or {p.host, host} <= {"127.0.0.1", "localhost", "0.0.0.0"}):
                return True
            if p.host == host and p.port == port:
                return True
        return False

    def _send_heartbeat(self, host: str, port: int) -> None:
        url = "http://{}:{}/heartbeat".format(host, port)
        # Collect our current known active peers to share in gossip
        known_peers_list = []
        if hasattr(self._table, "summary") and callable(self._table.summary):
            for p in self._table.summary():
                if p.get("alive") and p.get("node_id") != self._node_id:
                    known_peers_list.append({
                        "node_id": p.get("node_id"),
                        "host": p.get("host"),
                        "port": p.get("port"),
                    })

        payload = {
            "node_id": self._node_id,
            "host": self._host,
            "http_port": self._http_port,
            "snapshot": self._snapshot(),
            "known_peers": known_peers_list,
        }
        resp, rtt, ok = self._client.post_json(url, payload)
        if not ok:
            self._log.debug("heartbeat to %s:%d failed", host, port)
            return
        if isinstance(resp, dict) and resp.get("node_id"):
            resp_nid = str(resp["node_id"])
            if resp_nid == self._node_id:
                return
            # Evict any stale entry at this addr with a different node_id (node restarted)
            self._table.remove_stale_by_addr(host, port, keep_node_id=resp_nid)
            peer = self._table.upsert(resp_nid, host, port)
            self._table.observe(peer.node_id, resp.get("snapshot", {}) or {}, rtt)

            # Transitive peer discovery (PEX): merge peers returned by the remote node
            remote_known_peers = resp.get("known_peers", []) or []
            for rp in remote_known_peers:
                r_nid = str(rp.get("node_id", ""))
                r_host = str(rp.get("host", ""))
                r_port = int(rp.get("port") or rp.get("http_port") or 0)
                if r_nid and r_nid != self._node_id and r_host and r_port > 0:
                    if not self._covered(r_host, r_port):
                        # Also evict any stale entry at this PEX addr
                        self._table.remove_stale_by_addr(r_host, r_port, keep_node_id=r_nid)
                        self._table.upsert(r_nid, r_host, r_port)

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                targets = []
                seen_targets = set()
                for peer in self._table.all():
                    if peer.node_id != self._node_id and (peer.host, peer.port) not in seen_targets:
                        targets.append((peer.host, peer.port))
                        seen_targets.add((peer.host, peer.port))
                for (host, port) in list(self._seeds):
                    if (host, port) not in seen_targets and not (port == self._http_port and host in ("127.0.0.1", "localhost")):
                        targets.append((host, port))
                        seen_targets.add((host, port))

                for (host, port) in targets:
                    self._send_heartbeat(host, port)
            except Exception as exc:
                self._log.warning("gossip cycle error: %s", exc)
            self._stop.wait(self._interval)

    def stop(self) -> None:
        self._stop.set()