"""Transport layer: UDP discovery, HTTP data plane, heartbeat gossip.

Three cooperating parts:

1. ``UDPDiscovery``
   Broadcasts ``announce`` frames on the control port every few seconds and
   listens for peer announcements. Receiving a peer's announce registers it
   in the ``PeerTable``; we also reply with our own announce (unicast) so a
   node is known even when switches disable directed broadcast.

2. ``NodeHttpServer``
   A threaded ``http.server`` instance exposing the data/control endpoints:
   ``POST /submit``, ``POST /route``, ``POST /heartbeat``, ``GET /health``,
   ``GET /peers``, ``GET /metrics``. All request logic is delegated to the
   injected ``app`` (see node.py / pipeline.py); this module stays I/O-only.

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
        # Continuous receiver loop (announce broadcasts also drive sending
        # so a single socket does both discovery and reply).
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
                        # Reply unicast so the sender learns about us even if
                        # broadcast is one-way (directed broadcast disabled).
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
        app = self.server.app
        if parsed.path == "/health":
            self._json(200, app.on_health())
        elif parsed.path == "/peers":
            self._json(200, app.on_peers())
        elif parsed.path == "/metrics":
            self._json(200, app.on_metrics())
        elif parsed.path == "/":
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
        # Drop seeds that point at ourselves (same port on localhost) to avoid
        # self-heartbeat loops.
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
            if p.host == host and p.port == port:
                return True
        return False

    def _send_heartbeat(self, host: str, port: int) -> None:
        url = "http://{}:{}/heartbeat".format(host, port)
        payload = {
            "node_id": self._node_id,
            "host": self._host,
            "http_port": self._http_port,
            "snapshot": self._snapshot(),
        }
        resp, rtt, ok = self._client.post_json(url, payload)
        if not ok:
            self._log.debug("heartbeat to %s:%d failed", host, port)
            return
        if isinstance(resp, dict) and resp.get("node_id"):
            if resp["node_id"] == self._node_id:
                return  # seed endpoint pointed back at ourselves
            # We now know the peer's identity -> upsert so future gossip flows.
            peer = self._table.upsert(
                str(resp["node_id"]), host, port
            )
            self._table.observe(peer.node_id, resp.get("snapshot", {}) or {}, rtt)

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                for peer in self._table.live():
                    if peer.node_id != self._node_id:
                        self._send_heartbeat(peer.host, peer.port)
                # Confirm static seeds that aren't yet covered by a live peer.
                for (host, port) in list(self._seeds):
                    if not self._covered(host, port):
                        self._send_heartbeat(host, port)
            except Exception as exc:
                self._log.warning("gossip cycle error: %s", exc)
            self._stop.wait(self._interval)

    def stop(self) -> None:
        self._stop.set()
