"""Node entry point: wires all modules into a runnable cluster member.

Usage:
    python3 node.py --config config.json [--peers 192.168.1.20:8080]

Each laptop runs exactly this program. Configuration is read from a JSON
file (stdlib has no YAML parser), and the static seed peer list can be
overridden on the command line for quick trials.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
import time
from typing import Dict, List, Optional, Tuple

from identity import NodeIdentity
from metrics import MetricsSampler
from parser import ContextParser
from pipeline import Pipeline
from ratelimit import RateLimiter
from render import Renderer
from routing import PeerTable, RoutingSwitch
from state import StateConfig, StateMachine
from transport import (
    GossipLoop,
    HTTPClient,
    NodeHttpServer,
    UDPDiscovery,
    local_ip,
)

log = logging.getLogger("node")


def load_config(path: str) -> dict:
    with open(path) as fh:
        return json.load(fh)


def parse_peers(raw: List[str], default_port: int) -> List[Tuple[str, int]]:
    out: List[Tuple[str, int]] = []
    for item in raw:
        host, _, port = item.partition(":")
        host = host.strip()
        if not host:
            continue
        try:
            port = int(port) if port else default_port
        except ValueError:
            port = default_port
        out.append((host, port))
    return out


class Node:
    """A single laptop: discovery, gossip, pipeline, HTTP server."""

    def __init__(self, cfg: dict, seed_peers: Optional[List[Tuple[str, int]]] = None):
        self.cfg = cfg
        self.name = str(cfg.get("name", "node"))
        identity = NodeIdentity.create(str(cfg.get("node_id", "auto")), self.name)
        self.identity = identity
        self.node_id = identity.node_id

        self.http_port = int(cfg.get("http_port", 8080))
        self.listen_host = str(cfg.get("listen_host", "0.0.0.0"))
        self.advertise_host = str(cfg.get("advertise_host", "") or local_ip())
        self.udp_port = int(cfg.get("udp_port", 48765))
        self.broadcast_addr = str(cfg.get("udp_broadcast_addr", "255.255.255.255"))

        # Feature modules.
        self.state = StateMachine(StateConfig(cfg.get("state", {})))
        self.limiter = RateLimiter(cfg.get("limits", {}))
        self.parser = ContextParser()
        self.renderer = Renderer(cfg.get("render", {}))
        self.peers = PeerTable(float(cfg.get("peer_timeout_s", 8)), log)
        rcfg = cfg.get("routing", {})
        self.routing = RoutingSwitch(
            self.peers,
            max_hops=int(rcfg.get("max_hops", 3)),
            min_peer_headroom=float(rcfg.get("min_peer_headroom", 15)),
            node_id_provider=lambda: self.node_id,
        )
        self.http_client = HTTPClient(timeout_s=3.0, logger=log)

        self.pipeline = Pipeline(
            node_id=self.node_id,
            cfg=cfg,
            state=self.state,
            limiter=self.limiter,
            parser=self.parser,
            renderer=self.renderer,
            routing=self.routing,
            peers=self.peers,
            http_client=self.http_client,
            advertised_host=self.advertise_host,
        )

        # Metrics sampler feeds both state machine and peers.
        self.metrics = MetricsSampler(
            interval_s=float(cfg.get("metrics_interval_s", 1)),
            queue_depth_provider=self.pipeline.queue_depth,
            rate_provider=self.pipeline.rates,
            logger=log,
            on_snapshot=self.state.evaluate,
        )
        self.pipeline.attach_snapshot(self.metrics.snapshot)

        # Transport.
        seed = seed_peers or parse_peers(cfg.get("peers", []), self.http_port)
        self._http_server = NodeHttpServer(self, self.listen_host, self.http_port, log)
        self.http_port = self._http_server.port
        self.udp = UDPDiscovery(
            node_id=self.node_id,
            advertised_host=self.advertise_host,
            http_port=self.http_port,
            udp_port=self.udp_port,
            broadcast_addr=self.broadcast_addr,
            announce_interval_s=float(cfg.get("announce_interval_s", 3)),
            on_peer=self._on_discovered_peer,
            logger=log,
        )
        self.gossip = GossipLoop(
            node_id=self.node_id,
            advertised_host=self.advertise_host,
            http_port=self.http_port,
            peer_table=self.peers,
            snapshot_provider=lambda: self.metrics.snapshot().to_dict(),
            seed_endpoints=seed,
            heartbeat_interval_s=float(cfg.get("heartbeat_interval_s", 2)),
            timeout_s=float(cfg.get("peer_timeout_s", 8)),
            logger=log,
        )
        self._stopping = threading.Event()

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        self.metrics.start()
        self.udp.start_senders()
        threading.Thread(
            name="http-server", target=self._http_server.serve, daemon=True
        ).start()
        self.gossip.start()
        log.info(
            "node %s up on http://%s:%d (advertise=%s) udp=%d",
            self.node_id,
            self.listen_host,
            self.http_port,
            self.advertise_host,
            self.udp_port,
        )

    def stop(self) -> None:
        self._stopping.set()
        self.gossip.stop()
        self.udp.stop()
        self._http_server.stop()

    def wait(self) -> None:
        while not self._stopping.is_set():
            self._stopping.wait(1.0)

    # -- discovery/gossip callbacks -----------------------------------------

    def _on_discovered_peer(self, node_id: str, host: str, port: int) -> None:
        if node_id == self.node_id:
            return  # ignore our own broadcast echoing back
        self.peers.upsert(node_id, host, port)

    # -- HTTP app callbacks --------------------------------------------------

    def on_banner(self) -> Dict[str, object]:
        return {
            "status": "ok",
            "service": "llm-node",
            "node_id": self.node_id,
            "identity": self.identity.summary(),
            "state": self.state.current().value,
            "peers": self.peers.summary(),
            "uptime_s": int(time.time() - self.identity.started_at),
        }

    def on_health(self) -> Dict[str, object]:
        snap = self.metrics.snapshot()
        return {
            "status": "ok",
            "node_id": self.node_id,
            "state": self.state.current().value,
            "backpressure": self.state.backpressure(),
            "is_accepting": self.state.is_accepting(),
            "load": snap.load_score(),
            "queue_depth": snap.queue_depth,
            "req_rate_1s": snap.req_rate_1s,
            "uptime_s": int(time.time() - self.identity.started_at),
        }

    def on_peers(self) -> Dict[str, object]:
        return {"node_id": self.node_id, "peers": self.peers.summary()}

    def on_metrics(self) -> Dict[str, object]:
        return {
            "node_id": self.node_id,
            "snapshot": self.metrics.snapshot().to_dict(),
            "limiter": self.limiter.stats(),
            "routing": self.routing.stats(),
            "pipeline": self.pipeline.stats(),
            "peers": self.peers.summary(),
        }

    def on_heartbeat(self, body: dict) -> Dict[str, object]:
        sender_id = str(body.get("node_id", ""))
        host = str(body.get("host", ""))
        port = int(body.get("http_port", 0) or 0)
        if sender_id and sender_id != self.node_id and host and port:
            peer = self.peers.upsert(sender_id, host, port)
            self.peers.observe(sender_id, body.get("snapshot", {}) or {}, rtt_s=0.0)
            return {
                "ok": True,
                "node_id": self.node_id,
                "host": self.advertise_host,
                "http_port": self.http_port,
                "snapshot": self.metrics.snapshot().to_dict(),
            }
        return {"ok": False, "node_id": self.node_id}

    def on_submit(self, body: dict, query_string: str = "") -> Dict[str, object]:
        return self.pipeline.handle_submit(body, query_string=query_string)

    def on_route(self, body: dict) -> Dict[str, object]:
        return self.pipeline.handle_route(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM discovery & personalization node")
    parser.add_argument("--config", default="config.json", help="JSON config file")
    parser.add_argument(
        "--peers", nargs="*", default=None,
        help="override static peers, e.g. --peers 192.168.1.20:8080",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    logging.basicConfig(
        level=getattr(logging, str(cfg.get("logging", "INFO")).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    seed = parse_peers(args.peers or [], int(cfg.get("http_port", 8080)))
    node = Node(cfg, seed_peers=seed or None)

    def _shutdown(signum, frame):
        log.info("signal %d received, shutting down", signum)
        node.stop()

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    node.start()
    print("\n{} @ {}:{}  ({})\n".format(node.node_id, node.advertise_host, node.http_port, node.identity.hostname))
    print("  http://localhost:{}/health   state & congestion".format(node.http_port))
    print("  http://localhost:{}/peers    cluster view".format(node.http_port))
    print("  http://localhost:{}/metrics  full snapshot".format(node.http_port))
    print("  press Ctrl-C to stop\n")
    node.wait()
    print("node stopped.")


if __name__ == "__main__":
    main()
