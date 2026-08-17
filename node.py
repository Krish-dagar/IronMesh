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
from collections import deque
from typing import Dict, List, Optional, Tuple

from identity import NodeIdentity
from metrics import MetricsSampler
from parser import ContextParser
from pipeline import Pipeline
from ratelimit import RateLimiter
from render import Renderer
from routing import PeerTable, RoutingSwitch
from security import pqc_node
from state import StateConfig, StateMachine
from transport import (
    GossipLoop,
    HTTPClient,
    NodeHttpServer,
    UDPDiscovery,
    local_ip,
)

log = logging.getLogger("node")

# Ring buffer storing recent message telemetry
MESSAGE_LOGS = deque(maxlen=100)

# Tracker storing peer link state events
CONNECTION_LOGS: List[Dict[str, object]] = []


def log_message_event(sender_id: str, action: str, payload_summary: object, status: str = "SUCCESS") -> None:
    """Tracks message routing and delivery events."""
    MESSAGE_LOGS.appendleft({
        "timestamp": time.time(),
        "sender_id": sender_id,
        "action": action,  # e.g., "RECEIVED", "FORWARDED", "DECRYPTED"
        "payload_echo": payload_summary,
        "status": status,
    })


def register_connection(peer_id: str, peer_ip: str, status: str = "CONNECTED") -> None:
    """Logs active handshakes and peer link state."""
    event = {
        "timestamp": time.time(),
        "peer_id": peer_id,
        "peer_ip": peer_ip,
        "status": status,
        "message": f"Successfully connected to host {peer_id} at {peer_ip}",
    }
    CONNECTION_LOGS.append(event)
    print(f"[MANET LINK] {event['message']}")


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
    """A single laptop: discovery, gossip, pipeline, HTTP server, tactical anti-jamming."""

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

        # Connection logs instance storage
        self.connection_logs: List[Dict[str, object]] = []

        # Spectrum / Anti-Jamming Parameters
        self.channel_list = cfg.get("channel_list", [
            "2.412 GHz (Ch 1)",
            "2.437 GHz (Ch 6)",
            "2.462 GHz (Ch 11)",
            "5.180 GHz (Ch 36)"
        ])
        self.current_channel = self.channel_list[0]
        self.is_jammed = False
        self.jamming_events: List[Dict[str, object]] = []

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
            on_snapshot=self._on_metrics_snapshot,
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

    def _has_peer(self, peer_id: str) -> bool:
        """Safely checks if a peer exists across different PeerTable internal attribute designs."""
        if hasattr(self.peers, "peers"):
            return peer_id in self.peers.peers
        elif hasattr(self.peers, "table"):
            return peer_id in self.peers.table
        try:
            return peer_id in self.peers
        except TypeError:
            return False

    # -- Spectrum & Anti-Jamming Logic --------------------------------------

    def check_jamming_status(self, packet_drop_rate: float) -> None:
        """Monitors drop rates and triggers frequency hop if drop rate > 50%."""
        if packet_drop_rate > 0.5 and not self.is_jammed:
            self.is_jammed = True
            self.trigger_frequency_hop(reason=f"High packet loss detected ({packet_drop_rate:.0%})")

    def trigger_frequency_hop(self, reason: str = "Manual Alert / Anti-Jamming Trigger") -> Dict[str, object]:
        """Coordinates a synchronized frequency switch across all connected mesh nodes."""
        old_chan = self.current_channel
        curr_idx = self.channel_list.index(self.current_channel) if self.current_channel in self.channel_list else 0
        self.current_channel = self.channel_list[(curr_idx + 1) % len(self.channel_list)]
        self.is_jammed = False

        event = {
            "timestamp": time.time(),
            "event": "FREQUENCY_HOP",
            "from_channel": old_chan,
            "to_channel": self.current_channel,
            "reason": reason
        }
        self.jamming_events.insert(0, event)
        self.jamming_events = self.jamming_events[:10]  # Store last 10 hops

        log.warning("[ANTI-JAMMING AGILITY] Hopped from %s -> %s | Reason: %s", old_chan, self.current_channel, reason)
        print(f"[ANTI-JAMMING AGILITY] Hopped from {old_chan} -> {self.current_channel}")
        return event

    def _on_metrics_snapshot(self, snap: object) -> None:
        """Callback for MetricsSampler: evaluates state & monitors packet loss for jamming."""
        self.state.evaluate(snap)
        # Check packet drop rate for jamming mitigation if the snapshot provides it
        drop_rate = getattr(snap, "packet_drop_rate", 0.0)
        self.check_jamming_status(drop_rate)

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        self.metrics.start()
        self.udp.start_senders()
        threading.Thread(
            name="http-server", target=self._http_server.serve, daemon=True
        ).start()
        self.gossip.start()
        log.info(
            "node %s up on http://%s:%d (advertise=%s) udp=%d | channel=%s",
            self.node_id,
            self.listen_host,
            self.http_port,
            self.advertise_host,
            self.udp_port,
            self.current_channel,
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
        peer_addr = f"{host}:{port}"
        if not self._has_peer(node_id):
            register_connection(node_id, peer_addr, status="CONNECTED")
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
            "current_channel": self.current_channel,
            "is_jammed": self.is_jammed,
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
            "current_channel": self.current_channel,
            "is_jammed": self.is_jammed,
            "uptime_s": int(time.time() - self.identity.started_at),
        }

    def on_peers(self) -> Dict[str, object]:
        return {"node_id": self.node_id, "peers": self.peers.summary()}

    def on_connections(self) -> Dict[str, object]:
        """Returns active peer link count and connection event history."""
        active_peers = (
            self.peers.summary().get("active_count", len(self.peers.peers if hasattr(self.peers, "peers") else []))
            if hasattr(self.peers, "summary")
            else 0
        )
        return {
            "node_id": self.node_id,
            "active_peers_count": active_peers,
            "connection_history": getattr(self, "connection_logs", CONNECTION_LOGS[-20:]),
        }

    def on_metrics(self) -> Dict[str, object]:
        return {
            "node_id": self.node_id,
            "snapshot": self.metrics.snapshot().to_dict(),
            "limiter": self.limiter.stats(),
            "routing": self.routing.stats(),
            "pipeline": self.pipeline.stats(),
            "peers": self.peers.summary(),
            "spectrum": {
                "current_channel": self.current_channel,
                "is_jammed": self.is_jammed,
                "available_channels": self.channel_list,
                "recent_hops": self.jamming_events,
            },
        }

    def on_messages(self) -> Dict[str, object]:
        """Returns recent message routing and telemetry logs."""
        return {
            "node_id": self.node_id,
            "total_logged": len(MESSAGE_LOGS),
            "messages": list(MESSAGE_LOGS),
        }

    def on_spectrum(self) -> Dict[str, object]:
        """Returns current frequency spectrum telemetry and hopping event history."""
        return {
            "node_id": self.node_id,
            "current_channel": self.current_channel,
            "is_jammed": self.is_jammed,
            "channel_list": self.channel_list,
            "jamming_events": self.jamming_events,
        }

    def on_heartbeat(self, body: dict) -> Dict[str, object]:
        sender_id = body.get("sender_id") or body.get("node_id")
        sender_ip = body.get("host") or body.get("ip")

        # Initialize log storage on the node instance if missing
        if not hasattr(self, "connection_logs"):
            self.connection_logs = []

        # Log incoming peer handshake event
        if sender_id and sender_ip:
            log_entry = {
                "timestamp": time.time(),
                "peer_id": sender_id,
                "host": sender_ip,
                "status": "HANDSHAKE_RECEIVED",
                "message": f"Connected to {sender_id} at {sender_ip}",
            }

            # Keep only the last 20 connection events
            self.connection_logs.insert(0, log_entry)
            self.connection_logs = self.connection_logs[:20]

        # Existing heartbeat processing logic
        host = str(body.get("host", ""))
        port = int(body.get("http_port", 0) or 0)
        sender_id_str = str(sender_id or "")

        if sender_id_str and sender_id_str != self.node_id and host and port:
            peer_addr = f"{host}:{port}"
            if not self._has_peer(sender_id_str):
                register_connection(sender_id_str, peer_addr, status="CONNECTED")
            self.peers.upsert(sender_id_str, host, port)
            self.peers.observe(sender_id_str, body.get("snapshot", {}) or {}, rtt_s=0.0)
            return {
                "ok": True,
                "node_id": self.node_id,
                "host": self.advertise_host,
                "http_port": self.http_port,
                "current_channel": self.current_channel,
                "snapshot": self.metrics.snapshot().to_dict(),
            }
        return {"ok": False, "node_id": self.node_id}

    def on_submit(self, body: dict, query_string: str = "") -> Dict[str, object]:
        # 1. Basic Type Validation
        if not isinstance(body, dict):
            return {"ok": False, "error": "Invalid JSON format"}

        sender_id = str(body.get("sender_id", body.get("node_id", "unknown")))
        log_message_event(sender_id, "RECEIVED", str(body.get("prompt", body))[:80])

        # 2. PQC Security / Decryption Middleware
        if body.get("pqc_encrypted"):
            try:
                body = pqc_node.decrypt_payload(body)
                print("[SECURITY LAYER] Decrypted AES-256 payload successfully.")
                log_message_event(sender_id, "DECRYPTED", "AES-256 payload decrypted successfully")
            except Exception as e:
                print(f"[SECURITY LAYER ERROR] Decryption failed: {e}")
                log_message_event(sender_id, "DECRYPT_FAILED", str(e), status="FAILED")
                return {"ok": False, "error": f"Decryption failed: {str(e)}"}

        # 3. Custom Payload Logging & Defaults
        log.info("Received submit payload: %s", body)
        if "timestamp" not in body:
            body["timestamp"] = time.time()

        # 4. Hand off to the pipeline module
        res = self.pipeline.handle_submit(body, query_string=query_string)
        log_message_event(sender_id, "PROCESSED", res.get("status", "COMPLETED"))
        return res

    def on_route(self, body: dict) -> Dict[str, object]:
        sender_id = str(body.get("sender_id", body.get("node_id", "unknown")))
        log_message_event(sender_id, "FORWARDED", str(body.get("prompt", body))[:80])
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
    print("  http://localhost:{}/health      state, congestion & channel".format(node.http_port))
    print("  http://localhost:{}/peers       cluster view".format(node.http_port))
    print("  http://localhost:{}/connections active link history".format(node.http_port))
    print("  http://localhost:{}/metrics     full snapshot".format(node.http_port))
    print("  http://localhost:{}/messages    message telemetry".format(node.http_port))
    print("  press Ctrl-C to stop\n")
    node.wait()
    print("node stopped.")


if __name__ == "__main__":
    main()