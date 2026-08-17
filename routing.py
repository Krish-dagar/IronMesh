"""Peer table & dynamic routing switch.

This module owns the decentralized view of the cluster and the routing
decision itself.

PeerTable
    In-memory registry of every peer node the current node knows about,
    refreshed by heartbeat gossip (``transport.GossipLoop``). Each entry
    stores the peer's last metrics snapshot, a smoothed RTT, and liveness
    (evicted after ``peer_timeout_s`` without a heartbeat).

RoutingSwitch
    Given a ``Context`` (intent urgency) and the current node's own state,
    decides whether to handle a request locally or forward it to a peer:

    - Every live peer is scored: headroom - queue penalty - RTT penalty.
    - High-urgency intents prefer the *fastest* reachable peer (lowest RTT
      among peers with sufficient headroom) so latency stays low.
    - Low-urgency intents prefer the *most idle* peer so bulk work lands
      where capacity is cheapest.
    - If no peer is worth it, fall back to the local queue. The local node
      keeps control when it is HEALTHY and the context is not urgent.
    - TTL / hop-count budgeting (``Context.alive()``) guarantees a payload
      is never forwarded in a loop.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from metrics import MetricsSnapshot
from parser import Context

log = logging.getLogger("node.routing")

# Fields accepted by MetricsSnapshot.__init__; heartbeat payloads may carry
# extra computed keys (load_score, headroom) that we must strip before
# reconstruction.
_METRIC_FIELDS = {
    "timestamp", "cpu_percent", "mem_percent", "net_rx_bps", "net_tx_bps",
    "queue_depth", "req_rate_1s", "req_rate_30s", "active_requests",
}


@dataclass
class Peer:
    node_id: str
    host: str
    port: int
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    rtt_s: float = 0.05  # initial guess, smoothed by heartbeats
    snapshot: Optional[MetricsSnapshot] = None
    fail_count: int = 0

    def alive(self, timeout_s: float, now: float) -> bool:
        return (now - self.last_seen) <= timeout_s

    def score(self, min_headroom: float) -> Optional[float]:
        """Higher is better. Returns None when the peer cannot take work."""
        if self.snapshot is None:
            return None
        headroom = self.snapshot.headroom()
        if headroom < min_headroom:
            return None
        queue_penalty = min(50.0, self.snapshot.queue_depth * 2.0)
        rtt_penalty = min(40.0, self.rtt_s * 4000.0)
        return headroom - queue_penalty - rtt_penalty


class PeerTable:
    def __init__(self, timeout_s: float, logger=None):
        self._timeout = timeout_s
        self._log = logger or log
        self._lock = threading.Lock()
        self._peers: Dict[str, Peer] = {}

    def upsert(self, node_id: str, host: str, port: int) -> Peer:
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is None:
                peer = Peer(node_id=node_id, host=host, port=port)
                self._peers[node_id] = peer
                self._log.info("peer discovered: %s @ %s:%d", node_id, host, port)
            else:
                peer.host = host
                peer.port = port
            peer.last_seen = time.time()
            return peer

    def observe(self, node_id: str, snapshot_dict: dict, rtt_s: float) -> Optional[Peer]:
        """Update a peer from a heartbeat payload."""
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is None:
                return None
            peer.last_seen = time.time()
            peer.rtt_s = 0.7 * peer.rtt_s + 0.3 * max(0.0, rtt_s)  # EMA
            try:
                kwargs = {k: v for k, v in (snapshot_dict or {}).items() if k in _METRIC_FIELDS}
                peer.snapshot = MetricsSnapshot(**kwargs) if kwargs else None
            except Exception:
                peer.snapshot = None
            return peer

    def mark_failure(self, node_id: str) -> None:
        with self._lock:
            peer = self._peers.get(node_id)
            if peer:
                peer.fail_count += 1
                if peer.fail_count >= 3:
                    peer.snapshot = None  # stop routing to it

    def live(self) -> List[Peer]:
        now = time.time()
        with self._lock:
            return [p for p in self._peers.values() if p.alive(self._timeout, now)]

    def get(self, node_id: str) -> Optional[Peer]:
        with self._lock:
            return self._peers.get(node_id)

    def all(self) -> List[Peer]:
        with self._lock:
            return list(self._peers.values())

    def summary(self) -> List[Dict[str, object]]:
        now = time.time()
        with self._lock:
            out = []
            for p in self._peers.values():
                out.append(
                    {
                        "node_id": p.node_id,
                        "host": p.host,
                        "port": p.port,
                        "alive": p.alive(self._timeout, now),
                        "rtt_ms": round(p.rtt_s * 1000, 1),
                        "headroom": (
                            round(p.snapshot.headroom(), 1) if p.snapshot else None
                        ),
                        "state": "unknown",
                        "fail_count": p.fail_count,
                    }
                )
            return out


class RoutingSwitch:
    def __init__(
        self,
        peer_table: PeerTable,
        max_hops: int,
        min_peer_headroom: float,
        node_id_provider,
    ):
        self._table = peer_table
        self._max_hops = max_hops
        self._min_headroom = float(min_peer_headroom)
        self._node_id = node_id_provider

    def decide(
        self,
        ctx: Context,
        local_snapshot: MetricsSnapshot,
        local_jammed: bool,
    ) -> tuple:
        """Return ``(target, reason)`` where target is ``"local"`` or a peer.

        target == "local" means the payload is processed on this node;
        otherwise a Peer to forward to.
        """
        if not ctx.alive() or ctx.hop_count >= self._max_hops:
            return "local", "ttl-budget-exhausted"

        live = self._table.live()
        local_ok = not local_jammed

        if ctx.urgency == "high":
            # Urgent traffic is served by whichever node is fastest to
            # answer. A healthy node is the fastest path for itself, so we
            # keep urgent work local there - offloading healthy nodes caused
            # forwarding loops in the mesh. A jammed node hands urgent work
            # to the fastest peer with headroom instead of queueing it.
            if not local_ok:
                candidates = [p for p in live if p.score(self._min_headroom) is not None]
                if candidates:
                    best = min(candidates, key=lambda p: p.rtt_s)
                    return best, "urgent-offload-local-jammed"
                return "local", "urgent-no-peer-capacity"
            return "local", "urgent-local-healthy"
        else:
            # Bulk / non-urgent: most idle peer (max headroom) absorbs it.
            candidates = [p for p in live if p.score(self._min_headroom) is not None]
            if not candidates:
                return "local", "bulk-no-peer-capacity"
            if local_jammed:
                best = max(candidates, key=lambda p: p.snapshot.headroom())
                return best, "bulk-offload-local-jammed"
            # Local node healthy: keep bulk work local to avoid gratuitous hops.
            if local_snapshot.headroom() >= 30.0:
                return "local", "bulk-local-idle"
            best = max(candidates, key=lambda p: p.snapshot.headroom())
            if best.snapshot.headroom() > local_snapshot.headroom() + 10:
                return best, "bulk-offload-peer-idler"
            return "local", "bulk-local-balanced"

    def validate_hop(self, ctx: Context) -> bool:
        """True if this node may still forward a payload onward."""
        return ctx.alive() and ctx.hop_count <= self._max_hops

    def stats(self) -> Dict[str, object]:
        return {"live_peers": len(self._table.live()), "known_peers": len(self._table.all())}
