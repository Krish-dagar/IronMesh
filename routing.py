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
    decides whether to handle a request locally or forward it to a peer.
    Actively hunts for the most idle or fastest peer to ensure a true mesh
    topology and distribute work across the network.
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

    def remove_stale_by_addr(self, host: str, port: int, keep_node_id: str) -> None:
        """Evict any peer at (host, port) whose node_id is NOT keep_node_id.

        Called after we receive a heartbeat response with a new node_id from
        an address we already know — meaning the remote node restarted and got
        a new identity.  Without this, the old ghost entry keeps being gossiped
        via PEX and creates duplicate nodes in the dashboard.
        """
        with self._lock:
            stale = [
                nid for nid, p in self._peers.items()
                if p.host == host and p.port == port and nid != keep_node_id
            ]
            for nid in stale:
                del self._peers[nid]

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
        """Return ``(target, reason)`` where target is ``"local"`` or a peer."""
        # Stop routing if it's bounced around too many times
        if not ctx.alive() or ctx.hop_count >= self._max_hops:
            return "local", "ttl-budget-exhausted"

        live = self._table.live()
        candidates = [p for p in live if p.score(self._min_headroom) is not None]

        # No peers available? We have to do it ourselves.
        if not candidates:
            return "local", "no-peer-capacity"

        local_headroom = local_snapshot.headroom() if local_snapshot else 0.0

        if ctx.urgency == "high":
            best = min(candidates, key=lambda p: p.rtt_s)
            # TRUE MESH: If local node is jammed, has queue, or lacks headroom, reroute to fastest peer.
            if local_jammed or (local_snapshot and local_snapshot.queue_depth > 0) or local_headroom < self._min_headroom:
                return best, "urgent-mesh-reroute"
            return "local", "urgent-local-idle"
        else:
            best = max(candidates, key=lambda p: p.snapshot.headroom())
            # Offload bulk work if local node is jammed, queued, or significantly constrained
            if local_jammed:
                return best, "bulk-offload-jammed"
            if local_headroom < self._min_headroom or (local_snapshot and local_snapshot.queue_depth > 0):
                return best, "bulk-mesh-reroute"
            if best.snapshot.headroom() > local_headroom + 25.0 and local_headroom < 60.0:
                return best, "bulk-mesh-reroute"
            return "local", "bulk-local-balanced"

    def validate_hop(self, ctx: Context) -> bool:
        """True if this node may still forward a payload onward."""
        return ctx.alive() and ctx.hop_count <= self._max_hops

    def stats(self) -> Dict[str, object]:
        return {"live_peers": len(self._table.live()), "known_peers": len(self._table.all())}