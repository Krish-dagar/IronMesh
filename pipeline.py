"""Pipeline orchestrator: ingest -> parse -> throttle -> route -> render.

This is the "brain" that glues the feature modules together for every
inbound request:

    POST /submit  (new request)
        parse referral string + headers  ->  Context
        rate limit (sliding window + token bucket, urgency-aware)
        congestion check (state machine) -> backpressure or shed
        routing switch -> process local OR forward to peer
        local processing (LLM discovery/personalization stage)
        dynamic rendering -> schema-injected envelope

    POST /route   (forwarded request from a peer)
        rehydrate Context, advance hop / decrement TTL
        if routing budget is exhausted -> process locally (terminal hop)
        otherwise the same pipeline runs again

``process_local`` is the deliberate plug-in point: it currently performs a
deterministic, dependency-free stand-in for the discovery/personalization
stage so the whole mesh is demonstrable end-to-end. Swap its body with your
real inference/embedding call.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from parser import Context, ContextParser
from ratelimit import RateLimiter
from render import Renderer
from routing import PeerTable, RoutingSwitch
from state import StateMachine
from transport import HTTPClient

log = logging.getLogger("node.pipeline")


class Pipeline:
    def __init__(
        self,
        node_id: str,
        cfg: dict,
        state: StateMachine,
        limiter: RateLimiter,
        parser: ContextParser,
        renderer: Renderer,
        routing: RoutingSwitch,
        peers: PeerTable,
        http_client: HTTPClient,
        advertised_host: str,
    ):
        self.node_id = node_id
        self._cfg = cfg
        self._state = state
        self._limiter = limiter
        self._parser = parser
        self._renderer = renderer
        self._routing = routing
        self._peers = peers
        self._http = http_client
        self._host = advertised_host
        self._max_work_ms = int((cfg.get("processing") or {}).get("max_work_ms", 50))
        self._max_fwd_bytes = int((cfg.get("routing") or {}).get("max_forward_size_bytes", 2 * 1024 * 1024))

        self._queue_lock = threading.Lock()
        self._active = 0
        self._counters = {"accepted": 0, "rejected": 0, "routed_out": 0, "processed": 0, "forwarded_in": 0}
        self._snapshot_provider = self._default_snapshot

    def _default_snapshot(self):
        from metrics import MetricsSnapshot

        return MetricsSnapshot(timestamp=time.time())

    # -- metrics -----------------------------------------------------------

    def queue_depth(self) -> int:
        with self._queue_lock:
            return self._active

    def rates(self) -> Dict[str, float]:
        return self._limiter.rates()

    def stats(self) -> Dict[str, object]:
        return dict(self._counters)

    def _enter(self) -> None:
        with self._queue_lock:
            self._active += 1

    def _leave(self) -> None:
        with self._queue_lock:
            self._active = max(0, self._active - 1)

    # -- public entry points ----------------------------------------------

    def handle_submit(self, body: dict, query_string: str = "") -> Dict[str, Any]:
        """Entry for POST /submit (new request from a client or assistant)."""
        self._enter()
        try:
            headers = body.get("headers") if isinstance(body.get("headers"), dict) else {}
            referral = body.get("query") or query_string or ""
            text = body.get("text", "")
            payload = body.get("payload", {})
            if isinstance(text, str) and not text and isinstance(payload, dict):
                text = payload.get("text", "")
            ctx = self._parser.build(
                query_string=referral, headers=headers, payload_text=str(text),
                default_ttl=int((self._cfg.get("routing") or {}).get("ttl_default", 5)),
            )
            route = [self.node_id]
            return self._process(ctx, payload, route, is_route=False)
        finally:
            self._leave()

    def handle_route(self, body: dict) -> Dict[str, Any]:
        """Entry for POST /route (payload forwarded by a peer)."""
        self._enter()
        try:
            self._counters["forwarded_in"] += 1
            raw_ctx = body.get("_ctx")
            if not isinstance(raw_ctx, dict):
                return {"status": "error", "error": "missing _ctx", "node_id": self.node_id}
            ctx = Context.from_dict(raw_ctx).with_hop()
            payload = body.get("payload", {})
            route = list(raw_ctx.get("route_path", [self.node_id]))
            route.append(self.node_id)
            return self._process(ctx, payload, route, is_route=True)
        finally:
            self._leave()

    # -- core pipeline -----------------------------------------------------

    def _process(self, ctx: Context, payload: dict, route: List[str], is_route: bool) -> Dict[str, Any]:
        # 1. Rate limit (urgency-aware: high urgency has a priority lane).
        source_key = "{}|{}".format(ctx.source, ctx.session_id or ctx.request_id or "anon")
        allowed, retry = self._limiter.check(source_key, urgent=ctx.urgency == "high")
        if not allowed:
            self._counters["rejected"] += 1
            return {
                "_http_status": 429,
                "status": "rate_limited",
                "retry_after": retry,
                "node_id": self.node_id,
                "route_path": route,
            }

        # 2. Adaptive refill: fold the current congestion state into limits.
        self._limiter.adapt(self._state.state_factor())

        # 3. Routing switch. Jammed nodes shed work to peers BEFORE rejecting,
        # so bulk traffic is re-routed on capacity, not dropped.
        snapshot = self._snapshot_provider()
        target, reason = self._routing.decide(ctx, snapshot, self._state.is_jammed())
        if target != "local":
            self._counters["routed_out"] += 1
            forwarded = self._forward_to(target, ctx, payload, route)
            if forwarded is not None:
                return forwarded
            log.info("forward to %s failed, falling back to local (%s)", target.node_id, reason)

        # 4. Congestion: only refuse locally when overloaded AND no peer took
        # the work; urgent traffic is always admitted (priority lane).
        backoff = self._state.backpressure()
        if backoff and ctx.urgency != "high":
            self._counters["rejected"] += 1
            return {
                "_http_status": 503,
                "status": "busy",
                "retry_after": backoff,
                "node_id": self.node_id,
                "route_path": route,
                "reason": "node-congested",
            }

        # 5. Local processing + rendering.
        self._counters["processed"] += 1
        self._counters["accepted"] += 1
        result = self.process_local(payload, ctx)
        return self._renderer.render(result, ctx, self.node_id, route)

    def _forward_to(self, peer, ctx: Context, payload: dict, route: List[str]) -> Optional[dict]:
        if len(json.dumps(payload).encode("utf-8")) > self._max_fwd_bytes:
            return None
        url = "http://{}:{}/route".format(peer.host, peer.port)
        outbound = {
            "_ctx": {
                **ctx.to_dict(),
                "route_path": route,
            },
            "payload": payload,
        }
        resp, _, ok = self._http.post_json(url, outbound)
        return resp if ok else None

    # -- processing stage (PLUG-IN POINT) -----------------------------------

    def process_local(self, payload: dict, ctx: Context) -> Dict[str, Any]:
        """Deterministic stand-in for the LLM discovery/personalization stage.

        Replace the body of this method with your real model/embedding call.
        The contract: take ``payload`` and ``ctx``, return a JSON-serializable
        result dict.
        """
        text = ""
        if isinstance(payload, dict):
            text = str(payload.get("text", payload.get("prompt", "")))
        elif isinstance(payload, str):
            text = payload

        # Simulated bounded compute (scales gently with input size).
        work_s = min(self._max_work_ms / 1000.0, 0.001 + len(text) * 0.0005)
        time.sleep(work_s)

        seed = "{}|{}".format(self.node_id, text)
        h = hashlib.sha256(seed.encode("utf-8")).digest()

        # Deterministic "model discovery" ranking over a fixed candidate set.
        candidates = [
            ("llm-fast-1", "latency-optimised"),
            ("llm-balanced-2", "general"),
            ("llm-deep-3", "quality-optimised"),
            ("llm-embed-4", "embedding"),
        ]
        ordered = sorted(candidates, key=lambda c: h[(candidates.index(c) * 3) % 32])
        discovery = [
            {"model": name, "role": role, "score": round(0.5 + (b % 40) / 100.0, 3)}
            for (name, role), b in zip(ordered[:3], h)
        ]

        profile = {
            "source": ctx.source,
            "intent": ctx.intent,
            "urgency": ctx.urgency,
            "personalization": ctx.personalization,
        }

        return {
            "echo": text,
            "discovery": discovery,
            "profile": profile,
            "work_ms": round(work_s * 1000, 2),
        }

    # -- snapshot provider hook --------------------------------------------

    def attach_snapshot(self, provider) -> None:
        """Wire the metrics snapshot source (called by node.py)."""
        self._snapshot_provider = provider
