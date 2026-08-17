"""Adaptive rate limiting: sliding window + token bucket.

Two complementary algorithms, both adjusted by the congestion controller:

1. **Token bucket** - per source, handles bursts. You get ``burst`` tokens
   up front; each request consumes one; the bucket refills at ``refill``
   tokens/second. An urgent request may draw from a small *priority bucket*
   so time-sensitive work is never starved by bulk traffic.

2. **Sliding window** - global aggregate. A deque of request timestamps
   within ``window_s`` seconds bounds the total cluster-inbound velocity
   (traffic velocity detection). Expired timestamps are pruned on read, so
   the window is accurate without a background sweeper.

Both buckets expose ``adapt(factor)`` so the state machine can shrink
refill rates under load (JAMMED -> factor 0.35). The limiter also tracks
1s/30s request rates for the metrics sampler.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Tuple

log = logging.getLogger("node.ratelimit")


class TokenBucket:
    def __init__(self, burst: float, refill_per_s: float, name: str = ""):
        self.capacity = float(burst)
        self.base_refill = float(refill_per_s)
        self.name = name
        self._lock = threading.Lock()
        self._tokens = float(burst)
        self._refill = float(refill_per_s)
        self._ts = time.time()

    def set_refill(self, refill_per_s: float) -> None:
        """Dynamically raise/lower the refill rate (congestion adaptation)."""
        with self._lock:
            self._refill = max(0.0, float(refill_per_s))

    def consume(self, n: float = 1.0) -> Tuple[bool, float, float]:
        """Try to consume ``n`` tokens.

        Returns ``(allowed, remaining_after, wait_seconds)``. ``wait`` is
        how long a retry would take if rejected (0 if allowed).
        """
        with self._lock:
            now = time.time()
            self._tokens = min(self.capacity, self._tokens + (now - self._ts) * self._refill)
            self._ts = now
            if self._tokens >= n:
                self._tokens -= n
                return True, self._tokens, 0.0
            need = n - self._tokens
            wait = need / self._refill if self._refill > 0 else float("inf")
            return False, self._tokens, wait

    def stats(self) -> Dict[str, object]:
        with self._lock:
            return {
                "capacity": self.capacity,
                "refill_per_s": round(self._refill, 3),
                "tokens": round(self._tokens, 3),
            }


class SlidingWindow:
    def __init__(self, limit_per_s: float, window_s: float = 5.0):
        self.limit = float(limit_per_s)
        self.window = float(window_s)
        self._lock = threading.Lock()
        self._hits: Deque[float] = deque()

    def allow(self) -> Tuple[bool, float, float]:
        """Record a hit if under the limit.

        Returns ``(allowed, current_rate, wait)``. ``wait`` is the seconds
        until the oldest hit exits the window (retry hint).
        """
        with self._lock:
            now = time.time()
            cutoff = now - self.window
            while self._hits and self._hits[0] <= cutoff:
                self._hits.popleft()
            rate = len(self._hits) / self.window
            if rate < self.limit:
                self._hits.append(now)
                return True, rate, 0.0
            wait = self._hits[0] - cutoff if self._hits else 0.0
            return False, rate, max(0.0, wait)

    def current_rate(self) -> float:
        with self._lock:
            now = time.time()
            cutoff = now - self.window
            while self._hits and self._hits[0] <= cutoff:
                self._hits.popleft()
            return len(self._hits) / self.window if self._hits else 0.0


class RateLimiter:
    """Combined limiter exposed to the pipeline."""

    def __init__(self, cfg: dict):
        self.default_burst = float(cfg.get("default_burst", 50))
        self.default_refill = float(cfg.get("default_refill", 10))
        self.priority_burst = float(cfg.get("priority_burst", 20))
        self.aggregate_rps = float(cfg.get("aggregate_rps", 200))
        self.window_s = float(cfg.get("window_s", 5))
        self._lock = threading.Lock()
        self._buckets: Dict[str, TokenBucket] = {}
        self._aggregate = SlidingWindow(self.aggregate_rps, self.window_s)
        self._priority = TokenBucket(self.priority_burst, 2.0, "priority")
        self._timestamps: Dict[str, Deque[float]] = defaultdict(deque)
        self._burst_count = 0
        self._factor = 1.0

    def _bucket(self, key: str) -> TokenBucket:
        with self._lock:
            b = self._buckets.get(key)
            if b is None:
                b = TokenBucket(self.default_burst, self.default_refill, key)
                b.set_refill(self.default_refill * self._factor)
                self._buckets[key] = b
                if len(self._buckets) > 10000:
                    # Bounded memory: drop the bucket with the lowest refill
                    # activity by evicting the first (oldest) entry.
                    self._buckets.pop(next(iter(self._buckets)), None)
            return b

    def adapt(self, factor: float) -> None:
        """Scale every per-source refill rate by ``factor`` in (0..1]."""
        with self._lock:
            self._factor = max(0.0, float(factor))
            for bucket in self._buckets.values():
                bucket.set_refill(bucket.base_refill * self._factor)

    def check(self, source: str, urgent: bool = False) -> Tuple[bool, int]:
        """Admission control for one request.

        Returns ``(allowed, retry_after_s)``. Rules:
        - Aggregate sliding window is a hard cap for the node.
        - Per-source token bucket is a soft cap (bursty sources get bounded).
        - Urgent requests fall back to the priority bucket when their own
          source bucket is dry.
        """
        allowed_agg, _, wait_agg = self._aggregate.allow()
        if not allowed_agg:
            return False, max(1, int(wait_agg) + 1)
        ok, _, wait = self._bucket(source).consume(1.0)
        if not ok and urgent:
            ok, _, wait = self._priority.consume(1.0)
        if not ok:
            return False, max(1, int(wait) + 1)
        with self._lock:
            now = time.time()
            self._burst_count += 1
            # Record for rate reporting; keep windows at 1s and 30s.
            for w in (1.0, 30.0):
                d = self._timestamps[w]
                d.append(now)
                while d and d[0] <= now - w:
                    d.popleft()
        return True, 0

    def rates(self) -> Dict[str, float]:
        """Requests/sec measured over 1s and 30s windows."""
        with self._lock:
            now = time.time()
            out: Dict[str, float] = {}
            for w in (1.0, 30.0):
                d = self._timestamps[w]
                while d and d[0] <= now - w:
                    d.popleft()
                out[str(int(w)) + "s"] = len(d) / w
            return out

    def stats(self) -> Dict[str, object]:
        with self._lock:
            return {
                "aggregate_rps": self.aggregate_rps,
                "window_s": self.window_s,
                "aggregate_rate": round(self._aggregate.current_rate(), 3),
                "priority": self._priority.stats(),
                "buckets": len(self._buckets),
                "burst_count": self._burst_count,
            }
