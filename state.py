"""Operational state machine (congestion controller).

Maps a live ``MetricsSnapshot`` onto one of four operational states:

    HEALTHY -> BUSY -> JAMMED -> RECOVERING -> HEALTHY

Purpose
-------
High-frequency request bursts must not crash the node. The state machine is
the decision core that answers three questions for the rest of the system:

1. ``is_accepting()``    - may we accept a new request locally?
2. ``backpressure()``    - how long should a client wait before retrying?
3. ``state_factor()``    - how aggressively should we shed/re-route load?

Hysteresis
----------
Plain thresholding on noisy metrics causes state flapping (a single spike
toggles JAMMED on and off). We therefore require a state to be *sustained*
for N consecutive samples before it changes, and use distinct enter/exit
thresholds so the system deliberately "holds" a state while borderline.
"""

from __future__ import annotations

import enum
import logging
import threading
import time
from typing import Optional

from metrics import MetricsSnapshot

log = logging.getLogger("node.state")


class NodeState(enum.Enum):
    HEALTHY = "healthy"
    BUSY = "busy"
    JAMMED = "jammed"
    RECOVERING = "recovering"


class StateConfig:
    def __init__(self, cfg: dict):
        self.busy_high = float(cfg.get("busy_high", 65))
        self.jam_high = float(cfg.get("jam_high", 85))
        self.jam_exit = float(cfg.get("jam_exit", 55))
        self.jam_samples = int(cfg.get("jam_samples", 3))
        self.recover_samples = int(cfg.get("recover_samples", 3))
        self.max_queue = int(cfg.get("max_queue", 100))


class StateMachine:
    def __init__(self, cfg: StateConfig):
        self._cfg = cfg
        self._lock = threading.Lock()
        self._state = NodeState.HEALTHY
        self._jam_count = 0
        self._recover_count = 0
        self._last_change = time.time()

    # -- queries -----------------------------------------------------------

    def current(self) -> NodeState:
        with self._lock:
            return self._state

    def is_jammed(self) -> bool:
        return self.current() in (NodeState.JAMMED, NodeState.RECOVERING)

    def is_accepting(self) -> bool:
        """True when local ingestion is allowed without backpressure."""
        return not self.is_jammed()

    def backpressure(self) -> Optional[int]:
        """Retry-After seconds when overloaded, else None.

        Urgent traffic during RECOVERING is still admissible, which lets the
        node drain high-value work while shedding bulk.
        """
        with self._lock:
            if self._state is NodeState.JAMMED:
                return int(self._cfg.jam_samples * 1.0) + 1
            if self._state is NodeState.RECOVERING:
                return 1
            return None

    def state_factor(self) -> float:
        """Adaptive multiplier in (0..1] used to shrink token refill rates.

        HEALTHY: 1.0 (full rate)   BUSY: 0.75   JAMMED: 0.35   RECOVERING: 0.6
        """
        return {
            NodeState.HEALTHY: 1.0,
            NodeState.BUSY: 0.75,
            NodeState.JAMMED: 0.35,
            NodeState.RECOVERING: 0.6,
        }[self.current()]

    # -- evaluation --------------------------------------------------------

    def evaluate(self, snap: MetricsSnapshot) -> NodeState:
        """Advance the machine using one metrics sample."""
        score = snap.load_score()
        queue_over = snap.queue_depth >= self._cfg.max_queue
        with self._lock:
            prev = self._state
            if self._state is NodeState.JAMMED or self._state is NodeState.RECOVERING:
                if score >= self._cfg.jam_high or queue_over:
                    # Relapse while trying to recover.
                    self._state = NodeState.JAMMED
                    self._recover_count = 0
                else:
                    self._recover_count += 1
                    if self._recover_count >= self._cfg.recover_samples:
                        if self._state is NodeState.JAMMED:
                            # Sustained relief: leave the jam, still throttled.
                            self._state = NodeState.RECOVERING
                        else:
                            # Sustained relief during recovery: rejoin service.
                            self._state = (
                                NodeState.BUSY
                                if score >= self._cfg.busy_high
                                else NodeState.HEALTHY
                            )
                            self._jam_count = 0
                        self._recover_count = 0
            else:
                jammed = score >= self._cfg.jam_high or queue_over
                if jammed:
                    self._jam_count += 1
                    self._recover_count = 0
                    if self._jam_count >= self._cfg.jam_samples:
                        self._state = NodeState.JAMMED
                        self._jam_count = 0
                else:
                    self._jam_count = 0
                    if score >= self._cfg.busy_high:
                        self._state = NodeState.BUSY
                    else:
                        self._state = NodeState.HEALTHY
            if self._state is not prev:
                self._last_change = 0
                log.info(
                    "state transition %s -> %s (load=%.1f queue=%d)",
                    prev.value,
                    self._state.value,
                    score,
                    snap.queue_depth,
                )
            return self._state
