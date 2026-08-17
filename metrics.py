"""Host metrics sampler.

The metrics sampler is the sensory layer of the congestion controller. It
reads host-level signals (CPU %, memory %, network byte velocity) plus
application-level signals (request queue depth and request rate) on a fixed
cadence and publishes a thread-safe ``MetricsSnapshot``.

Where a rich library (``psutil``) is present we use it; otherwise we fall
back to portable ``/proc`` parsing (Linux), ``sysctl`` (macOS) and ctypes
(Windows). Every reader is defensive: a failure degrades to ``None`` instead
of crashing the node.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

try:  # optional but preferred
    import psutil  # type: ignore

    _HAS_PSUTIL = True
except Exception:  # pragma: no cover - depends on the host
    _HAS_PSUTIL = False


def _run(argv):
    """Run a short subprocess and return stdout or None."""
    try:
        import subprocess

        return subprocess.run(
            argv, capture_output=True, text=True, timeout=3
        ).stdout
    except Exception:
        return None


def _read_proc_stat() -> Optional[Dict[str, float]]:
    """Return aggregated CPU counters from /proc/stat."""
    try:
        with open("/proc/stat") as fh:
            line = fh.readline()
        parts = line.split()
        if len(parts) < 5:
            return None
        vals = [float(x) for x in parts[1:]]
        idle = vals[3] + (vals[4] if len(vals) > 4 else 0.0)
        return {"total": sum(vals), "idle": idle}
    except Exception:
        return None


def _cpu_percent() -> Optional[float]:
    """CPU usage percent (0..100), sampled over a 0.5s interval."""
    if _HAS_PSUTIL:
        try:
            return psutil.cpu_percent(interval=0.5)
        except Exception:
            pass
    # Linux: delta over two reads of /proc/stat.
    a = _read_proc_stat()
    if a is not None:
        time.sleep(0.5)
        b = _read_proc_stat()
        if b is not None:
            dt = b["total"] - a["total"]
            if dt > 0:
                return max(0.0, min(100.0, 100.0 * (1 - (b["idle"] - a["idle"]) / dt)))
    # Darwin: load average normalised by core count.
    out = _run(["sysctl", "-n", "vm.loadavg"])
    if out is not None:
        try:
            la1 = float(out.split()[0])
            cores = float(os.cpu_count() or 1)
            return max(0.0, min(100.0, la1 / cores * 100.0))
        except Exception:
            pass
    return None


def _mem_percent() -> Optional[float]:
    """Memory usage percent (0..100)."""
    if _HAS_PSUTIL:
        try:
            return psutil.virtual_memory().percent
        except Exception:
            pass
    try:
        if os.path.exists("/proc/meminfo"):
            total = avail = None
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemTotal:"):
                        total = float(line.split()[1])
                    elif line.startswith("MemAvailable:"):
                        avail = float(line.split()[1])
                    if total is not None and avail is not None:
                        break
            if total and avail is not None:
                return max(0.0, min(100.0, 100.0 * (1 - avail / total)))
            if total:
                # MemAvailable missing (old kernel): approximate with free+cached.
                free = cached = None
                with open("/proc/meminfo") as fh:
                    for line in fh:
                        if line.startswith("MemFree:"):
                            free = float(line.split()[1])
                        elif line.startswith("Cached:"):
                            cached = float(line.split()[1])
                        if free is not None and cached is not None:
                            break
                if free is not None and cached is not None:
                    return max(0.0, min(100.0, 100.0 * (1 - (free + cached) / total)))
        if os.name == "nt":
            import ctypes

            class _MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            st = _MEMORYSTATUSEX()
            st.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st)):
                return float(st.dwMemoryLoad)
    except Exception:
        pass
    return None


def _net_dev_counters() -> Optional[Dict[str, int]]:
    """Sum of rx/tx bytes across non-loopback interfaces (/proc/net/dev)."""
    try:
        rx = tx = 0
        with open("/proc/net/dev") as fh:
            for line in fh.readlines()[2:]:
                head, _, rest = line.partition(":")
                if not rest:
                    continue
                if head.strip() == "lo":
                    continue
                fields = rest.split()
                rx += int(fields[0])
                tx += int(fields[8])
        return {"rx": rx, "tx": tx}
    except Exception:
        return None


def _net_velocity_bps() -> Dict[str, int]:
    """Per-second rx/tx bytes, or zeros when unavailable."""
    if _HAS_PSUTIL:
        try:
            c0 = psutil.net_io_counters()
            time.sleep(0.5)
            c1 = psutil.net_io_counters()
            dt = max(0.5, 0.5)
            return {
                "rx_bps": max(0, (c1.bytes_recv - c0.bytes_recv) // int(dt)),
                "tx_bps": max(0, (c1.bytes_sent - c0.bytes_sent) // int(dt)),
            }
        except Exception:
            pass
    return {"rx_bps": 0, "tx_bps": 0}


@dataclass
class MetricsSnapshot:
    """Immutable point-in-time view of node health."""

    timestamp: float
    cpu_percent: Optional[float] = None
    mem_percent: Optional[float] = None
    net_rx_bps: int = 0
    net_tx_bps: int = 0
    queue_depth: int = 0
    req_rate_1s: float = 0.0
    req_rate_30s: float = 0.0
    active_requests: int = 0

    def load_score(self) -> float:
        """Normalised 0..100 composite load used by the state machine.

        - ``cpu_percent`` is the primary host signal.
        - ``queue_depth`` is the primary application signal (5 pts per item,
          capped at 100).
        - Memory contributes only its *overage* above 85% (max 15 pts) so a
          busy but not critically-pressured host is not misclassified by
          noisy mem readings in shared containers.
        """
        cpu = self.cpu_percent if self.cpu_percent is not None else 0.0
        mem = self.mem_percent if self.mem_percent is not None else 0.0
        q = min(100.0, self.queue_depth * 5.0)
        mem_overage = max(0.0, mem - 85.0)
        return max(cpu, q, mem_overage)

    def headroom(self) -> float:
        """Free capacity in percent; 100 = idle, 0 = saturated."""
        return max(0.0, 100.0 - self.load_score())

    def to_dict(self) -> Dict[str, object]:
        return {
            "timestamp": self.timestamp,
            "cpu_percent": self.cpu_percent,
            "mem_percent": self.mem_percent,
            "net_rx_bps": self.net_rx_bps,
            "net_tx_bps": self.net_tx_bps,
            "queue_depth": self.queue_depth,
            "req_rate_1s": self.req_rate_1s,
            "req_rate_30s": self.req_rate_30s,
            "active_requests": self.active_requests,
            "load_score": round(self.load_score(), 2),
            "headroom": round(self.headroom(), 2),
        }


class MetricsSampler(threading.Thread):
    """Background thread that refreshes the node's snapshot on a cadence."""

    def __init__(
        self,
        interval_s: float,
        queue_depth_provider: Callable[[], int],
        rate_provider: Callable[[], Dict[str, float]],
        logger,
        on_snapshot: Optional[Callable[["MetricsSnapshot"], None]] = None,
    ):
        super().__init__(name="metrics-sampler", daemon=True)
        self._interval = max(0.5, float(interval_s))
        self._queue_depth = queue_depth_provider
        self._rate_provider = rate_provider
        self._on_snapshot = on_snapshot
        self._log = logger
        self._lock = threading.Lock()
        self._snapshot = MetricsSnapshot(timestamp=time.time())

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return self._snapshot

    def run(self) -> None:
        while True:
            try:
                rates = self._rate_provider() or {}
                snap = MetricsSnapshot(
                    timestamp=time.time(),
                    cpu_percent=_cpu_percent(),
                    mem_percent=_mem_percent(),
                    net_rx_bps=_net_velocity_bps()["rx_bps"],
                    net_tx_bps=_net_velocity_bps()["tx_bps"],
                    queue_depth=self._queue_depth(),
                    req_rate_1s=rates.get("1s", 0.0),
                    req_rate_30s=rates.get("30s", 0.0),
                    active_requests=self._queue_depth(),
                )
                with self._lock:
                    self._snapshot = snap
            except Exception as exc:  # never let sampling kill the node
                self._log.warning("metrics sample failed: %s", exc)
            if self._on_snapshot is not None:
                try:
                    self._on_snapshot(self.snapshot())
                except Exception as exc:
                    self._log.warning("snapshot callback failed: %s", exc)
            time.sleep(self._interval)
