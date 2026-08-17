"""Node identity and hardware capability reporting.

Every cluster member runs the identical codebase; the only thing that
distinguishes one laptop from another is its ``NodeIdentity``. Identity is
used for:

- Stable cluster membership (peer tables are keyed by ``node_id``).
- Capability-aware routing (a node advertises what it can run).
- Traceability of every payload's route path (``x-node-id`` markers).
"""

from __future__ import annotations

import getpass
import os
import platform
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional


def _short_uuid() -> str:
    """Return a compact, collision-resistant id (12 hex chars)."""
    return uuid.uuid4().hex[:12]


def _hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown-host"


def _cpu_count() -> int:
    try:
        return os.cpu_count() or 1
    except Exception:
        return 1


def _mem_total_bytes() -> Optional[int]:
    """Total physical RAM in bytes, best effort across OSes."""
    try:
        if os.path.exists("/proc/meminfo"):
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return kb * 1024
        if platform.system() == "Darwin":
            out = _run(["sysctl", "-n", "hw.memsize"])
            if out is not None:
                return int(out.strip())
        if platform.system() == "Windows":
            try:
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
                    return int(st.ullTotalPhys)
            except Exception:
                pass
    except Exception:
        pass
    return None


def _run(argv: List[str]) -> Optional[str]:
    """Run a subprocess and return stdout, or None on any failure."""
    try:
        import subprocess

        return subprocess.run(
            argv, capture_output=True, text=True, timeout=3
        ).stdout
    except Exception:
        return None


@dataclass
class NodeIdentity:
    """Static facts a node advertises to the cluster."""

    node_id: str
    hostname: str
    user: str
    started_at: float
    capabilities: Dict[str, object] = field(default_factory=dict)

    @classmethod
    def create(cls, node_id: str, name: str) -> "NodeIdentity":
        """Build an identity; ``node_id="auto"`` generates a stable-ish id."""
        host = _hostname()
        if not node_id or node_id.lower() == "auto":
            node_id = "{}-{}-{}".format(name or "node", host, _short_uuid())
        caps = {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": _cpu_count(),
            "mem_total_bytes": _mem_total_bytes(),
            "mem_total_mb": (
                int(_mem_total_bytes() / (1024 * 1024))
                if _mem_total_bytes()
                else None
            ),
        }
        return cls(
            node_id=node_id,
            hostname=host,
            user=getpass.getuser(),
            started_at=time.time(),
            capabilities=caps,
        )

    def summary(self) -> Dict[str, object]:
        return {
            "node_id": self.node_id,
            "hostname": self.hostname,
            "user": self.user,
            "started_at": self.started_at,
            "uptime_s": int(time.time() - self.started_at),
            "capabilities": self.capabilities,
        }
