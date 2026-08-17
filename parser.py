"""Context & parsing: decode referral strings and AI-assistant HTTP headers.

Generative-AI assistants (chat apps, copilots, agent frameworks) emit two
kinds of inbound metadata we must decode before any routing decision:

1. **Referral strings** - URL query strings / fragments such as::

       ?ref=claude&intent=research&urgency=high&session=s-9f3a
       ?ctx=<base64url-encoded-json>
       #/from/chatgpt?deep_link=1

2. **HTTP headers** - conventional referer/referrer plus assistant-specific
   X-* headers produced by our gateway/client SDK::

       X-Source: claude
       X-Request-Id: req_abc123
       X-Intent-Urgency: high
       X-Personalization-Context: { "locale": "en", "age_group": "adult" }
       Accept-Schema: jsonld
       X-Referrer: https://copilot.example.com/c/xyz

The parser normalises all of this into a typed ``Context`` object that
drives rate-limit priority, routing, and schema selection downstream. Every
decoding step is defensive: garbage input never raises, it degrades to
sensible defaults.
"""

from __future__ import annotations

import base64
import json
import logging
import re
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("node.parser")

# Known assistants / source markers. A referral string may contain these as
# `ref=`, `source=`, `from=` or in the `utm_source` slot.
_KNOWN_SOURCES = [
    "chatgpt",
    "gpt",
    "openai",
    "claude",
    "anthropic",
    "gemini",
    "bard",
    "copilot",
    "perplexity",
    "assistant",
    "agent",
    "llama",
    "mistral",
    "deepseek",
    "groq",
]

# Words that bump inferred urgency when no explicit urgency is provided.
_URGENT_WORDS = re.compile(
    r"\b(urgent|asap|immediately|critical|deadline|expires|now|tonight|today)\b",
    re.IGNORECASE,
)

_HEADER_ALIASES = {
    "x-source": "source",
    "x-referrer": "referrer",
    "x-request-id": "request_id",
    "x-intent-urgency": "urgency",
    "x-urgency": "urgency",
    "x-schema": "schema_hint",
    "accept-schema": "schema_hint",
    "x-session-id": "session_id",
    "x-hop-count": "hop_count",
    "x-ttl": "ttl",
    "x-personalization-context": "personalization",
}


def _coerce_urgency(value: Optional[str]) -> str:
    """Normalise an urgency value to low/medium/high (default medium)."""
    if not value:
        return "medium"
    v = str(value).strip().lower()
    if v in ("high", "urgent", "critical", "asap", "1", "0.9", "0.8"):
        return "high"
    if v in ("low", "relaxed", "0.1", "0.2", "0.3", "0"):
        return "low"
    return "medium"


def _infer_urgency(text: str) -> str:
    """Heuristic urgency from free-text payload content."""
    if text and _URGENT_WORDS.search(text):
        return "high"
    return "medium"


def _detect_source(needle: str) -> Optional[str]:
    """Return a canonical source name if the string references one."""
    low = (needle or "").lower()
    for src in _KNOWN_SOURCES:
        if src in low:
            return src
    return None


def _b64url_decode(value: str) -> Optional[str]:
    """Decode base64url (RFC 4648) with padding tolerance."""
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return None


def parse_referral_string(raw: str) -> Dict[str, str]:
    """Extract structured key/value pairs from any referral string shape.

    Handles: full URLs, bare query strings, fragments, key=value tokens,
    and a base64url-encoded JSON blob passed as ``ctx=`` / ``data=``.
    Returns a flat dict (values are strings).
    """
    out: Dict[str, str] = {}
    if not raw:
        return out
    raw = raw.strip()

    # Split off an optional fragment and query from a full URL.
    qs = raw
    if "#" in raw:
        qs, frag = raw.split("#", 1)
        if "=" in frag:
            out.update({k: v for k, v in urllib.parse.parse_qsl(frag)})
    if "?" in qs:
        _, qs = qs.split("?", 1)

    if "=" in qs:
        # One or more key=value tokens (URL-decoded per token).
        for key, val in urllib.parse.parse_qsl(qs, keep_blank_values=True):
            out[key.lower()] = val
    else:
        # Bare token: try to detect an assistant source, e.g. "claude".
        src = _detect_source(qs)
        if src:
            out["source"] = src

    # Expand a base64url JSON blob.
    for key in ("ctx", "data", "payload"):
        if key in out:
            decoded = _b64url_decode(out[key])
            if decoded:
                try:
                    blob = json.loads(decoded)
                    if isinstance(blob, dict):
                        for k, v in blob.items():
                            out[str(k).lower()] = v
                except Exception:
                    log.debug("unparseable ctx blob for key=%s", key)
    return out


def parse_headers(headers) -> Dict[str, str]:
    """Normalise HTTP headers into context key/value pairs.

    Accepts any object supporting ``.get(name)`` (an http.server request
    handler's headers or a plain dict).
    """
    out: Dict[str, str] = {}
    raw: Dict[str, str] = {}
    try:
        if hasattr(headers, "get_all"):
            for key in headers.keys() or []:
                raw[str(key).lower()] = headers.get(key, "")
        elif hasattr(headers, "get"):
            for key in headers.keys() or []:
                raw[str(key).lower()] = headers.get(key, "")
        elif isinstance(headers, dict):
            raw = {str(k).lower(): str(v) for k, v in headers.items()}
    except Exception:
        return out

    for header, field_name in _HEADER_ALIASES.items():
        if header in raw:
            out[field_name] = raw[header]
    # Conventional referer/referrer fallback.
    referrer = raw.get("referer") or raw.get("x-referrer")
    if referrer:
        out["referrer"] = referrer
    if "x-personalization-context" in raw:
        try:
            blob = json.loads(raw["x-personalization-context"])
            if isinstance(blob, dict):
                out["personalization"] = json.dumps(blob)
        except Exception:
            pass
    return out


@dataclass
class Context:
    """Structured, normalized view of one inbound request."""

    source: str = "unknown"
    intent: str = "general"
    urgency: str = "medium"  # low | medium | high
    session_id: str = ""
    request_id: str = ""
    schema_hint: str = "auto"
    referrer: str = ""
    ttl: int = 5
    hop_count: int = 0
    personalization: Dict[str, object] = field(default_factory=dict)
    signals: Dict[str, str] = field(default_factory=dict)
    raw_query: str = ""
    parsed_at: float = field(default_factory=time.time)

    @property
    def urgency_score(self) -> float:
        return {"low": 0.25, "medium": 0.6, "high": 1.0}[self.urgency]

    def with_hop(self) -> "Context":
        """Return a copy with hop advanced / ttl decremented (forwarding)."""
        ctx = Context(
            source=self.source,
            intent=self.intent,
            urgency=self.urgency,
            session_id=self.session_id,
            request_id=self.request_id,
            schema_hint=self.schema_hint,
            referrer=self.referrer,
            ttl=self.ttl - 1,
            hop_count=self.hop_count + 1,
            personalization=dict(self.personalization),
            signals=dict(self.signals),
            raw_query=self.raw_query,
            parsed_at=self.parsed_at,
        )
        return ctx

    def alive(self) -> bool:
        """False once the TTL budget is spent (routing must stop)."""
        return self.ttl > 0 and self.hop_count < 8

    def to_dict(self) -> Dict[str, object]:
        return {
            "source": self.source,
            "intent": self.intent,
            "urgency": self.urgency,
            "urgency_score": self.urgency_score,
            "session_id": self.session_id,
            "request_id": self.request_id,
            "schema_hint": self.schema_hint,
            "referrer": self.referrer,
            "ttl": self.ttl,
            "hop_count": self.hop_count,
            "personalization": self.personalization,
            "signals": self.signals,
            "parsed_at": self.parsed_at,
        }

    @classmethod
    def from_dict(cls, data: dict, default_ttl: int = 5) -> "Context":
        """Rehydrate a Context forwarded inside a route payload."""
        return cls(
            source=str(data.get("source", "unknown")),
            intent=str(data.get("intent", "general")),
            urgency=str(data.get("urgency", "medium")),
            session_id=str(data.get("session_id", "")),
            request_id=str(data.get("request_id", "")),
            schema_hint=str(data.get("schema_hint", "auto")),
            referrer=str(data.get("referrer", "")),
            ttl=int(data.get("ttl", default_ttl)),
            hop_count=int(data.get("hop_count", 0)),
            personalization=data.get("personalization", {}) or {},
            signals=data.get("signals", {}) or {},
            raw_query=str(data.get("raw_query", "")),
            parsed_at=float(data.get("parsed_at", time.time())),
        )


class ContextParser:
    """Entry point used by the pipeline to build a Context."""

    def build(
        self,
        query_string: str = "",
        headers=None,
        payload_text: str = "",
        default_ttl: int = 5,
    ) -> Context:
        ref = parse_referral_string(query_string or "")
        hdr = parse_headers(headers or {})
        merged: Dict[str, str] = {}
        merged.update(ref)
        merged.update(hdr)

        source = (
            merged.get("source")
            or merged.get("ref")
            or merged.get("from")
            or merged.get("utm_source")
            or _detect_source(merged.get("referrer", ""))
            or "unknown"
        )
        intent = merged.get("intent", "general")
        urgency = _coerce_urgency(merged.get("urgency"))
        if urgency == "medium" and payload_text:
            urgency = _infer_urgency(payload_text)
        schema_hint = (merged.get("schema_hint") or "auto").lower()
        session_id = merged.get("session_id", "")
        request_id = merged.get("request_id", "")
        referrer = merged.get("referrer", "")

        try:
            ttl = max(1, int(merged.get("ttl", default_ttl)))
        except Exception:
            ttl = default_ttl
        try:
            hop = max(0, int(merged.get("hop_count", 0)))
        except Exception:
            hop = 0

        personalization: Dict[str, object] = {}
        if "personalization" in merged:
            try:
                personalization = json.loads(merged["personalization"])
                if not isinstance(personalization, dict):
                    personalization = {}
            except Exception:
                personalization = {"raw": merged["personalization"]}

        signals = {k: v for k, v in merged.items() if k not in {
            "source", "intent", "urgency", "schema_hint", "session_id",
            "request_id", "referrer", "ttl", "hop_count", "personalization",
        }}

        return Context(
            source=source,
            intent=intent,
            urgency=urgency,
            session_id=session_id,
            request_id=request_id,
            schema_hint=schema_hint,
            referrer=referrer,
            ttl=ttl,
            hop_count=hop,
            personalization=personalization,
            signals=signals,
            raw_query=query_string or "",
        )

    @staticmethod
    def rehydrate(data: dict, default_ttl: int = 5) -> "Context":
        """Backwards-compatible alias for ``Context.from_dict``."""
        return Context.from_dict(data, default_ttl=default_ttl)
