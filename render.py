"""Dynamic rendering & machine-readable schema injection.

After a payload has been parsed and (possibly) routed, the renderer decides
**how** the result is serialized. Output is driven by ``Context``:

- ``schema_hint`` selects the envelope: ``jsonld`` (linked data),
  ``json-schema`` (explicit schema fragment), or ``compact`` (minimal).
- Urgent contexts get a minimal, stream-friendly envelope (fast to parse);
  low-urgency contexts get verbose metadata (useful for offline analysis).
- Every response carries provenance markers (``x-node-id``, ``x-route-path``)
  so a payload's journey across the mesh is reconstructable.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from parser import Context


def _jsonld_envelope(result: Any, ctx: Context, node_id: str, route: List[str]) -> Dict[str, Any]:
    """Linked-data envelope: @context/@type semantics, schema-injectable."""
    return {
        "@context": "https://schema.org/",
        "@type": "LlmDiscoveryResult",
        "x-node-id": node_id,
        "x-route-path": route,
        "x-request-id": ctx.request_id,
        "x-session-id": ctx.session_id,
        "x-urgency": ctx.urgency,
        "x-generated-at": int(time.time()),
        "result": result,
    }


def _json_schema_envelope(result: Any, ctx: Context, node_id: str, route: List[str]) -> Dict[str, Any]:
    """Self-describing envelope: embeds a $schema fragment describing result."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:llm-node:result:v1",
        "title": "LlmDiscoveryResult",
        "type": "object",
        "properties": {
            "data": {"description": "Discovery / personalization payload"},
            "meta": {
                "type": "object",
                "properties": {
                    "node_id": {"type": "string"},
                    "route_path": {"type": "array", "items": {"type": "string"}},
                    "request_id": {"type": "string"},
                    "urgency": {"type": "string"},
                },
            },
        },
        "data": result,
        "meta": {
            "node_id": node_id,
            "route_path": route,
            "request_id": ctx.request_id,
            "urgency": ctx.urgency,
            "generated_at": int(time.time()),
        },
    }


def _compact_envelope(result: Any, ctx: Context, node_id: str, route: List[str]) -> Dict[str, Any]:
    """Minimal envelope for high-velocity / urgent traffic."""
    return {
        "d": result,
        "n": node_id,
        "r": route,
        "u": ctx.urgency,
        "t": int(time.time()),
    }


def _verbose_envelope(result: Any, ctx: Context, node_id: str, route: List[str]) -> Dict[str, Any]:
    """Verbose envelope for low-urgency / analytic traffic."""
    return {
        "status": "ok",
        "node_id": node_id,
        "route_path": route,
        "request_id": ctx.request_id,
        "session_id": ctx.session_id,
        "source": ctx.source,
        "intent": ctx.intent,
        "urgency": ctx.urgency,
        "personalization": ctx.personalization,
        "generated_at": int(time.time()),
        "result": result,
    }


class Renderer:
    """Serialises processing results into a context-appropriate envelope."""

    def __init__(self, cfg: dict):
        self.default_schema = cfg.get("default_schema", "json-schema")
        self.include_metadata = bool(cfg.get("include_metadata", True))
        self._builders = {
            "jsonld": _jsonld_envelope,
            "json-schema": _json_schema_envelope,
            "compact": _compact_envelope,
            "verbose": _verbose_envelope,
        }

    def select_schema(self, ctx: Context) -> str:
        """Resolve the effective schema name for a context."""
        hint = (ctx.schema_hint or "auto").lower()
        if hint in self._builders:
            return hint
        if hint in ("linked-data", "schema-org", "rdf"):
            return "jsonld"
        if hint == "minimal":
            return "compact"
        return self.default_schema

    def render(
        self,
        result: Any,
        ctx: Context,
        node_id: str,
        route: List[str],
    ) -> Dict[str, Any]:
        """Build the final response envelope."""
        schema = self.select_schema(ctx)
        builder = self._builders[schema]
        envelope = builder(result, ctx, node_id, route)

        # Urgent traffic should not carry heavy metadata; strip it unless the
        # operator explicitly asked for verbose output.
        if ctx.urgency == "high" and schema == "verbose":
            envelope = _compact_envelope(result, ctx, node_id, route)
            schema = "compact"

        payload = {
            "status": "ok",
            "schema": schema,
            "node_id": node_id,
            "route_path": route,
            "result": envelope,
        }
        return payload
