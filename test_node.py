"""Unit tests for the llm-node feature modules.

Run with:  python3 -m unittest test_node -v
Stdlib unittest only - no third-party test framework required.
"""

from __future__ import annotations

import unittest

from metrics import MetricsSnapshot
from parser import Context, ContextParser, parse_referral_string
from ratelimit import RateLimiter, SlidingWindow, TokenBucket
from render import Renderer
from routing import PeerTable, RoutingSwitch
from state import StateConfig, StateMachine


def snapshot(cpu=0.0, mem=0.0, queue=0):
    return MetricsSnapshot(
        timestamp=0.0, cpu_percent=cpu, mem_percent=mem, queue_depth=queue,
    )


# --------------------------------------------------------------------------
# parser.py
# --------------------------------------------------------------------------

class TestParser(unittest.TestCase):
    def test_referral_query(self):
        out = parse_referral_string("?ref=claude&intent=research&urgency=high&session=s-1")
        self.assertEqual(out["intent"], "research")
        self.assertEqual(out["urgency"], "high")
        self.assertEqual(out["session"], "s-1")

    def test_referral_bare_token(self):
        out = parse_referral_string("claude")
        self.assertEqual(out.get("source"), "claude")

    def test_referral_base64_ctx(self):
        import base64, json

        blob = base64.urlsafe_b64encode(json.dumps({"intent": "coding"}).encode()).decode()
        out = parse_referral_string("?ctx=" + blob)
        self.assertEqual(out.get("intent"), "coding")

    def test_build_context_from_headers(self):
        p = ContextParser()
        ctx = p.build(
            query_string="ref=gemini&urgency=low",
            headers={
                "X-Request-Id": "req_1",
                "X-Personalization-Context": '{"locale":"en"}',
            },
            payload_text="hello",
        )
        self.assertEqual(ctx.source, "gemini")
        self.assertEqual(ctx.urgency, "low")
        self.assertEqual(ctx.request_id, "req_1")
        self.assertEqual(ctx.personalization, {"locale": "en"})

    def test_urgency_inference_from_text(self):
        p = ContextParser()
        ctx = p.build(payload_text="this is urgent, deadline today")
        self.assertEqual(ctx.urgency, "high")
        ctx2 = p.build(payload_text="a calm request")
        self.assertEqual(ctx2.urgency, "medium")

    def test_hop_advance(self):
        ctx = Context(ttl=3, hop_count=1)
        self.assertTrue(ctx.alive())
        nxt = ctx.with_hop()
        self.assertEqual(nxt.ttl, 2)
        self.assertEqual(nxt.hop_count, 2)
        dead = Context(ttl=0)
        self.assertFalse(dead.alive())


# --------------------------------------------------------------------------
# ratelimit.py
# --------------------------------------------------------------------------

class TestRateLimit(unittest.TestCase):
    def test_token_bucket_burst(self):
        b = TokenBucket(burst=5, refill_per_s=1.0)
        for _ in range(5):
            ok, _, _ = b.consume()
            self.assertTrue(ok)
        ok, _, _ = b.consume()
        self.assertFalse(ok)

    def test_sliding_window_cap(self):
        w = SlidingWindow(limit_per_s=4.0, window_s=1.0)
        allowed = 0
        for _ in range(6):
            ok, _, _ = w.allow()
            allowed += int(ok)
        self.assertEqual(allowed, 4)

    def test_limiter_rejects_when_burst_spent(self):
        rl = RateLimiter({"default_burst": 2, "default_refill": 1, "aggregate_rps": 1000})
        self.assertTrue(rl.check("src", urgent=False)[0])
        self.assertTrue(rl.check("src", urgent=False)[0])
        ok, retry = rl.check("src", urgent=False)
        self.assertFalse(ok)
        self.assertGreater(retry, 0)

    def test_limiter_priority_lane(self):
        rl = RateLimiter({"default_burst": 1, "default_refill": 0.1, "priority_burst": 3, "aggregate_rps": 1000})
        rl.check("src", urgent=False)
        ok, _ = rl.check("src", urgent=False)
        self.assertFalse(ok)
        ok, _ = rl.check("src", urgent=True)  # falls back to priority bucket
        self.assertTrue(ok)

    def test_adapt_shrinks_refill(self):
        rl = RateLimiter({"default_burst": 10, "default_refill": 10, "aggregate_rps": 1000})
        rl.adapt(0.35)
        self.assertEqual(rl._bucket("x").base_refill, 10.0)
        self.assertAlmostEqual(rl._bucket("x")._refill, 3.5)


# --------------------------------------------------------------------------
# state.py
# --------------------------------------------------------------------------

class TestStateMachine(unittest.TestCase):
    def _sm(self):
        return StateMachine(
            StateConfig(
                {"busy_high": 65, "jam_high": 85, "jam_exit": 55,
                 "jam_samples": 2, "recover_samples": 2}
            )
        )

    def test_healthy_by_default(self):
        sm = self._sm()
        sm.evaluate(snapshot(cpu=10))
        self.assertEqual(sm.current().value, "healthy")

    def test_jammed_requires_sustained_load(self):
        sm = self._sm()
        sm.evaluate(snapshot(cpu=90))   # 1st high sample: count only
        self.assertNotEqual(sm.current().value, "jammed")
        sm.evaluate(snapshot(cpu=90))   # 2nd high sample: jam
        self.assertEqual(sm.current().value, "jammed")
        self.assertTrue(sm.is_jammed())
        self.assertFalse(sm.is_accepting())
        self.assertIsNotNone(sm.backpressure())

    def test_recovery_sequence(self):
        sm = self._sm()
        sm.evaluate(snapshot(cpu=95))
        sm.evaluate(snapshot(cpu=95))
        self.assertEqual(sm.current().value, "jammed")
        sm.evaluate(snapshot(cpu=40))   # relieved once
        self.assertEqual(sm.current().value, "jammed")
        sm.evaluate(snapshot(cpu=40))   # sustained relief -> recovering
        self.assertEqual(sm.current().value, "recovering")
        sm.evaluate(snapshot(cpu=40))
        sm.evaluate(snapshot(cpu=40))   # recovered -> healthy
        self.assertEqual(sm.current().value, "healthy")

    def test_queue_depth_can_jam(self):
        sm = self._sm()
        sm.evaluate(snapshot(queue=999))
        sm.evaluate(snapshot(queue=999))
        self.assertEqual(sm.current().value, "jammed")

    def test_state_factor_scales(self):
        sm = self._sm()
        sm.evaluate(snapshot(cpu=95))
        sm.evaluate(snapshot(cpu=95))
        self.assertLess(sm.state_factor(), 0.5)


# --------------------------------------------------------------------------
# routing.py
# --------------------------------------------------------------------------

class TestRouting(unittest.TestCase):
    def _setup(self):
        table = PeerTable(timeout_s=60)
        rsw = RoutingSwitch(table, max_hops=3, min_peer_headroom=15, node_id_provider=lambda: "me")
        return table, rsw

    def test_routes_to_peer_when_local_jammed(self):
        table, rsw = self._setup()
        table.upsert("peerA", "10.0.0.2", 8080)
        table.observe("peerA", {"cpu_percent": 10, "mem_percent": 20,
                                "net_rx_bps": 0, "net_tx_bps": 0,
                                "queue_depth": 1, "req_rate_1s": 1.0,
                                "req_rate_30s": 1.0, "active_requests": 0,
                                "timestamp": 0.0}, rtt_s=0.01)
        ctx = Context(urgency="high")
        target, reason = rsw.decide(ctx, snapshot(cpu=99), local_jammed=True)
        self.assertEqual(target.node_id, "peerA")

    def test_keeps_local_when_idle(self):
        table, rsw = self._setup()
        table.upsert("peerA", "10.0.0.2", 8080)
        table.observe("peerA", {"cpu_percent": 10, "mem_percent": 20,
                                "net_rx_bps": 0, "net_tx_bps": 0,
                                "queue_depth": 1, "req_rate_1s": 1.0,
                                "req_rate_30s": 1.0, "active_requests": 0,
                                "timestamp": 0.0}, rtt_s=0.01)
        ctx = Context(urgency="low")
        target, reason = rsw.decide(ctx, snapshot(cpu=20), local_jammed=False)
        self.assertEqual(target, "local")

    def test_ttl_exhaustion_goes_local(self):
        table, rsw = self._setup()
        ctx = Context(ttl=0, hop_count=5)
        target, reason = rsw.decide(ctx, snapshot(cpu=50), local_jammed=False)
        self.assertEqual(target, "local")
        self.assertEqual(reason, "ttl-budget-exhausted")


# --------------------------------------------------------------------------
# render.py
# --------------------------------------------------------------------------

class TestRenderer(unittest.TestCase):
    def _renderer(self, default="json-schema"):
        return Renderer({"default_schema": default, "include_metadata": True})

    def test_default_json_schema(self):
        r = self._renderer()
        out = r.render({"x": 1}, Context(schema_hint="auto"), "n1", ["n1"])
        self.assertEqual(out["schema"], "json-schema")
        self.assertIn("$schema", out["result"])

    def test_jsonld_selection(self):
        r = self._renderer()
        out = r.render({"x": 1}, Context(schema_hint="jsonld"), "n1", ["n1"])
        self.assertEqual(out["schema"], "jsonld")
        self.assertEqual(out["result"]["@context"], "https://schema.org/")

    def test_urgent_verbose_becomes_compact(self):
        r = self._renderer(default="verbose")
        out = r.render({"x": 1}, Context(schema_hint="verbose", urgency="high"), "n1", ["n1"])
        self.assertEqual(out["schema"], "compact")

    def test_route_path_provenance(self):
        r = self._renderer()
        out = r.render({"x": 1}, Context(schema_hint="jsonld"), "n2", ["n1", "n2"])
        self.assertEqual(out["route_path"], ["n1", "n2"])
        self.assertEqual(out["result"]["x-route-path"], ["n1", "n2"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
