# llm-node — Distributed LLM Discovery & Personalization Node

Procedure:
for host:
1.	.\start.bat
2.	Check the wifi lan adapter ipv4 and save it as host ip
For the machines running the same:
1.	Download the folder as it is
2.	python node.py --peers <host ip>:8080


A single-node implementation for a **fully decentralized mesh** of laptops
(4-6 recommended). Every laptop runs the identical program; the cluster
discovers itself, monitors its own congestion, adaptively rate-limits,
routes work between peers, parses generative-AI referral context, and
renders schema-annotated responses.

Built with **Python 3 standard library only** (optional `psutil` for richer
metrics). No package installs, no build step.

## What one node does

```
inbound ──> parse (referral + headers) ──> rate limit (urgency-aware)
   └──> congestion check (state machine) ──> routing switch
           ├── local:  process (LLM discovery/personalization) ──> render ──> reply
           └── peer:   POST /route (gossip keeps the peer table fresh)
```

| Feature | Module | Mechanism |
|---|---|---|
| State monitoring & congestion | `state.py`, `metrics.py` | CPU%/mem/queue sampled on a cadence; FSM `healthy -> busy -> jammed -> recovering` with hysteresis; backpressure (`503` + Retry-After) while jammed |
| Frequency & routing algorithm | `ratelimit.py`, `routing.py` | Sliding window (aggregate velocity) + token bucket (per source, adaptive refill); scoring switch picks fastest peer for urgent, idlest peer for bulk; TTL/hop budget stops loops |
| Context & parsing | `parser.py` | Decodes referral strings (query/fragment/base64url-JSON) and AI-assistant headers into a typed `Context` |
| Dynamic rendering & markup | `render.py` | `jsonld` / `json-schema` / `compact` envelopes, schema injection, provenance (`x-node-id`, `x-route-path`) |

## Quick start (first laptop)

```bash
unzip llm-node.zip
cd llm-node
python3 node.py
```

That's it — the node listens on HTTP `:8080` and broadcasts discovery on
UDP `:48765`.

## Connect the other laptops

**Same Wi-Fi/LAN:** start each laptop the same way. Nodes auto-discover via
UDP broadcast within ~3-5 seconds. Verify with `GET /peers` on any node.

**Different networks / broadcast blocked:** edit `config.json` on every
laptop, or pass them on the command line:

```bash
python3 node.py --peers 192.168.1.20:8080 192.168.1.21:8080 192.168.1.22:8080
```

Seeds are just bootstrapping hints; once connected, nodes gossip and learn
the full cluster on their own.

### Windows

```bat
start.bat
```

## Verify the cluster

```bash
curl http://localhost:8080/health     # this node's state + load
curl http://localhost:8080/peers      # cluster membership view
curl http://localhost:8080/metrics    # full snapshot: metrics, limiter, routing
```

## Send a request

```bash
# reference client
python3 client.py --url http://<node-ip>:8080/submit \
  --query 'ref=claude&intent=research&urgency=high&session=s-1' \
  --header X-Request-Id:req_001 \
  --text 'urgently rank models for this task'

# plain curl
curl -s -X POST http://localhost:8080/submit \
  -H 'Content-Type: application/json' \
  -H 'X-Source: gemini' -H 'X-Intent-Urgency: high' \
  -d '{"text":"hello"}'
```

Request body shape (`POST /submit`):

```json
{
  "query":   "ref=claude&intent=research&urgency=low&session=s-1",
  "headers": {"X-Request-Id": "req_1", "X-Personalization-Context": "{\"locale\":\"en\"}"},
  "text":    "the payload text",
  "payload": { "any": "additional structured data" }
}
```

Response includes `status`, `schema`, `node_id`, `route_path` (every node
the request visited), and the rendered `result`.

### Congestion / rate-limit behaviors

- `429 {"status":"rate_limited","retry_after":N}` — sliding-window or token
  bucket exhausted; retry after `N` seconds.
- `503 {"status":"busy","retry_after":N}` — node is JAMMED and shedding
  load. Urgent traffic is still accepted (priority lane).
- `route_path` with `>1` entries means the request was re-routed to a peer
  with spare capacity.

### Burst test

```bash
python3 client.py --url http://<node-ip>:8080/submit \
  --text 'burst' --count 1000 --concurrency 50
```

Watch a node flip `healthy -> busy -> jammed -> recovering` via
`/health` or `/metrics`.

## Simulate a 3-node cluster on one machine

```bash
cp config.json config-a.json
cp config.json config-b.json
cp config.json config-c.json
# edit: http_port 8081 / 8082 / 8083, name node-a/b/c,
#       peers: ["127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083"]

python3 node.py --config config-a.json
python3 node.py --config config-b.json
python3 node.py --config config-c.json
```

## Configuration (`config.json`)

| Key | Default | Meaning |
|---|---|---|
| `node_id` | `auto` | cluster-unique id (`auto` = generated) |
| `name` | `node` | prefix for generated ids |
| `http_port` | `8080` | data plane port (expose/firewall this) |
| `udp_port` | `48765` | discovery port (UDP, same on all nodes) |
| `udp_broadcast_addr` | `255.255.255.255` | broadcast/multicast target |
| `peers` | `[]` | static seed endpoints `"host:port"` |
| `state.jam_high` | `85` | load % that triggers JAMMED (sustained) |
| `state.jam_exit` | `55` | load % below which jam clears |
| `state.max_queue` | `100` | queue depth that forces JAMMED |
| `limits.default_burst` / `default_refill` | `50` / `10` | per-source token bucket |
| `limits.aggregate_rps` | `200` | global sliding-window velocity cap |
| `routing.max_hops` | `3` | maximum forwarding depth |
| `routing.min_peer_headroom` | `15` | min headroom % to route to a peer |

## Architecture notes

- **Control plane:** UDP broadcast (`announce` frames, ~3s cadence) + TCP
  heartbeats every 2s carrying each node's metrics snapshot (gossip). A peer
  missing heartbeats for `peer_timeout_s` is treated as dead and excluded
  from routing.
- **Congestion hysteresis:** a state must be sustained for N consecutive
  samples to change, and enter/exit thresholds differ, so bursts of noisy
  metrics cannot flap the machine.
- **Adaptive frequency:** the state machine's `state_factor()` scales every
  token bucket's refill rate (1.0 healthy -> 0.35 jammed), so the node
  throttles itself before it crashes.
- **Loop safety:** every forwarded payload carries `_ctx.ttl` /
  `_ctx.hop_count`; routing stops once the budget is spent.

## Test

```bash
python3 -m unittest test_node -v
```

## Files

```
node.py        entry point / wiring / HTTP app callbacks
pipeline.py    stage orchestrator (ingest -> parse -> throttle -> route -> render)
parser.py      referral-string + AI-assistant header decoding
ratelimit.py   sliding window + token bucket (adaptive)
routing.py     peer table + dynamic routing switch
state.py       congestion state machine (hysteresis)
metrics.py     host metrics sampler (cpu/mem/network velocity)
render.py      schema selection + envelope injection
identity.py    node id + hardware capability report
transport.py   UDP discovery, HTTP server, heartbeat gossip
client.py      reference test client
config.json    node configuration
test_node.py   unit tests (stdlib unittest)
start.sh       Linux/macOS launcher
start.bat      Windows launcher
```

## Troubleshooting

- **Nodes don't discover each other:** confirm same subnet / UDP 48765
  allowed, or set `peers` in `config.json` explicitly.
- **Firewall:** allow inbound TCP on `http_port` and UDP on `udp_port`.
- **Across the internet:** static `peers` + port-forward `http_port` on each
  laptop (control-plane broadcast stays LAN-only by design).
- **Load pegged at 100%:** that's the point — it enters JAMMED and starts
  re-routing to peers. Lower `state.jam_high` if it should shed earlier.
