"""Reference client: send discovery/personalization payloads to any node.

Phase 4 Security Layer: Encapsulates ML-KEM-768 headers and encrypts
data payloads using AES-256-GCM before transmission.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.request
from collections import Counter
from typing import Dict, List, Optional

from security import pqc_node


def post_json(url: str, payload: dict, timeout: float = 5.0) -> dict:
    # Phase 4: Encrypt payload before sending over the wire
    encrypted_payload = pqc_node.encrypt_payload(payload)
    
    data = json.dumps(encrypted_payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run(args) -> None:
    headers = {}
    for h in args.header or []:
        if ":" in h:
            k, v = h.split(":", 1)
            headers[k.strip()] = v.strip()

    # Phase 4: Attach PQC ML-KEM Key Exchange Metadata to Headers
    pqc_header = pqc_node.encapsulate_pqc_header("target_node")
    headers["X-PQC-Alg"] = pqc_header["pqc_alg"]
    headers["X-PQC-Ciphertext"] = pqc_header["ciphertext_b64"]
    headers["X-PQC-SharedSecret"] = pqc_header["shared_secret_b64"]

    payload = {
        "query": args.query or "",
        "headers": headers,
        "text": args.text or "hello from llm-node client",
        "payload": {"text": args.text or ""},
    }

    if args.count <= 1:
        t0 = time.time()
        print("\n=== PQC HEADER (ML-KEM-768 Encapsulation) ===")
        print(json.dumps(pqc_header, indent=2))

        print("\n=== AES-256 ENCRYPTED PAYLOAD (IN-TRANSIT LOG) ===")
        encrypted_preview = pqc_node.encrypt_payload(payload)
        print(json.dumps(encrypted_preview, indent=2))

        resp = post_json(args.url, payload)
        print("\n=== SERVER RESPONSE ===")
        print(json.dumps(resp, indent=2, default=str))
        print("\nround-trip: {:.0f} ms".format((time.time() - t0) * 1000))
        return

    # Burst mode: N requests across C concurrent threads.
    results: List[str] = []
    lock = threading.Lock()
    t0 = time.time()

    def worker():
        while True:
            with lock:
                if len(results) >= args.count:
                    return
            try:
                r = post_json(args.url, payload, timeout=10)
                status = r.get("status", "?")
            except Exception as exc:
                status = "error:" + type(exc).__name__
            with lock:
                results.append(status)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    elapsed = max(0.001, time.time() - t0)
    counts = Counter(results)
    print("burst complete: {} requests in {:.2f}s -> {:.1f} req/s".format(
        args.count, elapsed, args.count / elapsed))
    for status, n in counts.most_common():
        print("  {:>6}  {}".format(n, status))


def main() -> None:
    p = argparse.ArgumentParser(description="llm-node reference client with PQC & AES-256")
    p.add_argument("--url", default="http://127.0.0.1:8080/submit", help="node endpoint")
    p.add_argument("--query", default="", help="referral string, e.g. ref=claude&intent=research&urgency=high")
    p.add_argument("--header", action="append", default=[], help="header as K:V (repeatable)")
    p.add_argument("--text", default="hello from llm-node client", help="payload text")
    p.add_argument("--count", type=int, default=1, help="number of requests (burst test)")
    p.add_argument("--concurrency", type=int, default=10, help="threads for burst test")
    args = p.parse_args()
    run(args)


if __name__ == "__main__":
    main()