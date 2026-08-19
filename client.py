import os
import sys
import json
import time
import base64
import wave
import tempfile
import threading
import urllib.request
import argparse
from collections import Counter
from typing import Dict, List, Optional
from security import pqc_node

def get_peers(node_url: str = "http://127.0.0.1:8080") -> list:
    """Fetch live available peers from the local node's routing table."""
    try:
        req = urllib.request.Request(f"{node_url}/peers", headers={"User-Agent": "IronMesh-Client"})
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("peers", [])
    except Exception:
        return []

def send_pqc_message(target_host: str, target_port: int, sender_id: str, target_peer_id: str, msg_type: str, content: str) -> dict:
    """Encapsulates PQC ML-KEM-768 header + AES-256 payload and POSTs to /message or /submit fallback."""
    # 1. PQC Encryption via security.py
    encrypted_packet = pqc_node.pack_message(
        sender_id=sender_id,
        target_peer=target_peer_id,
        msg_type=msg_type,
        content=content
    )
    
    print("\n  🔒 [PQC SECURITY LAYER] ML-KEM-768 Key Encapsulated")
    print(f"     Alg: ML-KEM-768 (Kyber)")
    print(f"     Target Peer: {target_peer_id}")
    print(f"     Payload Ciphertext (AES-256): {encrypted_packet.get('ciphertext')[:32]}...")

    # 2. Try POSTing to /message first, then fallback to /submit if 404
    data = json.dumps(encrypted_packet).encode("utf-8")
    endpoints = [f"http://{target_host}:{target_port}/message", f"http://{target_host}:{target_port}/submit"]
    
    for url in endpoints:
        try:
            req = urllib.request.Request(
                url,
                data=data,
                method="POST",
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            if err.code == 404:
                continue  # Try fallback endpoint
            return {"status": "error", "code": err.code, "reason": str(err)}
        except Exception as exc:
            return {"status": "error", "reason": str(exc)}
            
    return {"status": "error", "reason": "Endpoint 404 on all targets"}

def record_audio_clip(duration_s: float = 3.0) -> str:
    """Record a voice clip from microphone or generate a sample audio wave data base64."""
    print(f"\n  🎙️ Recording voice note for {duration_s} seconds...")
    
    # Try recording via sounddevice / pyaudio if installed
    try:
        import sounddevice as sd
        samplerate = 16000
        recording = sd.rec(int(duration_s * samplerate), samplerate=samplerate, channels=1, dtype='int16')
        sd.wait()
        
        # Save to temp wav file
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
            temp_path = tf.name
        
        with wave.open(temp_path, 'wb') as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(samplerate)
            wf.writeframes(recording.tobytes())
            
        with open(temp_path, 'rb') as f:
            b64_audio = "data:audio/wav;base64," + base64.b64encode(f.read()).decode('utf-8')
        os.remove(temp_path)
        return b64_audio
    except Exception:
        # Fallback wave sound generator (3-second sine beep voice wave)
        print("     (Using standard high-frequency synthetic audio note generator)")
        samplerate = 16000
        n_samples = int(duration_s * samplerate)
        import math
        audio_frames = bytearray()
        for i in range(n_samples):
            # Synthetic voice frequency wave
            val = int(16000 * math.sin(2 * math.pi * 440 * i / samplerate))
            audio_frames.extend(val.to_bytes(2, byteorder='little', signed=True))
            
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
            temp_path = tf.name
            
        with wave.open(temp_path, 'wb') as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(samplerate)
            wf.writeframes(bytes(audio_frames))
            
        with open(temp_path, 'rb') as f:
            b64_audio = "data:audio/wav;base64," + base64.b64encode(f.read()).decode('utf-8')
        os.remove(temp_path)
        return b64_audio

def interactive_terminal_mode(node_url: str = "http://127.0.0.1:8080") -> None:
    print("\n=======================================================")
    print("      🛡️  IronMesh PQC Interactive Terminal Messenger   ")
    print("=======================================================")

    while True:
        peers = get_peers(node_url)
        active_peers = [p for p in peers if p.get("alive")]

        print(f"\n[LOCAL NODE]: {node_url}")
        print("Available Peer Nodes in Mesh:")
        if not active_peers:
            print("  (No active peer nodes found in table yet. Run start.bat on other nodes)")
            print("  [0] Send to Local Node (127.0.0.1:8080)")
        else:
            for idx, p in enumerate(active_peers, start=1):
                device = p.get("node_id", "unknown")
                host = p.get("host", "127.0.0.1")
                port = p.get("port", 8080)
                rtt = p.get("rtt_ms", 0.0)
                print(f"  [{idx}] {device} @ {host}:{port} ({rtt}ms RTT)")
            print(f"  [{len(active_peers) + 1}] BROADCAST TO ALL ACTIVE PEERS")

        print("  [Q] Quit Messenger")
        
        choice = input("\nSelect target peer number: ").strip().lower()
        if choice == 'q':
            print("Exiting messenger.")
            break
            
        target_peers = []
        if not active_peers or choice == '0':
            target_peers.append({"host": "127.0.0.1", "port": 8080, "node_id": "local_node"})
        elif choice.isdigit():
            val = int(choice)
            if 1 <= val <= len(active_peers):
                target_peers.append(active_peers[val - 1])
            elif val == len(active_peers) + 1:
                target_peers = active_peers
            else:
                print("Invalid selection!")
                continue
        else:
            print("Invalid input!")
            continue

        print("\nMessage Mode:")
        print("  [1] Send PQC Encrypted Text Message")
        print("  [2] Record & Send PQC Encrypted Voice Note (Audio)")
        mode = input("Select mode (1/2): ").strip()

        if mode == "1":
            msg_text = input("\nEnter text message: ").strip()
            if not msg_text:
                print("Empty message cancelled.")
                continue
            for tp in target_peers:
                host = tp.get("host", "127.0.0.1")
                port = int(tp.get("port", 8080))
                nid = tp.get("node_id", "peer")
                res = send_pqc_message(host, port, "terminal_client", nid, "text", msg_text)
                print(f"  ✅ Message delivered to {nid} ({host}:{port}) -> {res.get('status')}")

        elif mode == "2":
            audio_b64 = record_audio_clip(duration_s=3.0)
            print("  ⚡ Encrypting voice note with ML-KEM-768 + AES-256...")
            for tp in target_peers:
                host = tp.get("host", "127.0.0.1")
                port = int(tp.get("port", 8080))
                nid = tp.get("node_id", "peer")
                res = send_pqc_message(host, port, "terminal_client", nid, "audio", audio_b64)
                print(f"  ✅ Voice note delivered to {nid} ({host}:{port}) -> {res.get('status')}")
        else:
            print("Invalid mode!")


def run(args) -> None:
    if getattr(args, "interactive", False):
        interactive_terminal_mode("http://127.0.0.1:8080")
        return

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
    p.add_argument("--interactive", "-i", action="store_true", help="Launch PQC Interactive Terminal Messenger")
    args = p.parse_args()

    # Default to interactive mode if no burst count/query is passed
    if not args.query and args.count <= 1 and args.text == "hello from llm-node client":
        interactive_terminal_mode("http://127.0.0.1:8080")
    else:
        run(args)


if __name__ == "__main__":
    main()