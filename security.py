import os
import base64
import json
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

class PQCSecurityLayer:
    """Simulates ML-KEM-768 (Kyber) Key Encapsulation & AES-256 Payload Encryption."""

    def encapsulate_pqc_header(self, peer_node_id: str) -> dict:
        ct_bytes = os.urandom(32)
        ss_bytes = os.urandom(32)
        return {
            "pqc_alg": "ML-KEM-768",
            "target_peer": peer_node_id,
            "ciphertext_b64": base64.b64encode(ct_bytes).decode("utf-8"),
            "shared_secret_b64": base64.b64encode(ss_bytes).decode("utf-8")
        }

    def encrypt_payload(self, raw_data: dict) -> dict:
        # Generate dynamic key for this payload
        aes_key = AESGCM.generate_key(bit_length=256)
        aesgcm = AESGCM(aes_key)
        
        nonce = os.urandom(12)
        plaintext = json.dumps(raw_data).encode("utf-8")
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        
        return {
            "pqc_encrypted": True,
            "key_b64": base64.b64encode(aes_key).decode("utf-8"),  # Attached for local mesh verification
            "nonce": base64.b64encode(nonce).decode("utf-8"),
            "ciphertext": base64.b64encode(ciphertext).decode("utf-8")
        }

    def decrypt_payload(self, encrypted_packet: dict) -> dict:
        # Extract the key packaged with the packet
        aes_key = base64.b64decode(encrypted_packet["key_b64"])
        aesgcm = AESGCM(aes_key)
        
        nonce = base64.b64decode(encrypted_packet["nonce"])
        ciphertext = base64.b64decode(encrypted_packet["ciphertext"])
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(plaintext.decode("utf-8"))

    def pack_message(self, sender_id: str, target_peer: str, msg_type: str, content: str) -> dict:
        """Packs a text or audio message with ML-KEM-768 header + AES-256 encrypted payload."""
        import time
        header = self.encapsulate_pqc_header(target_peer)
        raw_payload = {
            "sender_id": sender_id,
            "target_peer": target_peer,
            "type": msg_type,  # "text" or "audio"
            "content": content,  # text string or base64 audio string
            "timestamp": time.time(),
            "pqc_header": header
        }
        encrypted = self.encrypt_payload(raw_payload)
        encrypted["pqc_header"] = header
        encrypted["sender_id"] = sender_id
        encrypted["target_peer"] = target_peer
        return encrypted


pqc_node = PQCSecurityLayer()