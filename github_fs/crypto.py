from __future__ import annotations

import base64
import hashlib
import secrets
from typing import Iterable

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .utils import sha256_bytes


def encrypt_bytes(plaintext: bytes, key: bytes) -> dict:
    nonce = secrets.token_bytes(12)
    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(nonce, plaintext, None)
    return {
        "nonce_b64": base64.urlsafe_b64encode(nonce).decode().rstrip("="),
        "ciphertext": ciphertext,
        "plaintext_sha256": sha256_bytes(plaintext),
        "ciphertext_sha256": sha256_bytes(ciphertext),
        "algorithm": "AES-256-GCM",
    }


def decrypt_bytes(ciphertext: bytes, key: bytes, nonce_b64: str) -> bytes:
    padding = "=" * (-len(nonce_b64) % 4)
    nonce = base64.urlsafe_b64decode(nonce_b64 + padding)
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ciphertext, None)


class StreamingAESGCMDecryptor:
    def __init__(self, key: bytes, nonce_b64: str):
        padding = "=" * (-len(nonce_b64) % 4)
        self._nonce = base64.urlsafe_b64decode(nonce_b64 + padding)
        self._decryptor = Cipher(algorithms.AES(key), modes.GCM(self._nonce)).decryptor()
        self._buffer = b""
        self._plaintext_hasher = hashlib.sha256()
        self._finished = False

    def update(self, data: bytes) -> bytes:
        if self._finished:
            raise ValueError("El descifrador ya se ha cerrado.")

        self._buffer += data
        if len(self._buffer) <= 16:
            return b""

        emit_upto = len(self._buffer) - 16
        chunk = self._buffer[:emit_upto]
        self._buffer = self._buffer[emit_upto:]
        plaintext = self._decryptor.update(chunk)
        self._plaintext_hasher.update(plaintext)
        return plaintext

    def finalize(self) -> tuple[bytes, str]:
        if self._finished:
            raise ValueError("El descifrador ya se ha cerrado.")
        if len(self._buffer) < 16:
            raise ValueError("Ciphertext incompleto: falta el tag GCM.")

        ciphertext_tail = self._buffer[:-16]
        tag = self._buffer[-16:]
        self._buffer = b""

        plaintext = self._decryptor.update(ciphertext_tail) + self._decryptor.finalize_with_tag(tag)
        self._plaintext_hasher.update(plaintext)
        self._finished = True
        return plaintext, self._plaintext_hasher.hexdigest()


def chunk_bytes(data: bytes, chunk_size: int) -> Iterable[tuple[int, bytes]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor que 0")
    for index in range(0, len(data), chunk_size):
        yield index // chunk_size, data[index:index + chunk_size]
    if not data:
        yield 0, b""
