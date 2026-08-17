"""The ``SPDR1`` release-asset container.

An asset is self-describing, so the data survives total loss of local state:

    "SPDR1" (5B) | header_len (2B, BE) | header JSON (UTF-8) | ciphertext

    header = {nonce_b64, file_id, version_id, part, parts, part_plaintext_sha256}

The header deliberately omits ``rel_path``. ``file_id`` is already
``sha256(rel_path)[:16]``, so nothing leaks here that does not leak today; the
``file_id -> rel_path`` mapping lives in the consolidated manifest, which is
encrypted.

``part_plaintext_sha256`` is only known once the part has been read, but the
header has to precede the ciphertext. Rather than read the source twice or copy
the ciphertext, the header region is written at a fixed reserved size and filled
in with a seek-back at the end. ``header_len`` covers the whole reserved region
and the padding is trailing whitespace, which JSON ignores.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Iterator

from .crypto import StreamingAESGCMDecryptor, StreamingAESGCMEncryptor


MAGIC = b"SPDR1"
HEADER_LEN_BYTES = 2
HEADER_RESERVED = 512
PREFIX_SIZE = len(MAGIC) + HEADER_LEN_BYTES
DEFAULT_READ_SIZE = 4 * 1024 * 1024


class AssetFormatError(Exception):
    pass


@dataclass(frozen=True)
class PartInfo:
    name: str
    part: int
    parts: int
    nonce_b64: str
    part_plaintext_sha256: str
    plaintext_bytes: int
    total_bytes: int


def asset_name(file_id: str, version_id: str, part: int) -> str:
    """Deterministic, so an interrupted upload can be resumed idempotently.

    Being able to ask "does this asset already exist?" before re-uploading is
    what makes resume cheap; randomised names would make it impossible.
    """
    return f"{file_id}-{version_id}-{part:04d}.bin"


def write_part(
    *,
    destination: Path,
    key: bytes,
    file_id: str,
    version_id: str,
    part: int,
    parts: int,
    source: BinaryIO | None = None,
    length: int | None = None,
    payload: bytes | None = None,
    read_size: int = DEFAULT_READ_SIZE,
) -> PartInfo:
    """Write one encrypted part to ``destination``.

    Either ``source`` (read ``length`` bytes from the current position) or
    ``payload`` (in-memory bytes, used for the small consolidated manifest).
    """
    if (source is None) == (payload is None):
        raise ValueError("Indica exactamente uno de source o payload.")

    encryptor = StreamingAESGCMEncryptor(key)
    plaintext_bytes = 0

    with destination.open("wb+") as out:
        out.write(MAGIC)
        out.write(b"\x00" * HEADER_LEN_BYTES)
        out.write(b" " * HEADER_RESERVED)

        if payload is not None:
            plaintext_bytes = len(payload)
            out.write(encryptor.update(payload))
        else:
            remaining = length if length is not None else -1
            while remaining != 0:
                to_read = read_size if remaining < 0 else min(read_size, remaining)
                block = source.read(to_read)
                if not block:
                    break
                plaintext_bytes += len(block)
                if remaining > 0:
                    remaining -= len(block)
                out.write(encryptor.update(block))

        tail, part_plaintext_sha256 = encryptor.finalize()
        out.write(tail)
        total_bytes = out.tell()

        header = json.dumps(
            {
                "nonce_b64": encryptor.nonce_b64,
                "file_id": file_id,
                "version_id": version_id,
                "part": part,
                "parts": parts,
                "part_plaintext_sha256": part_plaintext_sha256,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(header) > HEADER_RESERVED:
            raise AssetFormatError(
                f"La cabecera ocupa {len(header)}B y el maximo reservado es {HEADER_RESERVED}B."
            )

        out.seek(len(MAGIC))
        out.write(struct.pack(">H", HEADER_RESERVED))
        out.write(header.ljust(HEADER_RESERVED, b" "))

    return PartInfo(
        name=asset_name(file_id, version_id, part),
        part=part,
        parts=parts,
        nonce_b64=encryptor.nonce_b64,
        part_plaintext_sha256=part_plaintext_sha256,
        plaintext_bytes=plaintext_bytes,
        total_bytes=total_bytes,
    )


def parse_header(prefix: bytes) -> tuple[dict[str, Any], int]:
    """Parse the container header. Returns ``(header, bytes_consumed)``."""
    if len(prefix) < PREFIX_SIZE:
        raise AssetFormatError("Asset truncado: falta la cabecera.")
    if prefix[: len(MAGIC)] != MAGIC:
        raise AssetFormatError(f"Asset con magic desconocido: {prefix[: len(MAGIC)]!r}")
    (header_len,) = struct.unpack(">H", prefix[len(MAGIC) : PREFIX_SIZE])
    end = PREFIX_SIZE + header_len
    if len(prefix) < end:
        raise AssetFormatError("Asset truncado: cabecera incompleta.")
    return json.loads(prefix[PREFIX_SIZE:end].decode("utf-8")), end


def decrypt_part(data: bytes, key: bytes) -> tuple[bytes, dict[str, Any]]:
    """Decrypt a whole part held in memory. Used by tests and small payloads."""
    header, offset = parse_header(data)
    decryptor = StreamingAESGCMDecryptor(key, header["nonce_b64"])
    plaintext = decryptor.update(data[offset:])
    tail, digest = decryptor.finalize()
    if digest != header["part_plaintext_sha256"]:
        raise AssetFormatError("El hash del plaintext no coincide con la cabecera.")
    return plaintext + tail, header


def iter_decrypted_part(
    chunks: Iterable[bytes], key: bytes
) -> tuple[dict[str, Any], Iterator[bytes], "_PartDigest"]:
    """Stream-decrypt a part without holding it in memory.

    Returns the header, an iterator over plaintext blocks, and a handle whose
    ``digest`` is filled in once the iterator is exhausted.
    """
    iterator = iter(chunks)
    buffer = bytearray()
    header: dict[str, Any] | None = None
    offset = 0
    for block in iterator:
        buffer.extend(block)
        if len(buffer) >= PREFIX_SIZE:
            (header_len,) = struct.unpack(">H", bytes(buffer[len(MAGIC) : PREFIX_SIZE]))
            if len(buffer) >= PREFIX_SIZE + header_len:
                header, offset = parse_header(bytes(buffer))
                break
    if header is None:
        raise AssetFormatError("Asset truncado: no se pudo leer la cabecera.")

    decryptor = StreamingAESGCMDecryptor(key, header["nonce_b64"])
    remainder = bytes(buffer[offset:])
    digest_handle = _PartDigest()

    def _generate() -> Iterator[bytes]:
        if remainder:
            yield decryptor.update(remainder)
        for block in iterator:
            yield decryptor.update(block)
        tail, digest = decryptor.finalize()
        digest_handle.digest = digest
        yield tail

    return header, _generate(), digest_handle


class _PartDigest:
    def __init__(self):
        self.digest: str | None = None
