from __future__ import annotations

import hashlib
import io
import json
import struct

import pytest

from app.asset_format import (
    HEADER_RESERVED,
    MAGIC,
    PREFIX_SIZE,
    AssetFormatError,
    asset_name,
    decrypt_part,
    iter_decrypted_part,
    parse_header,
    write_part,
)
from app.crypto import StreamingAESGCMDecryptor, StreamingAESGCMEncryptor


KEY = bytes(range(32))


def test_streaming_encryptor_round_trips_through_the_decryptor():
    plaintext = b"contenido de prueba" * 500
    encryptor = StreamingAESGCMEncryptor(KEY)
    ciphertext = b""
    for index in range(0, len(plaintext), 1024):
        ciphertext += encryptor.update(plaintext[index : index + 1024])
    tail, plaintext_sha = encryptor.finalize()
    ciphertext += tail

    assert plaintext_sha == hashlib.sha256(plaintext).hexdigest()

    decryptor = StreamingAESGCMDecryptor(KEY, encryptor.nonce_b64)
    recovered = b""
    for index in range(0, len(ciphertext), 777):
        recovered += decryptor.update(ciphertext[index : index + 777])
    rest, digest = decryptor.finalize()
    assert recovered + rest == plaintext
    assert digest == plaintext_sha


def test_each_part_gets_its_own_nonce():
    nonces = {StreamingAESGCMEncryptor(KEY).nonce for _ in range(50)}
    assert len(nonces) == 50


def test_asset_round_trip_header_decrypt_and_hash(tmp_path):
    """SPDR1 header -> decrypt -> hash must equal the local hash."""
    plaintext = b"\x00\xff" + b"datos binarios" * 1000
    source = tmp_path / "source.bin"
    source.write_bytes(plaintext)
    destination = tmp_path / "part.spdr"

    with source.open("rb") as handle:
        info = write_part(
            destination=destination,
            key=KEY,
            file_id="0123456789abcdef",
            version_id="20260101T000000000000Z",
            part=0,
            parts=1,
            source=handle,
            length=len(plaintext),
        )

    assert info.name == "0123456789abcdef-20260101T000000000000Z-0000.bin"
    assert info.plaintext_bytes == len(plaintext)
    assert info.part_plaintext_sha256 == hashlib.sha256(plaintext).hexdigest()

    raw = destination.read_bytes()
    assert raw.startswith(MAGIC)
    assert info.total_bytes == len(raw)

    header, offset = parse_header(raw)
    assert offset == PREFIX_SIZE + HEADER_RESERVED
    assert header == {
        "nonce_b64": info.nonce_b64,
        "file_id": "0123456789abcdef",
        "version_id": "20260101T000000000000Z",
        "part": 0,
        "parts": 1,
        "part_plaintext_sha256": info.part_plaintext_sha256,
    }

    recovered, header_again = decrypt_part(raw, KEY)
    assert recovered == plaintext
    assert header_again["part_plaintext_sha256"] == hashlib.sha256(recovered).hexdigest()


def test_header_omits_the_relative_path(tmp_path):
    """file_id is already sha256(rel_path)[:16]; the path itself must not leak
    into an asset — the mapping lives in the encrypted consolidated manifest."""
    destination = tmp_path / "part.spdr"
    write_part(
        destination=destination,
        key=KEY,
        file_id="0123456789abcdef",
        version_id="v1",
        part=0,
        parts=1,
        payload=b"x",
    )
    header, _ = parse_header(destination.read_bytes())
    assert "rel_path" not in header
    assert "path" not in header


def test_parts_are_independently_decryptable(tmp_path):
    plaintext = bytes(range(256)) * 40
    source = tmp_path / "source.bin"
    source.write_bytes(plaintext)
    part_size = 4096
    recovered = b""

    with source.open("rb") as handle:
        for part in range(3):
            destination = tmp_path / f"part{part}.spdr"
            handle.seek(part * part_size)
            length = min(part_size, len(plaintext) - part * part_size)
            info = write_part(
                destination=destination,
                key=KEY,
                file_id="f" * 16,
                version_id="v1",
                part=part,
                parts=3,
                source=handle,
                length=length,
            )
            # Decrypting one part needs nothing from the others.
            chunk, header = decrypt_part(destination.read_bytes(), KEY)
            assert header["part"] == part
            assert info.part_plaintext_sha256 == hashlib.sha256(chunk).hexdigest()
            recovered += chunk

    assert recovered == plaintext


def test_iter_decrypted_part_streams_and_reports_the_digest(tmp_path):
    plaintext = b"streaming" * 5000
    destination = tmp_path / "part.spdr"
    info = write_part(
        destination=destination,
        key=KEY,
        file_id="a" * 16,
        version_id="v1",
        part=2,
        parts=5,
        payload=plaintext,
    )

    raw = destination.read_bytes()
    chunks = [raw[index : index + 97] for index in range(0, len(raw), 97)]
    header, blocks, digest = iter_decrypted_part(chunks, KEY)
    assert header["part"] == 2
    assert digest.digest is None  # not known until the stream is drained

    recovered = b"".join(blocks)
    assert recovered == plaintext
    assert digest.digest == info.part_plaintext_sha256


def test_rejects_an_unknown_magic():
    with pytest.raises(AssetFormatError, match="magic"):
        parse_header(b"NOPE!" + struct.pack(">H", 2) + b"{}")


def test_rejects_a_truncated_asset():
    with pytest.raises(AssetFormatError, match="truncado"):
        parse_header(MAGIC + b"\x00")


def test_write_part_requires_exactly_one_input(tmp_path):
    with pytest.raises(ValueError):
        write_part(
            destination=tmp_path / "x.spdr", key=KEY, file_id="f", version_id="v", part=0, parts=1
        )
    with pytest.raises(ValueError):
        write_part(
            destination=tmp_path / "x.spdr",
            key=KEY,
            file_id="f",
            version_id="v",
            part=0,
            parts=1,
            source=io.BytesIO(b"a"),
            payload=b"a",
        )


def test_asset_names_are_deterministic():
    assert asset_name("abc", "v1", 7) == "abc-v1-0007.bin"
    assert asset_name("abc", "v1", 7) == asset_name("abc", "v1", 7)


def test_header_is_padded_but_still_valid_json(tmp_path):
    destination = tmp_path / "part.spdr"
    write_part(
        destination=destination, key=KEY, file_id="f" * 16, version_id="v1", part=0, parts=1,
        payload=b"x",
    )
    raw = destination.read_bytes()
    (declared,) = struct.unpack(">H", raw[len(MAGIC) : PREFIX_SIZE])
    assert declared == HEADER_RESERVED
    # The reserved region is JSON followed by padding, and json tolerates it.
    assert json.loads(raw[PREFIX_SIZE : PREFIX_SIZE + declared].decode("utf-8"))
