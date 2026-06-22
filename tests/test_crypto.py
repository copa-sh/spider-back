from app.crypto import StreamingAESGCMDecryptor, decrypt_bytes, encrypt_bytes


def test_encrypt_decrypt_roundtrip():
    key = bytes(range(32))
    payload = b"hola mundo" * 10
    encrypted = encrypt_bytes(payload, key)
    decrypted = decrypt_bytes(encrypted["ciphertext"], key, encrypted["nonce_b64"])
    assert decrypted == payload
    assert encrypted["plaintext_sha256"]
    assert encrypted["ciphertext_sha256"]


def test_streaming_decrypt_roundtrip():
    key = bytes(range(32))
    payload = b"abc123" * 1000
    encrypted = encrypt_bytes(payload, key)
    decryptor = StreamingAESGCMDecryptor(key, encrypted["nonce_b64"])
    recovered = bytearray()

    for index in range(0, len(encrypted["ciphertext"]), 37):
        recovered.extend(decryptor.update(encrypted["ciphertext"][index:index + 37]))

    tail, digest = decryptor.finalize()
    recovered.extend(tail)

    assert bytes(recovered) == payload
    assert digest == encrypted["plaintext_sha256"]
