from github_fs.config import generate_encryption_key, generate_pin


def test_generate_pin_is_numeric():
    pin = generate_pin()
    assert len(pin) == 8
    assert pin.isdigit()


def test_generate_encryption_key_decodes_to_32_bytes():
    key = generate_encryption_key()
    padding = "=" * (-len(key) % 4)
    import base64

    decoded = base64.urlsafe_b64decode(key + padding)
    assert len(decoded) == 32

