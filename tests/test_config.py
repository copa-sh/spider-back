from __future__ import annotations

from unittest.mock import patch

import pytest

from app.config import ConfigError, generate_encryption_key, generate_pin, load_config


def _base_env(tmp_path) -> dict[str, str]:
    return {
        "GITHUB_ACCOUNT_1_TOKEN": "token-1",
        "GITHUB_ACCOUNT_1_OWNER": "owner-1",
        "GITHUB_REPOSITORY_MAX_SIZE_KB": "1024",
        "GITHUB_ACCOUNT_DAILY_UPLOAD_LIMIT_GB": "1",
        "APP_STATE_DIR": str(tmp_path / "state"),
    }


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


def test_load_config_parses_numbered_accounts(tmp_path):
    with patch.dict("os.environ", _base_env(tmp_path), clear=True):
        config = load_config()
    assert [item.account_id for item in config.github_accounts] == ["account_1"]
    assert config.github_repository_prefix == "model"
    assert config.github_account_daily_upload_limit_bytes == 1024**3


def test_load_config_rejects_invalid_sleep_range(tmp_path):
    env = _base_env(tmp_path)
    env["GITHUB_UPLOAD_SLEEP_MIN_SECONDS"] = "2"
    env["GITHUB_UPLOAD_SLEEP_MAX_SECONDS"] = "1"
    with patch.dict("os.environ", env, clear=True):
        with pytest.raises(ConfigError):
            load_config()


def test_load_config_requires_complete_account(tmp_path):
    env = _base_env(tmp_path)
    env.pop("GITHUB_ACCOUNT_1_OWNER")
    with patch.dict("os.environ", env, clear=True):
        with pytest.raises(ConfigError):
            load_config()


def test_copy_count_reads_network_agnostic_env(tmp_path):
    env = _base_env(tmp_path)
    env["GITHUB_ACCOUNT_2_TOKEN"] = "token-2"
    env["GITHUB_ACCOUNT_2_OWNER"] = "owner-2"
    env["COPY_COUNT"] = "2"
    with patch.dict("os.environ", env, clear=True):
        config = load_config()
    assert config.copy_count == 2


def test_copy_count_falls_back_to_legacy_github_env(tmp_path):
    env = _base_env(tmp_path)
    env["GITHUB_ACCOUNT_2_TOKEN"] = "token-2"
    env["GITHUB_ACCOUNT_2_OWNER"] = "owner-2"
    env["GITHUB_COPY_COUNT"] = "2"  # legacy name still honored
    with patch.dict("os.environ", env, clear=True):
        config = load_config()
    assert config.copy_count == 2


def test_copy_count_cannot_exceed_account_total(tmp_path):
    env = _base_env(tmp_path)
    env["COPY_COUNT"] = "2"  # only one account configured
    with patch.dict("os.environ", env, clear=True):
        with pytest.raises(ConfigError):
            load_config()
