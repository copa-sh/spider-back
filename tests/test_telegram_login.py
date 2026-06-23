from __future__ import annotations

import threading

import pytest

from app.telegram_login import (
    AUTHORIZED,
    CODE_SENT,
    IDLE,
    PASSWORD_NEEDED,
    LoginError,
    TelegramLoginManager,
)


# ── Pyrogram-error stand-ins (matched by class name, like the real ones) ──────
class SessionPasswordNeeded(Exception):
    pass


class PhoneCodeInvalid(Exception):
    pass


class PhoneCodeExpired(Exception):
    pass


class PasswordHashInvalid(Exception):
    pass


class SentCode:
    def __init__(self, phone_code_hash: str):
        self.phone_code_hash = phone_code_hash


class FakeClient:
    """Mimics the slice of the Pyrogram sync client the login flow drives."""

    def __init__(self, *, requires_2fa: bool = False, valid_code: str = "12345",
                 valid_password: str = "hunter2", send_code_error: Exception | None = None):
        self.requires_2fa = requires_2fa
        self.valid_code = valid_code
        self.valid_password = valid_password
        self.send_code_error = send_code_error
        self.is_connected = False
        self.signed_in = False
        self.disconnected = False

    def connect(self):
        self.is_connected = True
        return False  # not yet authorized

    def send_code(self, phone):
        if self.send_code_error:
            raise self.send_code_error
        return SentCode("HASH-123")

    def sign_in(self, phone, phone_code_hash, code):
        assert phone_code_hash == "HASH-123"
        if code != self.valid_code:
            raise PhoneCodeInvalid("bad code")
        if self.requires_2fa:
            raise SessionPasswordNeeded("needs password")
        self.signed_in = True
        return {"id": 1}

    def check_password(self, password):
        if password != self.valid_password:
            raise PasswordHashInvalid("bad password")
        self.signed_in = True
        return {"id": 1}

    def disconnect(self):
        self.is_connected = False
        self.disconnected = True


def make_manager(tmp_path, client, *, phone="+34600000000", on_authorized=None):
    session_file = tmp_path / "tg_account_1.session"

    return (
        TelegramLoginManager(
            client_factory=lambda account_id: client,
            phone_for=lambda account_id: phone if account_id == "tg_account_1" else None,
            session_path=lambda account_id: str(tmp_path / f"{account_id}.session"),
            on_authorized=on_authorized,
        ),
        session_file,
    )


def test_happy_path_code_only(tmp_path):
    client = FakeClient()
    notified = []
    mgr, _ = make_manager(tmp_path, client, on_authorized=notified.append)

    assert mgr.state("tg_account_1") == IDLE
    assert mgr.start("tg_account_1") == CODE_SENT
    assert mgr.state("tg_account_1") == CODE_SENT

    assert mgr.submit_code("tg_account_1", "12345") == AUTHORIZED
    assert mgr.state("tg_account_1") == IDLE  # flow cleared
    assert client.signed_in is True
    assert client.disconnected is True  # disconnect flushes the session
    assert notified == ["tg_account_1"]  # server cache reset was triggered


def test_2fa_path(tmp_path):
    client = FakeClient(requires_2fa=True)
    mgr, _ = make_manager(tmp_path, client)

    assert mgr.start("tg_account_1") == CODE_SENT
    assert mgr.submit_code("tg_account_1", "12345") == PASSWORD_NEEDED
    assert mgr.state("tg_account_1") == PASSWORD_NEEDED

    with pytest.raises(LoginError):
        mgr.submit_password("tg_account_1", "wrong")
    # still mid-flow, can retry the password
    assert mgr.state("tg_account_1") == PASSWORD_NEEDED
    assert mgr.submit_password("tg_account_1", "hunter2") == AUTHORIZED
    assert client.signed_in is True


def test_bad_code_keeps_flow_open_for_retry(tmp_path):
    client = FakeClient()
    mgr, _ = make_manager(tmp_path, client)
    mgr.start("tg_account_1")

    with pytest.raises(LoginError):
        mgr.submit_code("tg_account_1", "00000")
    # bad code is recoverable — flow stays in CODE_SENT
    assert mgr.state("tg_account_1") == CODE_SENT
    assert mgr.submit_code("tg_account_1", "12345") == AUTHORIZED


def test_unknown_account_raises(tmp_path):
    client = FakeClient()
    mgr, _ = make_manager(tmp_path, client)
    with pytest.raises(LoginError):
        mgr.start("tg_account_99")


def test_existing_session_backed_up_and_restored_on_cancel(tmp_path):
    client = FakeClient()
    mgr, session_file = make_manager(tmp_path, client)
    session_file.write_text("OLD-SESSION", encoding="utf-8")

    mgr.start("tg_account_1")
    # during login the original is set aside as a .bak
    assert (tmp_path / "tg_account_1.session.bak").exists()

    mgr.cancel("tg_account_1")
    # cancel restores the operator's previous session intact
    assert session_file.exists()
    assert session_file.read_text(encoding="utf-8") == "OLD-SESSION"
    assert not (tmp_path / "tg_account_1.session.bak").exists()


def test_authorized_discards_backup(tmp_path):
    client = FakeClient()
    mgr, session_file = make_manager(tmp_path, client)
    session_file.write_text("OLD-SESSION", encoding="utf-8")

    mgr.start("tg_account_1")
    mgr.submit_code("tg_account_1", "12345")
    # on success the backup is dropped (new session supersedes it)
    assert not (tmp_path / "tg_account_1.session.bak").exists()


def test_start_failure_restores_session(tmp_path):
    client = FakeClient(send_code_error=RuntimeError("network down"))
    mgr, session_file = make_manager(tmp_path, client)
    session_file.write_text("OLD-SESSION", encoding="utf-8")

    with pytest.raises(LoginError):
        mgr.start("tg_account_1")
    # a failed start must not lose the previous session
    assert session_file.read_text(encoding="utf-8") == "OLD-SESSION"
    assert mgr.state("tg_account_1") == IDLE


def test_submit_without_start_raises(tmp_path):
    client = FakeClient()
    mgr, _ = make_manager(tmp_path, client)
    with pytest.raises(LoginError):
        mgr.submit_code("tg_account_1", "12345")


def test_has_session_reflects_disk(tmp_path):
    client = FakeClient()
    mgr, session_file = make_manager(tmp_path, client)
    assert mgr.has_session("tg_account_1") is False
    session_file.write_text("x", encoding="utf-8")
    assert mgr.has_session("tg_account_1") is True
