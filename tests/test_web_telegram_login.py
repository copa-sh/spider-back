from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from app.telegram_login import AUTHORIZED, CODE_SENT, IDLE, PASSWORD_NEEDED, LoginError
from app.web import create_web_app


@dataclass
class FakeAccount:
    account_id: str
    phone: str = "+34600000000"
    api_id: int = 123


class FakeLoginManager:
    """Records calls and returns scripted states, so we test the web wiring only."""

    def __init__(self):
        self._state = IDLE
        self.calls = []

    def state(self, account_id):
        return self._state

    def has_session(self, account_id):
        return False

    def start(self, account_id):
        self.calls.append(("start", account_id))
        self._state = CODE_SENT
        return CODE_SENT

    def submit_code(self, account_id, code):
        self.calls.append(("code", account_id, code))
        if code != "12345":
            raise LoginError("Código incorrecto. Vuelve a intentarlo.")
        self._state = AUTHORIZED
        return AUTHORIZED

    def submit_password(self, account_id, password):
        self.calls.append(("password", account_id, password))
        self._state = AUTHORIZED
        return AUTHORIZED

    def cancel(self, account_id):
        self.calls.append(("cancel", account_id))
        self._state = IDLE


def make_client(manager):
    service = SimpleNamespace(
        secrets=SimpleNamespace(flask_secret_key="test-secret", web_pin="1234"),
        telegram_account_by_id={"tg_account_1": FakeAccount("tg_account_1")},
    )
    app = create_web_app(service, login_manager=manager)
    app.config.update(TESTING=True)
    client = app.test_client()
    with client.session_transaction() as sess:
        sess["authenticated"] = True
    return client


def test_login_page_renders():
    client = make_client(FakeLoginManager())
    resp = client.get("/telegram/tg_account_1/login")
    assert resp.status_code == 200
    assert b"Login Telegram" in resp.data
    assert b"+34600000000" in resp.data


def test_unknown_account_404():
    client = make_client(FakeLoginManager())
    resp = client.get("/telegram/nope/login")
    assert resp.status_code == 404


def test_start_then_code_happy_path():
    mgr = FakeLoginManager()
    client = make_client(mgr)

    resp = client.post("/telegram/tg_account_1/login/start")
    assert resp.status_code == 302  # redirects back to login page

    resp = client.post("/telegram/tg_account_1/login/code", data={"code": "12345"})
    assert resp.status_code == 200
    assert "autenticada".encode() in resp.data
    assert ("start", "tg_account_1") in mgr.calls
    assert ("code", "tg_account_1", "12345") in mgr.calls


def test_bad_code_shows_error():
    mgr = FakeLoginManager()
    mgr._state = CODE_SENT
    client = make_client(mgr)
    resp = client.post("/telegram/tg_account_1/login/code", data={"code": "00000"})
    assert resp.status_code == 400
    assert "incorrecto".encode() in resp.data


def test_password_route():
    mgr = FakeLoginManager()
    mgr._state = PASSWORD_NEEDED
    client = make_client(mgr)
    resp = client.post("/telegram/tg_account_1/login/password", data={"password": "x"})
    assert resp.status_code == 200
    assert ("password", "tg_account_1", "x") in mgr.calls


def test_cancel_route():
    mgr = FakeLoginManager()
    client = make_client(mgr)
    resp = client.post("/telegram/tg_account_1/login/cancel")
    assert resp.status_code == 302
    assert ("cancel", "tg_account_1") in mgr.calls


def test_login_requires_auth():
    mgr = FakeLoginManager()
    service = SimpleNamespace(
        secrets=SimpleNamespace(flask_secret_key="test-secret", web_pin="1234"),
        telegram_account_by_id={"tg_account_1": FakeAccount("tg_account_1")},
    )
    app = create_web_app(service, login_manager=mgr)
    app.config.update(TESTING=True)
    client = app.test_client()  # no authenticated session
    resp = client.get("/telegram/tg_account_1/login")
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]
