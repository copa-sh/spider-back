"""Interactive Telegram login, driven from the web UI.

The running server only ever ``connect()``s with a pre-existing ``.session``
file (see ``TelegramClient._ensure_connection``); it never performs the
``send_code`` → ``sign_in`` handshake. When a session is missing or revoked
(``AUTH_KEY_UNREGISTERED``) there was no in-app way to re-authenticate — the
operator had to generate the session out of band and copy it into ``/state``.

This module performs that handshake over a sequence of HTTP requests. Because
Pyrogram's ``phone_code_hash`` is bound to a single connected client, the live
client is held in memory between requests (keyed by ``account_id``) until the
flow finishes or is cancelled.

The Pyrogram client is created via an injectable factory so the module imports
without pyrogram installed and the handshake is unit-testable with a fake.
"""

from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

# Login lifecycle states (also used as the dashboard/login-page discriminator).
IDLE = "idle"
CODE_SENT = "code_sent"
PASSWORD_NEEDED = "password_needed"
AUTHORIZED = "authorized"


class LoginError(Exception):
    """A user-facing login failure (bad code, expired code, wrong password…).

    Distinct from unexpected exceptions: the message is safe to render verbatim.
    """


# Pyrogram raises typed errors from ``pyrogram.errors``. We match on the class
# name rather than importing them, so the flow works even when pyrogram is
# absent (tests) and is resilient to import-path drift across versions.
_PASSWORD_NEEDED_EXC = "SessionPasswordNeeded"
_BAD_CODE_EXC = {"PhoneCodeInvalid", "PhoneCodeEmpty"}
_EXPIRED_CODE_EXC = {"PhoneCodeExpired"}
_BAD_PASSWORD_EXC = {"PasswordHashInvalid", "BadRequest"}


def _ensure_event_loop() -> None:
    """Pyrogram's sync wrappers need an event loop in the calling thread.

    Flask request handlers run in worker threads that have none by default —
    mirror the guard in ``TelegramClient._ensure_connection``.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("closed")
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


@dataclass
class _Pending:
    """A login in progress for one account."""

    client: Any
    phone: str
    state: str = IDLE
    phone_code_hash: Optional[str] = None
    # Path of the session file we set aside before starting, so a failed/cancelled
    # login can restore the operator's previous (possibly still-valid) session.
    backup_path: Optional[str] = None
    session_path: Optional[str] = None


@dataclass
class TelegramLoginManager:
    """Drives the multi-step Telegram auth handshake, one flow per account.

    ``client_factory(account_id)`` returns a fresh raw Pyrogram-style client
    whose session is named so it lands at ``session_path(account_id)``.
    ``phone_for(account_id)`` returns the account's configured phone number.
    ``session_path(account_id)`` returns the absolute ``.session`` path.
    ``on_authorized(account_id)`` (optional) is called after a successful login,
    e.g. to drop the running server's cached client so it reloads the new session.
    """

    client_factory: Callable[[str], Any]
    phone_for: Callable[[str], Optional[str]]
    session_path: Callable[[str], str]
    on_authorized: Optional[Callable[[str], None]] = None
    _pending: dict[str, _Pending] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    # ── Introspection ────────────────────────────────────────────────────────
    def state(self, account_id: str) -> str:
        with self._lock:
            pending = self._pending.get(account_id)
            return pending.state if pending else IDLE

    def has_session(self, account_id: str) -> bool:
        """Whether a ``.session`` file already exists on disk for this account."""
        try:
            return os.path.exists(self.session_path(account_id))
        except Exception:
            return False

    # ── Handshake steps ──────────────────────────────────────────────────────
    def start(self, account_id: str) -> str:
        """Begin (or restart) login: connect and request an SMS/app code.

        Any prior session file is set aside first so the handshake starts from a
        clean slate (a revoked auth key cannot be reused for ``send_code``). The
        previous file is restored if the flow is cancelled or errors out before
        completing.
        """
        phone = self.phone_for(account_id)
        if not phone:
            raise LoginError(f"Cuenta Telegram desconocida: {account_id}")

        _ensure_event_loop()
        with self._lock:
            self._abort_locked(account_id, restore=True)

            session_path = self.session_path(account_id)
            backup_path = self._set_aside_session(session_path)

            client = self.client_factory(account_id)
            pending = _Pending(
                client=client,
                phone=phone,
                session_path=session_path,
                backup_path=backup_path,
            )
            self._pending[account_id] = pending

            try:
                client.connect()
                sent = client.send_code(phone)
                pending.phone_code_hash = getattr(sent, "phone_code_hash", None) or (
                    sent.get("phone_code_hash") if isinstance(sent, dict) else None
                )
                if not pending.phone_code_hash:
                    raise LoginError("Telegram no devolvió phone_code_hash al enviar el código.")
                pending.state = CODE_SENT
                return CODE_SENT
            except LoginError:
                self._abort_locked(account_id, restore=True)
                raise
            except Exception as exc:  # noqa: BLE001 — normalize to user-facing error
                self._abort_locked(account_id, restore=True)
                raise LoginError(f"No se pudo iniciar sesión en Telegram: {exc}") from exc

    def submit_code(self, account_id: str, code: str) -> str:
        """Submit the login code. May advance to AUTHORIZED or PASSWORD_NEEDED."""
        _ensure_event_loop()
        with self._lock:
            pending = self._require(account_id, CODE_SENT)
            try:
                pending.client.sign_in(pending.phone, pending.phone_code_hash, code)
            except Exception as exc:  # noqa: BLE001
                name = type(exc).__name__
                if name == _PASSWORD_NEEDED_EXC:
                    pending.state = PASSWORD_NEEDED
                    return PASSWORD_NEEDED
                if name in _BAD_CODE_EXC:
                    raise LoginError("Código incorrecto. Vuelve a intentarlo.") from exc
                if name in _EXPIRED_CODE_EXC:
                    self._abort_locked(account_id, restore=True)
                    raise LoginError("El código ha caducado. Reinicia el login.") from exc
                self._abort_locked(account_id, restore=True)
                raise LoginError(f"Fallo al validar el código: {exc}") from exc
            return self._finalize_locked(account_id)

    def submit_password(self, account_id: str, password: str) -> str:
        """Submit the 2FA (cloud password) for accounts that require it."""
        _ensure_event_loop()
        with self._lock:
            pending = self._require(account_id, PASSWORD_NEEDED)
            try:
                pending.client.check_password(password)
            except Exception as exc:  # noqa: BLE001
                if type(exc).__name__ in _BAD_PASSWORD_EXC:
                    raise LoginError("Contraseña 2FA incorrecta. Vuelve a intentarlo.") from exc
                self._abort_locked(account_id, restore=True)
                raise LoginError(f"Fallo al validar la contraseña 2FA: {exc}") from exc
            return self._finalize_locked(account_id)

    def cancel(self, account_id: str) -> None:
        """Abort an in-progress login, restoring any prior session file."""
        _ensure_event_loop()
        with self._lock:
            self._abort_locked(account_id, restore=True)

    # ── Internals (call with the lock held) ──────────────────────────────────
    def _require(self, account_id: str, expected_state: str) -> _Pending:
        pending = self._pending.get(account_id)
        if pending is None:
            raise LoginError("No hay un login en curso. Inícialo de nuevo.")
        if pending.state != expected_state:
            raise LoginError(
                f"Paso de login inesperado (estado={pending.state}). Reinicia el proceso."
            )
        return pending

    def _finalize_locked(self, account_id: str) -> str:
        """Persist the session, clear the pending flow, notify the server."""
        pending = self._pending[account_id]
        # Disconnecting flushes Pyrogram's SQLite session to disk.
        self._safe_disconnect(pending.client)
        self._discard_backup(pending)
        pending.state = AUTHORIZED
        del self._pending[account_id]
        if self.on_authorized is not None:
            try:
                self.on_authorized(account_id)
            except Exception:  # noqa: BLE001 — never let a callback break the flow
                pass
        return AUTHORIZED

    def _abort_locked(self, account_id: str, *, restore: bool) -> None:
        pending = self._pending.pop(account_id, None)
        if pending is None:
            return
        self._safe_disconnect(pending.client)
        if restore:
            self._restore_session(pending)
        else:
            self._discard_backup(pending)

    @staticmethod
    def _safe_disconnect(client: Any) -> None:
        try:
            if getattr(client, "is_connected", False):
                disconnect = getattr(client, "disconnect", None)
                if callable(disconnect):
                    disconnect()
        except Exception:  # noqa: BLE001
            pass

    # ── Session-file backup / restore ────────────────────────────────────────
    @staticmethod
    def _set_aside_session(session_path: str) -> Optional[str]:
        """Move an existing session aside so login starts clean. Returns backup path."""
        if not session_path or not os.path.exists(session_path):
            return None
        backup_path = session_path + ".bak"
        try:
            if os.path.exists(backup_path):
                os.remove(backup_path)
            os.replace(session_path, backup_path)
            return backup_path
        except OSError:
            return None

    @staticmethod
    def _restore_session(pending: _Pending) -> None:
        # Remove whatever partial session the aborted flow left, then restore backup.
        if pending.session_path and os.path.exists(pending.session_path):
            try:
                os.remove(pending.session_path)
            except OSError:
                pass
        if pending.backup_path and os.path.exists(pending.backup_path):
            try:
                os.replace(pending.backup_path, pending.session_path)
            except OSError:
                pass

    @staticmethod
    def _discard_backup(pending: _Pending) -> None:
        if pending.backup_path and os.path.exists(pending.backup_path):
            try:
                os.remove(pending.backup_path)
            except OSError:
                pass
