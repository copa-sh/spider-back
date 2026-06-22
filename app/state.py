from __future__ import annotations

import json
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any

from .config import RuntimeSecrets, generate_encryption_key, generate_flask_secret, generate_pin
from .state_migrations import migrate_state
from .utils import utc_now_iso


class StateManager:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.state_path = state_dir / "index.json"
        self.secrets_path = state_dir / "secrets.json"
        self._lock = threading.RLock()

    def bootstrap_secrets(
        self,
        configured_pin: str | None,
        configured_encryption_key: str | None,
    ) -> tuple[RuntimeSecrets, dict[str, bool]]:
        with self._lock:
            existing: dict[str, Any] = {}
            if self.secrets_path.exists():
                existing = json.loads(self.secrets_path.read_text(encoding="utf-8"))

            generated = {"web_pin": False, "encryption_key": False, "flask_secret_key": False}

            web_pin = configured_pin or existing.get("web_pin")
            if not web_pin:
                web_pin = generate_pin()
                generated["web_pin"] = True

            encryption_key = configured_encryption_key or existing.get("encryption_key")
            if not encryption_key:
                encryption_key = generate_encryption_key()
                generated["encryption_key"] = True

            flask_secret_key = existing.get("flask_secret_key")
            if not flask_secret_key:
                flask_secret_key = generate_flask_secret()
                generated["flask_secret_key"] = True

            payload = {
                "web_pin": web_pin,
                "encryption_key": encryption_key,
                "flask_secret_key": flask_secret_key,
                "updated_at": utc_now_iso(),
            }
            self.secrets_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return RuntimeSecrets(
                encryption_key=encryption_key,
                web_pin=web_pin,
                flask_secret_key=flask_secret_key,
            ), generated

    def load(self, default_config: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if not self.state_path.exists():
                state = migrate_state(self._default_state(default_config))
                state["config"] = default_config
                self._write(state)
                return state

            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            state = migrate_state(raw)
            # Auto-sanitize: if migration had to add/normalize any field, persist
            # the upgraded structure back to disk so the on-disk state is always
            # current (and we don't re-run the migration every load). The volatile
            # `config` block is compared before being overwritten so a changed
            # effective config alone does not trigger a rewrite here.
            structure_changed = state != raw
            state["config"] = default_config
            if structure_changed:
                self._write(state)
            return state

    def save(self, state: dict[str, Any]) -> None:
        with self._lock:
            self._write(state)

    def snapshot(self, default_config: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self.load(default_config))

    def _write(self, state: dict[str, Any]) -> None:
        tmp_path = self.state_dir / f"{self.state_path.name}.tmp"
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.state_path)

    @staticmethod
    def _default_state(default_config: dict[str, Any]) -> dict[str, Any]:
        return {
            "created_at": utc_now_iso(),
            "config": default_config,
            "tasks": {
                "sync": {
                    "last_started_at": None,
                    "last_finished_at": None,
                    "last_result": "never",
                    "last_error": None,
                    "last_summary": {},
                    "last_manual_trigger_at": None,
                    "running": False,
                },
                "verify": {
                    "last_started_at": None,
                    "last_finished_at": None,
                    "last_result": "never",
                    "last_error": None,
                    "last_summary": {},
                    "last_manual_trigger_at": None,
                    "running": False,
                },
            },
            "files": {},
            "github_accounts": {},
        }
