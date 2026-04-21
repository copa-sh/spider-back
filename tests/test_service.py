from __future__ import annotations

import hashlib
from pathlib import Path

from github_fs.config import AppConfig, RuntimeSecrets
from github_fs.service import AppService
from github_fs.state import StateManager
from github_fs.web import create_web_app


class FakeGitHubClient:
    def __init__(self):
        self.blobs: dict[str, bytes] = {}
        self.files: dict[str, bytes] = {}
        self.last_commit_message: str | None = None

    def create_blob(self, payload: bytes) -> str:
        sha = hashlib.sha1(payload).hexdigest()
        self.blobs[sha] = payload
        return sha

    def commit_tree(self, entries: list[dict[str, str]], message: str) -> str | None:
        self.last_commit_message = message
        for entry in entries:
            self.files[entry["path"]] = self.blobs[entry["sha"]]
        return "commit-sha-1" if entries else None

    def raw_url(self, path: str) -> str:
        return f"memory://{path}"

    def fetch_bytes(self, url: str) -> bytes:
        return self.files[url.removeprefix("memory://")]


def make_service(tmp_path: Path) -> tuple[AppService, Path, Path]:
    data_dir = tmp_path / "datos"
    state_dir = tmp_path / "state"
    data_dir.mkdir()
    state_dir.mkdir()
    config = AppConfig(
        github_token="token",
        github_repository="owner/repo",
        github_branch="main",
        github_uploads_prefix="storage",
        github_chunk_size_mb=1,
        github_timeout_seconds=30,
        github_max_retry=1,
        github_backoff_seconds=1,
        app_data_dir=data_dir,
        app_state_dir=state_dir,
        app_web_host="127.0.0.1",
        app_web_port=8080,
        app_sync_interval_seconds=60,
        app_verify_interval_seconds=120,
        app_web_pin="12345678",
        app_encryption_key=None,
    )
    secrets = RuntimeSecrets(
        encryption_key="AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
        web_pin="12345678",
        flask_secret_key="secret",
    )
    manager = StateManager(state_dir)
    service = AppService(config, secrets, manager)
    service.github = FakeGitHubClient()
    manager.load(service.default_config)
    return service, data_dir, state_dir


def test_sync_modify_delete_and_verify_cycle(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_text("hola", encoding="utf-8")

    first_sync = service.run_sync()
    assert first_sync.ok is True
    state = service.get_state()
    assert first_sync.summary["uploaded_files"] == 1
    assert len(state["files"]) == 1

    file_id, entry = next(iter(state["files"].items()))
    first_version_id = entry["active_version_id"]
    assert entry["present"] is True

    verify = service.run_verify()
    assert verify.ok is True
    state = service.get_state()
    assert state["files"][file_id]["last_verification"]["ok"] is True

    sample.write_text("hola de nuevo", encoding="utf-8")
    second_sync = service.run_sync()
    assert second_sync.ok is True
    state = service.get_state()
    assert state["files"][file_id]["active_version_id"] != first_version_id
    assert len(state["files"][file_id]["versions"]) == 2

    sample.unlink()
    third_sync = service.run_sync()
    assert third_sync.ok is True
    state = service.get_state()
    assert state["files"][file_id]["present"] is False
    assert third_sync.summary["missing_files"] == 1


def test_web_login_and_manual_actions(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "archivo.txt").write_text("hola", encoding="utf-8")
    service.run_sync()

    app = create_web_app(service)
    client = app.test_client()

    invalid = client.post("/login", data={"pin": "0000"})
    assert invalid.status_code == 401

    login = client.post("/login", data={"pin": "12345678"})
    assert login.status_code == 302

    home = client.get("/")
    assert home.status_code == 200
    assert b"github-fs" in home.data

    trigger = client.post("/actions/verify")
    assert trigger.status_code == 302
