from __future__ import annotations

import hashlib
from pathlib import Path

from github_fs.config import AppConfig, GitHubAccountConfig, RuntimeSecrets
from github_fs.service import AppService
from github_fs.state import StateManager
from github_fs.web import create_web_app


class FakeRepositoryInfo:
    def __init__(self, owner: str, name: str, size_kb: int = 0, private: bool = True):
        self.owner = owner
        self.name = name
        self.size_kb = size_kb
        self.private = private


class FakeGitHubClient:
    def __init__(self, owner: str):
        self.owner = owner
        self.blobs: dict[str, bytes] = {}
        self.files: dict[tuple[str, str], bytes] = {}
        self.repositories: dict[str, FakeRepositoryInfo] = {}
        self.commits: list[tuple[str, str]] = []
        self.created_repositories: list[str] = []

    def list_managed_repositories(self, owner: str, prefix: str):
        return sorted([repo for repo in self.repositories.values() if repo.name.startswith(prefix)], key=lambda item: item.name)

    def get_repository(self, owner: str, repo: str):
        return self.repositories[repo]

    def create_repository(self, owner: str, name: str, private: bool):
        info = FakeRepositoryInfo(owner, name, size_kb=0, private=private)
        self.repositories[name] = info
        self.created_repositories.append(name)
        return info

    def create_blob(self, owner: str, repo: str, payload: bytes) -> str:
        sha = hashlib.sha1(payload).hexdigest()
        self.blobs[sha] = payload
        return sha

    def commit_tree(self, owner: str, repo: str, branch: str, entries: list[dict[str, str]], message: str):
        self.commits.append((repo, message))
        for entry in entries:
            self.files[(repo, entry["path"])] = self.blobs[entry["sha"]]
        self.repositories.setdefault(repo, FakeRepositoryInfo(owner, repo))
        return f"commit-{repo}-{len(self.commits)}"

    @staticmethod
    def raw_url(owner: str, repo: str, branch: str, path: str) -> str:
        return f"memory://{owner}/{repo}/{branch}/{path}"

    def fetch_bytes(self, url: str) -> bytes:
        _, payload = url.split("://", 1)
        owner, repo, _branch, path = payload.split("/", 3)
        return self.files[(repo, path)]


def make_service(tmp_path: Path, *, daily_limit_gb: float = 1, repo_limit_kb: int = 2048):
    data_dir = tmp_path / "datos"
    state_dir = tmp_path / "state"
    data_dir.mkdir()
    state_dir.mkdir()
    config = AppConfig(
        github_accounts=(
            GitHubAccountConfig("account_1", "owner-a", "token-a"),
            GitHubAccountConfig("account_2", "owner-b", "token-b"),
        ),
        github_branch="main",
        github_uploads_prefix="storage",
        github_repository_prefix="github-fs",
        github_repository_private=True,
        github_repository_max_size_kb=repo_limit_kb,
        github_account_daily_upload_limit_gb=daily_limit_gb,
        github_chunk_size_mb=1,
        github_timeout_seconds=30,
        github_max_retry=1,
        github_backoff_seconds=1,
        github_upload_sleep_min_seconds=0.1,
        github_upload_sleep_max_seconds=0.2,
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
    sampled_sleeps: list[float] = []

    def chooser(items):
        return items[0]

    def sampler(low: float, high: float) -> float:
        sampled_sleeps.append((low + high) / 2)
        return sampled_sleeps[-1]

    service = AppService(config, secrets, manager, chooser=chooser, sleep_sampler=sampler, sleeper=lambda _: None)
    service.github_clients = {
        "account_1": FakeGitHubClient("owner-a"),
        "account_2": FakeGitHubClient("owner-b"),
    }
    manager.load(service.default_config)
    return service, data_dir, sampled_sleeps


def test_sync_verify_and_repo_metadata_are_persisted(tmp_path):
    service, data_dir, sampled_sleeps = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_bytes(b"\x00\xffgocryptfs-ciphertext\x10\x11")

    sync = service.run_sync()
    assert sync.ok is True

    state = service.get_state()
    file_id, entry = next(iter(state["files"].items()))
    version = entry["versions"][0]
    assert version["account_id"] == "account_1"
    assert version["repository"] == "github-fs-0001"
    assert state["github_accounts"]["account_1"]["repositories"]["github-fs-0001"]["last_known_size_kb"] >= 1
    assert sampled_sleeps

    verify = service.run_verify()
    assert verify.ok is True
    state = service.get_state()
    assert state["files"][file_id]["last_verification"]["account_id"] == "account_1"
    assert state["files"][file_id]["last_verification"]["remote_sha256"] == state["files"][file_id]["source_sha256"]


def test_sync_by_name_skips_known_files_without_hashing_them(tmp_path, monkeypatch):
    service, data_dir, _ = make_service(tmp_path)
    known = data_dir / "archivo1.jpg"
    known.write_bytes(b"contenido conocido")

    first_sync = service.run_sync()
    assert first_sync.ok is True

    new_file = data_dir / "archivo2.jpg"
    new_file.write_bytes(b"contenido nuevo")

    hashed_paths: list[str] = []

    def fake_sha256_file(path, chunk_size: int = 1024 * 1024):
        hashed_paths.append(path.name)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    monkeypatch.setattr("github_fs.service.sha256_file", fake_sha256_file)

    sync = service.run_sync_by_name()
    assert sync.ok is True
    assert sync.summary["skipped_files"] == 1
    assert sync.summary["uploaded_files"] == 1
    assert "archivo1.jpg" not in hashed_paths
    assert "archivo2.jpg" in hashed_paths

    state = service.get_state()
    assert len(state["files"]) == 2
    new_entry = state["files"][next(file_id for file_id, entry in state["files"].items() if entry["path"] == "archivo2.jpg")]
    assert new_entry["versions"][0]["repository"] == "github-fs-0001"


def test_creates_new_repository_when_existing_one_is_full(tmp_path):
    service, data_dir, _ = make_service(tmp_path, repo_limit_kb=1)
    client = service.github_clients["account_1"]
    client.repositories["github-fs-0001"] = FakeRepositoryInfo("owner-a", "github-fs-0001", size_kb=1)
    (data_dir / "archivo.txt").write_text("hola", encoding="utf-8")

    sync = service.run_sync()
    assert sync.ok is True

    state = service.get_state()
    version = next(iter(state["files"].values()))["versions"][0]
    assert version["repository"] == "github-fs-0002"


def test_uses_second_account_when_first_reaches_daily_limit(tmp_path):
    service, data_dir, _ = make_service(tmp_path, daily_limit_gb=0.01)
    state = service.state_manager.load(service.default_config)
    state["github_accounts"] = {
        "account_1": {
            "account_id": "account_1",
            "owner": "owner-a",
            "repositories": {},
            "daily_uploads": {service._today_bucket(): service.config.github_account_daily_upload_limit_bytes},
            "last_metadata_refresh_at": None,
            "last_upload_at": None,
        }
    }
    service.state_manager.save(state)
    (data_dir / "archivo.txt").write_text("hola", encoding="utf-8")

    sync = service.run_sync()
    assert sync.ok is True
    saved = service.get_state()
    version = next(iter(saved["files"].values()))["versions"][0]
    assert version["account_id"] == "account_2"


def test_fails_when_all_accounts_are_over_daily_limit(tmp_path):
    service, data_dir, _ = make_service(tmp_path, daily_limit_gb=0.01)
    state = service.state_manager.load(service.default_config)
    today = service._today_bucket()
    state["github_accounts"] = {
        "account_1": {
            "account_id": "account_1",
            "owner": "owner-a",
            "repositories": {},
            "daily_uploads": {today: service.config.github_account_daily_upload_limit_bytes},
            "last_metadata_refresh_at": None,
            "last_upload_at": None,
        },
        "account_2": {
            "account_id": "account_2",
            "owner": "owner-b",
            "repositories": {},
            "daily_uploads": {today: service.config.github_account_daily_upload_limit_bytes},
            "last_metadata_refresh_at": None,
            "last_upload_at": None,
        },
    }
    service.state_manager.save(state)
    (data_dir / "archivo.txt").write_text("hola", encoding="utf-8")

    sync = service.run_sync()
    assert sync.ok is False
    assert "cuota diaria" in sync.error


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
    assert b"account_1" in home.data

    trigger = client.post("/actions/verify")
    assert trigger.status_code == 302


def test_web_logs_view_reads_persisted_log_file(tmp_path):
    service, _, _ = make_service(tmp_path)
    log_path = service.config.app_state_dir / "logs" / "github-fs.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "2026-05-25 10:00:00 INFO github-fs primero\n2026-05-25 10:00:01 WARNING github-fs segundo\n",
        encoding="utf-8",
    )

    app = create_web_app(service)
    client = app.test_client()
    client.post("/login", data={"pin": "12345678"})

    response = client.get("/logs?lines=1")
    assert response.status_code == 200
    assert b"segundo" in response.data
    assert b"primero" not in response.data


def test_state_reflects_when_sync_lock_is_held(tmp_path):
    service, _, _ = make_service(tmp_path)

    with service._acquire_task_file_lock("sync") as acquired:
        assert acquired is True
        state = service.get_state()
        assert state["tasks"]["sync"]["running"] is True
