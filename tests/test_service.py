from __future__ import annotations

import hashlib
import json
from pathlib import Path

from app.config import AppConfig, GitHubAccountConfig, TelegramAccountConfig, RuntimeSecrets
from app.github_api import GitHubError
from app.service import AppService
from app.state import StateManager
from app.web import create_web_app


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
        self.initialized_branches: set[tuple[str, str]] = set()
        self.create_repository_error: Exception | None = None
        self.list_managed_repositories_error: Exception | None = None
        # Releases: tag -> release payload; assets carry their bytes.
        self.releases: dict[tuple[str, str], dict] = {}
        self.asset_bodies: dict[int, bytes] = {}
        self.uploaded_asset_names: list[str] = []
        self.deleted_asset_ids: list[int] = []
        self.list_managed_repositories_calls = 0
        self.list_release_assets_calls = 0
        # Names that should come back zero-sized once, simulating the empty
        # asset a 502 can leave behind.
        self.truncate_once: set[str] = set()
        self._next_release_id = 1
        self._next_asset_id = 5000

    def list_managed_repositories(self, owner: str, prefix: str):
        self.list_managed_repositories_calls += 1
        if self.list_managed_repositories_error:
            raise self.list_managed_repositories_error
        return sorted([repo for repo in self.repositories.values() if repo.name.startswith(prefix)], key=lambda item: item.name)

    # ── releases ────────────────────────────────────────────────────────────

    def get_release_by_tag(self, owner: str, repo: str, tag: str):
        return self.releases.get((repo, tag))

    def create_release(self, owner: str, repo: str, tag: str, name: str | None = None):
        release = {"id": self._next_release_id, "tag_name": tag, "assets": {}, "repo": repo}
        self._next_release_id += 1
        self.releases[(repo, tag)] = release
        self.repositories.setdefault(repo, FakeRepositoryInfo(owner, repo))
        return release

    def get_or_create_release(self, owner: str, repo: str, tag: str):
        return self.get_release_by_tag(owner, repo, tag) or self.create_release(owner, repo, tag)

    def _release_by_id(self, release_id: int):
        for release in self.releases.values():
            if release["id"] == release_id:
                return release
        raise KeyError(f"release {release_id} desconocida")

    def list_release_assets(self, owner: str, repo: str, release_id: int):
        self.list_release_assets_calls += 1
        return [dict(asset) for asset in self._release_by_id(release_id)["assets"].values()]

    def upload_release_asset(self, owner: str, repo: str, release_id: int, name: str, body, size: int):
        release = self._release_by_id(release_id)
        data = body.read()
        if name in release["assets"]:
            return None  # HTTP 422: the name is taken
        asset_id = self._next_asset_id
        self._next_asset_id += 1
        reported_size = len(data)
        if name in self.truncate_once:
            self.truncate_once.discard(name)
            reported_size = 0
        asset = {"id": asset_id, "name": name, "size": reported_size}
        release["assets"][name] = asset
        self.asset_bodies[asset_id] = data
        self.uploaded_asset_names.append(name)
        return dict(asset)

    def delete_release_asset(self, owner: str, repo: str, asset_id: int) -> None:
        self.deleted_asset_ids.append(asset_id)
        for release in self.releases.values():
            for name, asset in list(release["assets"].items()):
                if asset["id"] == asset_id:
                    del release["assets"][name]
        self.asset_bodies.pop(asset_id, None)

    def stream_release_asset(self, owner: str, repo: str, asset_id: int, chunk_size: int = 1 << 22):
        data = self.asset_bodies[asset_id]
        return (data[index : index + chunk_size] for index in range(0, max(len(data), 1), chunk_size))

    def asset_named(self, name: str):
        for release in self.releases.values():
            if name in release["assets"]:
                return release["assets"][name]
        return None

    def corrupt_asset(self, name: str) -> None:
        """Replace an asset's bytes keeping its size, so only a deep check catches it."""
        asset = self.asset_named(name)
        body = self.asset_bodies[asset["id"]]
        self.asset_bodies[asset["id"]] = bytes(len(body))

    def get_repository(self, owner: str, repo: str):
        return self.repositories[repo]

    def create_repository(self, owner: str, name: str, private: bool):
        if self.create_repository_error:
            raise self.create_repository_error
        info = FakeRepositoryInfo(owner, name, size_kb=0, private=private)
        self.repositories[name] = info
        self.created_repositories.append(name)
        return info

    def ensure_branch_initialized(self, owner: str, repo: str, branch: str):
        self.initialized_branches.add((repo, branch))
        self.repositories.setdefault(repo, FakeRepositoryInfo(owner, repo))

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


class FakeChannelInfo:
    def __init__(self, chat_id: int, title: str, is_private: bool = True):
        self.chat_id = chat_id
        self.title = title
        self.is_private = is_private


class FakeTelegramClient:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self._channels: list[FakeChannelInfo] = []
        self._next_chat_id = -1001000000001
        self._next_message_id = 200

    def list_managed_channels(self, prefix: str) -> list[FakeChannelInfo]:
        return [ch for ch in self._channels if ch.title.startswith(prefix)]

    def create_channel(self, title: str) -> FakeChannelInfo:
        ch = FakeChannelInfo(self._next_chat_id, title)
        self._next_chat_id -= 1
        self._channels.append(ch)
        return ch

    def commit_copy(
        self,
        chat_id: int,
        version_id: str,
        chunks_data: list,
        chunk_filenames: list,
        *,
        sleep_after_upload=None,
    ) -> dict:
        chunks_meta = []
        uploaded_bytes = 0
        for index, data in enumerate(chunks_data):
            self._next_message_id += 1
            message_id = self._next_message_id
            chunks_meta.append(
                {
                    "index": index,
                    "message_id": message_id,
                    "file_unique_id": f"uid-{message_id}",
                    "size": len(data),
                    "network": "telegram",
                }
            )
            uploaded_bytes += len(data)
            if sleep_after_upload is not None:
                sleep_after_upload()
        self._next_message_id += 1
        manifest_id = self._next_message_id
        manifest_size = 256
        uploaded_bytes += manifest_size
        return {
            "network": "telegram",
            "channel_id": chat_id,
            "manifest_message_id": manifest_id,
            "manifest_file_unique_id": f"uid-{manifest_id}",
            "uploaded_bytes": uploaded_bytes,
            "chunks": chunks_meta,
        }


def make_service_with_telegram(tmp_path: Path):
    data_dir = tmp_path / "datos"
    state_dir = tmp_path / "state"
    data_dir.mkdir()
    state_dir.mkdir()
    tg_account = TelegramAccountConfig("tg_account_1", 123456, "hash-abc", "+34600000001")
    config = AppConfig(
        github_accounts=(GitHubAccountConfig("account_1", "owner-a", "token-a"),),
        github_branch="main",
        github_uploads_prefix="storage",
        github_repository_prefix="model",
        github_repository_private=True,
        github_repository_max_size_kb=2048,
        github_account_daily_upload_limit_gb=1,
        copy_count=2,
        github_chunk_size_mb=1,
        github_timeout_seconds=30,
        github_max_retry=1,
        github_backoff_seconds=1,
        github_upload_sleep_min_seconds=0.0,
        github_upload_sleep_max_seconds=0.0,
        app_data_dir=data_dir,
        app_state_dir=state_dir,
        app_web_host="127.0.0.1",
        app_web_port=8080,
        app_sync_interval_seconds=60,
        app_verify_interval_seconds=120,
        app_web_pin="12345678",
        app_encryption_key=None,
        telegram_accounts=(tg_account,),
    )
    secrets = RuntimeSecrets(
        encryption_key="AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
        web_pin="12345678",
        flask_secret_key="secret",
    )
    manager = StateManager(state_dir)
    service = AppService(
        config, secrets, manager,
        chooser=lambda items: items[0],
        sleep_sampler=lambda low, high: 0.0,
        sleeper=lambda _: None,
    )
    service.github_clients = {"account_1": FakeGitHubClient("owner-a")}
    service.telegram_clients = {"tg_account_1": FakeTelegramClient("tg_account_1")}
    service.telegram_account_by_id = {"tg_account_1": tg_account}
    manager.load(service.default_config)
    return service, data_dir


def make_service(
    tmp_path: Path,
    *,
    daily_limit_gb: float = 1,
    repo_limit_kb: int = 2048,
    copy_count: int = 1,
    verify_deep_every_n: int = 1,
    part_size_mb: int = 1024,
):
    data_dir = tmp_path / "datos"
    state_dir = tmp_path / "state"
    data_dir.mkdir()
    state_dir.mkdir()
    return make_service_for_dirs(
        data_dir,
        state_dir,
        daily_limit_gb=daily_limit_gb,
        repo_limit_kb=repo_limit_kb,
        copy_count=copy_count,
        verify_deep_every_n=verify_deep_every_n,
        part_size_mb=part_size_mb,
    )


def make_service_for_dirs(
    data_dir: Path,
    state_dir: Path,
    *,
    daily_limit_gb: float = 1,
    repo_limit_kb: int = 2048,
    copy_count: int = 1,
    verify_deep_every_n: int = 1,
    part_size_mb: int = 1024,
):
    config = AppConfig(
        github_accounts=(
            GitHubAccountConfig("account_1", "owner-a", "token-a"),
            GitHubAccountConfig("account_2", "owner-b", "token-b"),
        ),
        github_branch="main",
        github_uploads_prefix="storage",
        github_repository_prefix="model",
        github_repository_private=True,
        github_repository_max_size_kb=repo_limit_kb,
        github_account_daily_upload_limit_gb=daily_limit_gb,
        copy_count=copy_count,
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
        verify_deep_every_n=verify_deep_every_n,
        github_part_size_mb=part_size_mb,
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


def only_file(service):
    """The single registry row, for tests that sync exactly one file."""
    rows = service.registry.list_files(limit=10)
    assert len(rows) == 1
    return rows[0]


def active_version(service, file_id: str):
    return service.registry.get_active_version(file_id)


def test_sync_verify_and_repo_metadata_are_persisted(tmp_path):
    service, data_dir, sampled_sleeps = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_bytes(b"\x00\xffgocryptfs-ciphertext\x10\x11")

    sync = service.run_sync()
    assert sync.ok is True

    state = service.get_state()
    row = only_file(service)
    version = active_version(service, row.file_id)
    assert version["account_id"] == "account_1"
    assert version["repository"] == "model-0001"
    assert version["replication_complete"] is True
    assert len(version["copies"]) == 1
    assert "model-0001" in state["github_accounts"]["account_1"]["repositories"]
    # The version is stored as release assets, one per part.
    assert version["storage"] == "release"
    assert len(version["copies"][0]["parts"]) == 1
    # The GitHub path no longer sleeps between uploads: pacing is the
    # RateLimiter's job, and the fixed per-blob sleep was pure dead time.
    assert sampled_sleeps == []
    assert ("model-0001", "main") in service.github_clients["account_1"].initialized_branches

    verify = service.run_verify()
    assert verify.ok is True
    detail = service.get_file_detail(row.file_id)
    assert detail["last_verification"]["account_id"] == "account_1"
    assert detail["last_verification"]["remote_sha256"] == detail["source_sha256"]


def test_sync_can_create_multiple_copies_in_distinct_accounts(tmp_path):
    service, data_dir, _ = make_service(tmp_path, copy_count=2)
    sample = data_dir / "archivo.txt"
    sample.write_text("contenido replicado", encoding="utf-8")

    sync = service.run_sync()
    assert sync.ok is True

    version = active_version(service, only_file(service).file_id)
    assert version["copy_count_requested"] == 2
    assert version["copy_count_completed"] == 2
    assert version["replication_complete"] is True
    assert len(version["copies"]) == 2
    assert {copy["account_id"] for copy in version["copies"]} == {"account_1", "account_2"}
    assert all(copy["network"] == "github" for copy in version["copies"])
    assert all(copy["parts"] for copy in version["copies"])
    # Each copy has its own assets, in its own account's release.
    assert len({copy["parts"][0]["release_tag"] for copy in version["copies"]}) == 2


def test_verify_checks_every_copy(tmp_path):
    service, data_dir, _ = make_service(tmp_path, copy_count=2)
    (data_dir / "archivo.txt").write_text("contenido replicado", encoding="utf-8")
    assert service.run_sync().ok is True

    verify = service.run_verify()
    assert verify.ok is True
    assert verify.summary["copies_verified"] == 2
    assert verify.summary["copies_failed"] == 0

    last_verification = service.get_file_detail(only_file(service).file_id)["last_verification"]
    assert last_verification["ok"] is True
    assert last_verification["copies_total"] == 2
    assert last_verification["copies_verified"] == 2


def test_verify_detects_corrupted_secondary_copy(tmp_path):
    service, data_dir, _ = make_service(tmp_path, copy_count=2)
    (data_dir / "archivo.txt").write_text("contenido replicado", encoding="utf-8")
    assert service.run_sync().ok is True

    # Corrupt the SECOND copy's assets only, keeping their size so the metadata
    # tier cannot see it. The legacy verify (primary copy only) would have
    # missed this; the copy-aware deep verify must catch it.
    version = active_version(service, only_file(service).file_id)
    second_copy = version["copies"][1]
    corrupt_client = service.github_clients[second_copy["account_id"]]
    for part in second_copy["parts"]:
        corrupt_client.corrupt_asset(part["name"])

    verify = service.run_verify()
    assert verify.ok is False
    assert verify.summary["copies_failed"] == 1
    assert verify.summary["copies_verified"] == 1


def test_sync_reuses_persisted_file_state_after_restart(tmp_path, monkeypatch):
    service, data_dir, _ = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_text("contenido estable", encoding="utf-8")

    first_sync = service.run_sync()
    assert first_sync.ok is True

    restarted_service, _, _ = make_service_for_dirs(data_dir, service.config.app_state_dir)
    restarted_service.github_clients = service.github_clients

    def fail_if_hashed(path, chunk_size: int = 1024 * 1024):
        raise AssertionError(f"sha256_file no deberia ejecutarse para {path}")

    monkeypatch.setattr("app.service.sha256_file", fail_if_hashed)

    sync = restarted_service.run_sync()
    assert sync.ok is True
    assert sync.summary["scanned_files"] == 1
    assert sync.summary["uploaded_files"] == 0
    assert sync.summary["failed_files"] == 0


def test_sync_reuses_already_uploaded_copy_via_sqlite(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    first = data_dir / "archivo1.jpg"
    second = data_dir / "subdir" / "archivo2.jpg"
    first.write_text("contenido duplicado", encoding="utf-8")
    second.parent.mkdir(parents=True)
    second.write_text("contenido duplicado", encoding="utf-8")

    sync = service.run_sync()
    assert sync.ok is True
    assert sync.summary["uploaded_files"] == 1
    assert sync.summary["reused_files"] == 1

    rows = service.registry.list_files(limit=10)
    assert len(rows) == 2
    assert len({row.version_id for row in rows}) == 1


def test_sync_reuses_already_uploaded_copy_after_restart_via_sqlite(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    state_dir = service.config.app_state_dir
    sample = data_dir / "archivo1.jpg"
    sample.write_text("contenido compartido", encoding="utf-8")
    assert service.run_sync().ok is True

    copied = data_dir / "archivo2.jpg"
    copied.write_text("contenido compartido", encoding="utf-8")

    restarted_service, _, _ = make_service_for_dirs(data_dir, state_dir)
    restarted_service.github_clients = service.github_clients

    sync = restarted_service.run_sync()
    assert sync.ok is True
    assert sync.summary["uploaded_files"] == 0
    assert sync.summary["reused_files"] == 1

    assert restarted_service.registry.count_files() == 2


def test_sync_detects_modifications_without_a_full_mode(tmp_path):
    """There is no full mode any more: the single sync must notice a changed
    file by itself, via (size, mtime_ns), and upload a new version."""
    service, data_dir, _ = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_text("version inicial", encoding="utf-8")

    assert service.run_sync().ok is True

    row = only_file(service)
    original_version_id = row.version_id
    original_sha = row.source_sha256

    sample.write_text("version modificada y mas larga", encoding="utf-8")

    second = service.run_sync()
    assert second.ok is True
    assert second.summary["uploaded_files"] == 1

    updated = only_file(service)
    assert updated.version_id != original_version_id
    assert updated.source_sha256 != original_sha
    assert len(service.registry.list_versions(updated.file_id)) == 2


def test_unchanged_file_is_never_hashed(tmp_path, monkeypatch):
    """The fast path must skip on (size, mtime_ns) alone — no hashing, and no
    reading of the file at all."""
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "archivo.txt").write_text("contenido estable", encoding="utf-8")
    assert service.run_sync().ok is True

    def fail_if_hashed(path, chunk_size: int = 1024 * 1024):
        raise AssertionError(f"sha256_file no deberia ejecutarse para {path}")

    monkeypatch.setattr("app.service.sha256_file", fail_if_hashed)

    second = service.run_sync()
    assert second.ok is True
    assert second.summary["scanned_files"] == 1
    assert second.summary["unchanged_files"] == 1
    assert second.summary["uploaded_files"] == 0


def test_metadata_only_change_does_not_reupload(tmp_path):
    """Same bytes, new size/mtime metadata: the hash settles it, so the file is
    re-hashed but not re-uploaded."""
    service, data_dir, _ = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_text("contenido identico", encoding="utf-8")
    assert service.run_sync().ok is True

    original = only_file(service)
    client = service.github_clients["account_1"]
    commits_before = len(client.commits)

    # Rewrite byte-identical content: mtime moves, content does not.
    sample.write_text("contenido identico", encoding="utf-8")
    import os

    os.utime(sample, ns=(original.mtime_ns + 10**9, original.mtime_ns + 10**9))

    second = service.run_sync()
    assert second.ok is True
    assert second.summary["uploaded_files"] == 0
    assert second.summary["metadata_only_files"] == 1
    assert len(client.commits) == commits_before

    updated = only_file(service)
    assert updated.version_id == original.version_id
    assert updated.mtime_ns != original.mtime_ns


def test_deleted_files_are_marked_absent_only_after_the_walk(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "a.txt").write_text("a", encoding="utf-8")
    (data_dir / "b.txt").write_text("b", encoding="utf-8")
    assert service.run_sync().ok is True
    assert service.get_stats()["absent"] == 0

    (data_dir / "b.txt").unlink()

    # A second run inside the same wall-clock second must still notice: absence
    # is keyed on the run counter, not on a one-second-resolution timestamp.
    second = service.run_sync()
    assert second.ok is True
    assert second.summary["missing_files"] == 1
    assert service.get_stats()["absent"] == 1


def test_release_rolls_over_when_it_reaches_the_asset_limit(tmp_path, monkeypatch):
    """Repository rollover is gone — releases do not count against repository
    size. What rolls over now is the release, at 1000 assets."""
    monkeypatch.setattr("app.registry.RELEASE_ASSET_LIMIT", 4)
    monkeypatch.setattr("app.service.RELEASE_ASSET_LIMIT", 4)
    service, data_dir, _ = make_service(tmp_path)
    for index in range(4):
        (data_dir / f"archivo{index}.txt").write_text(f"contenido {index}", encoding="utf-8")

    assert service.run_sync().ok is True

    releases = service.registry.list_releases("account_1")
    assert [release.tag for release in releases] == ["model-0001", "model-0002"]
    # Sealed one short of the limit, leaving the last slot for the manifest.
    assert releases[0].sealed is True
    assert releases[0].asset_count == 3
    assert releases[1].sealed is False
    assert releases[1].asset_count == 1

    # Every part landed somewhere, and each release carries its own manifest.
    client = service.github_clients["account_1"]
    assert sum(len(release["assets"]) for release in client.releases.values()) == 4 + 2


def test_a_second_noop_sync_makes_no_content_generating_requests(tmp_path):
    """The whole point of the fast path: a no-op sync must not touch GitHub."""
    service, data_dir, _ = make_service(tmp_path)
    for index in range(5):
        (data_dir / f"archivo{index}.txt").write_text(f"contenido {index}", encoding="utf-8")
    assert service.run_sync().ok is True

    client = service.github_clients["account_1"]
    uploads_before = len(client.uploaded_asset_names)
    repo_listings_before = client.list_managed_repositories_calls

    second = service.run_sync()
    assert second.ok is True
    assert second.summary["unchanged_files"] == 5
    assert len(client.uploaded_asset_names) == uploads_before
    # And the repository listing is not repeated per file — it is resolved once
    # and cached in the registry.
    assert client.list_managed_repositories_calls == repo_listings_before


def test_repository_is_listed_once_not_once_per_file(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    for index in range(8):
        (data_dir / f"archivo{index}.txt").write_text(f"contenido {index}", encoding="utf-8")

    assert service.run_sync().ok is True

    client = service.github_clients["account_1"]
    assert client.list_managed_repositories_calls == 1


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
    version = active_version(service, only_file(service).file_id)
    assert version["account_id"] == "account_2"


def test_reports_actionable_error_when_token_cannot_create_repository(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    client = service.github_clients["account_1"]
    second_client = service.github_clients["account_2"]
    client.create_repository_error = GitHubError(
        'HTTP 403: {"message":"Resource not accessible by personal access token","status":"403"}'
    )
    (data_dir / "archivo.txt").write_text("hola", encoding="utf-8")

    sync = service.run_sync()

    assert sync.ok is True
    state = service.get_state()
    version = active_version(service, only_file(service).file_id)
    assert version["account_id"] == "account_2"
    assert second_client.created_repositories == ["model-0001"]
    account_1_state = state["github_accounts"]["account_1"]
    assert account_1_state["available"] is False
    assert "retirada del pool activo" in account_1_state["unavailable_reason"]
    assert account_1_state["alerts"][0]["code"] == "personal_access_token_forbidden"


def test_removes_account_from_active_pool_when_pat_cannot_access_repositories(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    blocked_client = service.github_clients["account_1"]
    fallback_client = service.github_clients["account_2"]
    blocked_client.list_managed_repositories_error = GitHubError(
        'HTTP 403: {"message":"Resource not accessible by personal access token","status":"403"}'
    )
    (data_dir / "archivo.txt").write_text("hola", encoding="utf-8")

    sync = service.run_sync()

    assert sync.ok is True
    state = service.get_state()
    version = active_version(service, only_file(service).file_id)
    assert version["account_id"] == "account_2"
    assert fallback_client.created_repositories == ["model-0001"]
    account_1_state = state["github_accounts"]["account_1"]
    assert account_1_state["available"] is False
    assert account_1_state["alerts"][0]["code"] == "personal_access_token_forbidden"
    assert "retirada del pool activo" in account_1_state["unavailable_reason"]


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
    # La sync ya no se aborta entera al primer archivo sin destino: completa el
    # escaneo y reporta el archivo como fallido (failed_files), de modo que con
    # cuentas Telegram disponibles el resto de archivos sí podrían subirse.
    assert sync.ok is False
    assert sync.summary["failed_files"] == 1
    assert sync.summary["scanned_files"] == 1

    # El motivo real queda registrado en el archivo y menciona el cupo de GitHub.
    file_errors = [row.last_error for row in service.registry.iter_files()]
    assert any(err and "cupo diario" in err for err in file_errors)


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
    assert b"spider-back" in home.data
    assert b"account_1" in home.data

    trigger = client.post("/actions/verify")
    assert trigger.status_code == 302

    # Three operations became two: the full-sync endpoint is gone.
    assert client.post("/actions/full-sync").status_code == 404


def test_home_shows_github_account_alerts_table(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    blocked_client = service.github_clients["account_1"]
    blocked_client.create_repository_error = GitHubError(
        'HTTP 403: {"message":"Resource not accessible by personal access token","status":"403"}'
    )
    (data_dir / "archivo.txt").write_text("hola", encoding="utf-8")
    sync = service.run_sync()
    assert sync.ok is True

    app = create_web_app(service)
    client = app.test_client()
    client.post("/login", data={"pin": "12345678"})

    response = client.get("/")
    assert response.status_code == 200
    assert b"Alertas" in response.data
    assert b"owner-a (account_1)" in response.data
    assert b"retirada del pool activo" in response.data


def test_web_logs_view_reads_persisted_log_file(tmp_path):
    service, _, _ = make_service(tmp_path)
    log_path = service.config.app_state_dir / "logs" / "spider-back.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "2026-05-25 10:00:00 INFO spider-back primero\n2026-05-25 10:00:01 WARNING spider-back segundo\n",
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


def test_new_version_invalidates_prior_verification(tmp_path):
    """A successful re-upload must clear last_verification so the UI does not
    treat the old verification result as fresh for the new active version."""
    service, data_dir, _ = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_text("v1", encoding="utf-8")

    assert service.run_sync().ok is True
    assert service.run_verify().ok is True

    row = only_file(service)
    v1_id = row.version_id
    detail = service.get_file_detail(row.file_id)
    assert detail["last_verification"]["ok"] is True
    assert detail["last_verification"]["version_id"] == v1_id

    sample.write_text("v2 longer contents", encoding="utf-8")
    assert service.run_sync().ok is True

    updated = only_file(service)
    assert updated.version_id != v1_id
    assert service.get_file_detail(updated.file_id)["last_verification"] is None


def test_home_verified_count_requires_version_match(tmp_path):
    """The 'verified' tile must only count files whose stored verification is
    for the current active version."""
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "archivo.txt").write_text("v1", encoding="utf-8")

    assert service.run_sync().ok is True
    assert service.run_verify().ok is True
    assert service.get_stats()["verified"] == 1

    # Fabricate the stale condition: bump the active version without clearing
    # the verification (state that could pre-exist the fix).
    row = only_file(service)
    service.registry._execute(
        "UPDATE files SET version_id = ? WHERE file_id = ?",
        ("different-version-id", row.file_id),
    )

    assert service.get_stats()["verified"] == 0


def test_file_detail_labels_source_sha_as_upload_time(tmp_path):
    """The detail page must not label source_sha256 as 'SHA local' — that
    value reflects the SHA at upload time, not the current on-disk SHA."""
    service, data_dir, _ = make_service(tmp_path)
    sample = data_dir / "archivo.txt"
    sample.write_text("contenido", encoding="utf-8")
    assert service.run_sync().ok is True

    file_id = only_file(service).file_id

    app = create_web_app(service)
    client = app.test_client()
    client.post("/login", data={"pin": "12345678"})

    response = client.get(f"/files/{file_id}")
    assert response.status_code == 200
    body = response.data.decode("utf-8")
    assert "SHA local" not in body
    assert "SHA al subir" in body


def test_sync_places_copy_on_each_network(tmp_path):
    service, data_dir = make_service_with_telegram(tmp_path)
    (data_dir / "archivo.txt").write_bytes(b"contenido de prueba para telegram")

    sync = service.run_sync()
    assert sync.ok is True

    version = active_version(service, only_file(service).file_id)
    assert version["copy_count_requested"] == 2
    assert version["copy_count_completed"] == 2
    assert version["replication_complete"] is True
    assert len(version["copies"]) == 2
    networks = {copy["network"] for copy in version["copies"]}
    assert networks == {"github", "telegram"}
    assert service.distinct_account_copy_count(version) == 2


def _exhaust_github_quota(service):
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
    }
    service.state_manager.save(state)


def test_sync_falls_back_to_telegram_when_github_quota_exhausted(tmp_path):
    # Con GitHub sin cupo diario, la copia debe colocarse en Telegram en lugar
    # de cancelar la subida (Telegram no tiene cupo diario).
    service, data_dir = make_service_with_telegram(tmp_path)
    _exhaust_github_quota(service)
    (data_dir / "archivo.txt").write_bytes(b"contenido de prueba para telegram")

    service.run_sync()

    version = active_version(service, only_file(service).file_id)
    networks = {copy["network"] for copy in version["copies"]}
    assert "telegram" in networks
    assert "github" not in networks
    assert version["copy_count_completed"] >= 1


def test_unplaceable_file_does_not_abort_whole_sync(tmp_path):
    # Regresión: antes, si GitHub estaba sin cupo y la copia en Telegram fallaba,
    # se relanzaba NoAvailableAccountsError y se abortaba TODA la sync (summary
    # vacío). Ahora el archivo se marca como fallido y la sync completa el
    # escaneo, dejando el motivo real (Telegram) registrado.
    from app.telegram_api import TelegramError

    class FailingTelegramClient(FakeTelegramClient):
        def commit_copy(self, *args, **kwargs):
            raise TelegramError("canal no disponible")

    service, data_dir = make_service_with_telegram(tmp_path)
    service.telegram_clients = {"tg_account_1": FailingTelegramClient("tg_account_1")}
    _exhaust_github_quota(service)
    (data_dir / "archivo.txt").write_bytes(b"contenido de prueba")

    sync = service.run_sync()

    assert sync.ok is False
    # summary poblado => el escaneo terminó (no se abortó a mitad)
    assert sync.summary["scanned_files"] == 1
    assert sync.summary["failed_files"] == 1

    row = only_file(service)
    assert row.last_error
    assert "telegram" in row.last_error.lower()


def test_verify_skips_telegram_copy_but_passes_github(tmp_path):
    service, data_dir = make_service_with_telegram(tmp_path)
    (data_dir / "archivo.txt").write_bytes(b"contenido de prueba para telegram")

    assert service.run_sync().ok is True

    verify = service.run_verify()
    assert verify.ok is True
    assert verify.summary["copies_verified"] >= 1
    assert verify.summary["copies_skipped"] == 1
    assert verify.summary["copies_failed"] == 0

    detail = service.get_file_detail(only_file(service).file_id)
    assert detail["last_verification"]["ok"] is True


def test_verify_with_n_1_checks_every_file(tmp_path):
    service, data_dir, _ = make_service(tmp_path, verify_deep_every_n=1)
    for index in range(6):
        (data_dir / f"archivo{index}.txt").write_text(f"contenido {index}", encoding="utf-8")
    assert service.run_sync().ok is True

    verify = service.run_verify()
    assert verify.ok is True
    assert verify.summary["deep_checked_files"] == 6
    assert verify.summary["verified_files"] == 6


def test_verify_with_n_3_covers_every_file_across_three_runs_without_repeats(tmp_path):
    """Rotating selection: N runs must cover the whole set exactly once each."""
    service, data_dir, _ = make_service(tmp_path, verify_deep_every_n=3)
    for index in range(9):
        (data_dir / f"archivo{index}.txt").write_text(f"contenido {index}", encoding="utf-8")
    assert service.run_sync().ok is True

    seen: list[set[str]] = []
    for _ in range(3):
        service.run_verify()
        deep = {
            row.file_id
            for row in service.registry.iter_files()
            if (service.registry.verification_detail(row.file_id) or {}).get("depth") == "deep"
            and service.registry.verification_detail(row.file_id)["checked_at"] is not None
        }
        seen.append(deep)

    all_ids = {row.file_id for row in service.registry.iter_files()}
    # Every file is deep-checked at some point across the three runs...
    assert set().union(*seen) == all_ids
    # ...and each run picks a disjoint slice, so nothing is checked twice.
    counts = [len(run) for run in seen]
    assert sum(counts) == len(all_ids)


def test_legacy_index_json_files_are_imported_into_the_registry(tmp_path):
    """A state dir written by the previous version must come up with its file
    map intact — and index.json must stop carrying it."""
    import json

    data_dir = tmp_path / "datos"
    state_dir = tmp_path / "state"
    data_dir.mkdir()
    state_dir.mkdir()
    (data_dir / "archivo.txt").write_text("contenido", encoding="utf-8")

    legacy_version = {
        "version_id": "20260101T000000000000Z",
        "created_at": "2026-01-01T00:00:00+00:00",
        "network": "github",
        "account_id": "account_1",
        "repository_owner": "owner-a",
        "repository": "model-0001",
        "branch": "main",
        "plaintext_sha256": "deadbeef",
        "chunks": [],
        "encryption": {"nonce_b64": "AAAAAAAAAAAAAAAA", "algorithm": "AES-256-GCM"},
    }
    (state_dir / "index.json").write_text(
        json.dumps(
            {
                "created_at": "2026-01-01T00:00:00+00:00",
                "config": {},
                "tasks": {},
                "github_accounts": {},
                "files": {
                    "abc0123456789def": {
                        "file_id": "abc0123456789def",
                        "path": "archivo.txt",
                        "size": 9,
                        "mtime_ns": 123,
                        "source_sha256": "deadbeef",
                        "present": True,
                        "active_version_id": "20260101T000000000000Z",
                        "versions": [legacy_version],
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    service, _, _ = make_service_for_dirs(data_dir, state_dir)

    row = service.registry.get_file("abc0123456789def")
    assert row is not None
    assert row.rel_path == "archivo.txt"
    assert row.source_sha256 == "deadbeef"
    assert row.version_id == "20260101T000000000000Z"
    # The commit-based version stays readable, so it remains verifiable.
    assert service.registry.get_active_version(row.file_id)["repository"] == "model-0001"

    persisted = json.loads((state_dir / "index.json").read_text(encoding="utf-8"))
    assert "files" not in persisted
    assert "tasks" in persisted

    # Idempotent: a second boot must not re-import or crash.
    again, _, _ = make_service_for_dirs(data_dir, state_dir)
    assert again.registry.count_files() == 1


def test_large_file_is_split_into_parts_and_streamed(tmp_path):
    """~1 GiB parts are the point; here a tiny part size proves the split, the
    per-part nonces and the round trip."""
    service, data_dir, _ = make_service(tmp_path, part_size_mb=1)
    # 3 parts at a 1 MiB part size.
    payload = bytes(range(256)) * 10_000  # 2.56 MB
    (data_dir / "grande.bin").write_bytes(payload)

    assert service.run_sync().ok is True

    version = active_version(service, only_file(service).file_id)
    parts = version["copies"][0]["parts"]
    assert len(parts) == 3
    assert [part["part"] for part in parts] == [0, 1, 2]
    # Every part carries its own nonce, so each is independently decryptable.
    assert len({part["nonce_b64"] for part in parts}) == 3
    # One content-generating request per part, not five per file.
    client = service.github_clients["account_1"]
    assert len([name for name in client.uploaded_asset_names if name.endswith(".bin")]) == 3

    # And the deep verification closes the loop against the local hash.
    assert service.run_verify().ok is True


def test_encryption_never_materialises_the_file(tmp_path, monkeypatch):
    """service.py used to do file_path.read_bytes(), peaking at ~2x the file
    size. With ~1 GiB parts that is not viable."""
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "archivo.bin").write_bytes(b"x" * 4096)

    original = Path.read_bytes

    def fail_on_data_dir(self, *args, **kwargs):
        if data_dir in self.parents:
            raise AssertionError(f"read_bytes no deberia usarse para {self}")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", fail_on_data_dir)
    assert service.run_sync().ok is True


def test_duplicate_asset_name_is_deleted_and_reuploaded(tmp_path):
    """Documented 422. Deterministic names make it reachable on resume, so it
    must be recovered from rather than reported as a failure."""
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "archivo.txt").write_text("contenido", encoding="utf-8")
    client = service.github_clients["account_1"]

    real_upload = client.upload_release_asset
    squatted: dict[str, bool] = {}

    def upload_with_squatter(owner, repo, release_id, name, body, size):
        if name.endswith(".bin") and not squatted.get(name):
            squatted[name] = True
            # Something already holds this exact name (a previous interrupted run).
            client._release_by_id(release_id)["assets"][name] = {"id": 99, "name": name, "size": 5}
            client.asset_bodies[99] = b"basura"
        return real_upload(owner, repo, release_id, name, body, size)

    client.upload_release_asset = upload_with_squatter

    assert service.run_sync().ok is True
    assert 99 in client.deleted_asset_ids
    assert service.run_verify().ok is True


def test_zero_size_asset_after_a_502_is_deleted_and_retried(tmp_path):
    """A 502 can leave an empty asset behind; the size the API returns is the
    only way to notice."""
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "archivo.txt").write_text("contenido", encoding="utf-8")
    client = service.github_clients["account_1"]

    real_upload = client.upload_release_asset
    tripped: dict[str, bool] = {}

    def upload_once_empty(owner, repo, release_id, name, body, size):
        asset = real_upload(owner, repo, release_id, name, body, size)
        if asset is not None and name.endswith(".bin") and not tripped.get(name):
            tripped[name] = True
            asset["size"] = 0
            client._release_by_id(release_id)["assets"][name]["size"] = 0
        return asset

    client.upload_release_asset = upload_once_empty

    assert service.run_sync().ok is True
    assert client.deleted_asset_ids  # the empty asset was removed
    # The retry left exactly one non-empty asset for the part.
    parts = active_version(service, only_file(service).file_id)["copies"][0]["parts"]
    assert len(parts) == 1
    assert parts[0]["size"] > 0
    assert service.run_verify().ok is True


def test_interrupted_upload_is_reconciled_against_the_remote_not_reuploaded(tmp_path):
    """A row left in `uploading` must be resumed by checking which parts are
    already on the remote, not by re-uploading the whole file."""
    service, data_dir, _ = make_service(tmp_path, part_size_mb=1)
    (data_dir / "grande.bin").write_bytes(bytes(range(256)) * 10_000)  # 3 parts
    # Only account_1 may take this file, so the retry lands on the same account.
    service._runtime_unavailable_accounts.add("account_2")

    client = service.github_clients["account_1"]
    real_upload = client.upload_release_asset

    def fail_on_last_part(owner, repo, release_id, name, body, size):
        if name.endswith("-0002.bin"):
            raise GitHubError("HTTP 502: la conexion se corto")
        return real_upload(owner, repo, release_id, name, body, size)

    client.upload_release_asset = fail_on_last_part

    first = service.run_sync()
    assert first.ok is False
    assert first.summary["failed_files"] == 1
    # Parts 0 and 1 made it and were recorded.
    data_assets = [name for name in client.uploaded_asset_names if name.endswith(".bin")]
    assert len(data_assets) == 2
    assert data_assets[0].endswith("-0000.bin")
    assert data_assets[1].endswith("-0001.bin")

    row = only_file(service)
    assert row.status == "error"
    interrupted_version_id = row.version_id
    assert interrupted_version_id is not None

    # Second run: the failure is gone.
    client.upload_release_asset = real_upload
    client.uploaded_asset_names.clear()

    second = service.run_sync()
    assert second.ok is True
    assert second.summary["uploaded_files"] == 1

    # The version id survived, so only the missing part was sent: parts 0 and 1
    # were reconciled against the remote listing rather than re-uploaded.
    resumed = only_file(service)
    assert resumed.version_id == interrupted_version_id
    assert [name for name in client.uploaded_asset_names if name.endswith(".bin")] == [
        f"{resumed.file_id}-{interrupted_version_id}-0002.bin"
    ]

    version = active_version(service, resumed.file_id)
    assert len(version["copies"][0]["parts"]) == 3
    assert service.run_verify().ok is True


def test_consolidated_manifest_is_one_asset_per_release_not_per_file(tmp_path):
    """A per-file manifest would cost a second request per file and cancel out
    half the gain, so there is exactly one per release."""
    from app.asset_format import decrypt_part
    from app.service import MANIFEST_ASSET_NAME

    service, data_dir, _ = make_service(tmp_path)
    for index in range(4):
        (data_dir / f"archivo{index}.txt").write_text(f"contenido {index}", encoding="utf-8")
    assert service.run_sync().ok is True

    client = service.github_clients["account_1"]
    manifests = [name for name in client.uploaded_asset_names if name == MANIFEST_ASSET_NAME]
    assert len(manifests) == 1

    asset = client.asset_named(MANIFEST_ASSET_NAME)
    payload, _ = decrypt_part(client.asset_bodies[asset["id"]], service.secrets.encryption_key_bytes())
    manifest = json.loads(payload)
    assert manifest["release_tag"] == "model-0001"
    assert len(manifest["files"]) == 4
    # The file_id -> rel_path mapping only exists here, and it is encrypted.
    paths = {entry["path"] for entry in manifest["files"].values()}
    assert paths == {f"archivo{index}.txt" for index in range(4)}
    assert b"archivo0.txt" not in client.asset_bodies[asset["id"]]


def test_verify_metadata_tier_catches_a_missing_asset(tmp_path):
    service, data_dir, _ = make_service(tmp_path, verify_deep_every_n=100)
    (data_dir / "archivo.txt").write_text("contenido", encoding="utf-8")
    assert service.run_sync().ok is True

    client = service.github_clients["account_1"]
    part = active_version(service, only_file(service).file_id)["copies"][0]["parts"][0]
    client.delete_release_asset("owner-a", "model-0001", part["asset_id"])

    verify = service.run_verify()
    assert verify.ok is False
    detail = service.get_file_detail(only_file(service).file_id)["last_verification"]
    assert "ausente" in detail["copies"][0]["error"]


def test_verify_metadata_tier_catches_a_truncated_asset(tmp_path):
    service, data_dir, _ = make_service(tmp_path, verify_deep_every_n=100)
    (data_dir / "archivo.txt").write_text("contenido", encoding="utf-8")
    assert service.run_sync().ok is True

    client = service.github_clients["account_1"]
    part = active_version(service, only_file(service).file_id)["copies"][0]["parts"][0]
    client.asset_named(part["name"])["size"] = 12

    verify = service.run_verify()
    assert verify.ok is False


def test_legacy_blob_copy_still_verifies_through_raw_url(tmp_path):
    """Commit-based data written before the Releases backend stays readable and
    verifiable; verification dispatches on copy["storage"]."""
    from app.crypto import encrypt_bytes
    from app.utils import sha256_bytes, stable_file_id

    service, data_dir, _ = make_service(tmp_path)
    payload = b"contenido legado"
    (data_dir / "legado.txt").write_bytes(payload)

    client = service.github_clients["account_1"]
    encrypted = encrypt_bytes(payload, service.secrets.encryption_key_bytes())
    chunk_path = "storage/legacy/chunk_0000.bin"
    client.files[("model-0001", chunk_path)] = encrypted["ciphertext"]

    file_id = stable_file_id("legado.txt")
    stat = (data_dir / "legado.txt").stat()
    legacy_version = {
        "version_id": "20250101T000000000000Z",
        "created_at": "2025-01-01T00:00:00+00:00",
        "storage": "blob",
        "plaintext_sha256": encrypted["plaintext_sha256"],
        "copies": [
            {
                "copy_index": 1,
                "network": "github",
                "storage": "blob",
                "account_id": "account_1",
                "repository_owner": "owner-a",
                "repository": "model-0001",
                "branch": "main",
                "encryption": {"nonce_b64": encrypted["nonce_b64"], "algorithm": "AES-256-GCM"},
                "chunks": [
                    {
                        "index": 0,
                        "path": chunk_path,
                        "raw_url": client.raw_url("owner-a", "model-0001", "main", chunk_path),
                        "sha256": sha256_bytes(encrypted["ciphertext"]),
                        "size": len(encrypted["ciphertext"]),
                        "repository": "model-0001",
                    }
                ],
            }
        ],
        "copy_count_requested": 1,
        "copy_count_completed": 1,
        "replication_complete": True,
    }
    service.registry.upsert_file(
        file_id=file_id,
        rel_path="legado.txt",
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        source_sha256=encrypted["plaintext_sha256"],
        version_id="20250101T000000000000Z",
        status="complete",
    )
    service.registry.save_version(
        file_id, legacy_version, distinct_account_copies=1, storage="blob"
    )

    verify = service.run_verify()
    assert verify.ok is True
    detail = service.get_file_detail(file_id)["last_verification"]
    assert detail["copies"][0]["storage"] == "blob"
    assert detail["copies"][0]["remote_sha256"] == encrypted["plaintext_sha256"]


def test_empty_file_round_trips(tmp_path):
    """A zero-byte file still produces one part (header + GCM tag), so the
    non-empty-asset check must not reject it."""
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "vacio.bin").write_bytes(b"")

    assert service.run_sync().ok is True
    parts = active_version(service, only_file(service).file_id)["copies"][0]["parts"]
    assert len(parts) == 1
    assert parts[0]["size"] > 0
    assert service.run_verify().ok is True


def test_file_detail_page_describes_release_versions(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    (data_dir / "archivo.txt").write_text("contenido", encoding="utf-8")
    assert service.run_sync().ok is True

    app = create_web_app(service)
    client = app.test_client()
    client.post("/login", data={"pin": "12345678"})

    response = client.get(f"/files/{only_file(service).file_id}")
    assert response.status_code == 200
    body = response.data.decode("utf-8")
    assert "almacenamiento=release" in body
    assert "release=model-0001" in body
    assert "partes=1" in body


def test_files_listing_is_paginated(tmp_path):
    service, data_dir, _ = make_service(tmp_path)
    for index in range(5):
        (data_dir / f"archivo{index}.txt").write_text(f"contenido {index}", encoding="utf-8")
    assert service.run_sync().ok is True

    app = create_web_app(service)
    client = app.test_client()
    client.post("/login", data={"pin": "12345678"})

    first = client.get("/files?per_page=2")
    assert first.status_code == 200
    assert b"archivo0.txt" in first.data
    assert b"archivo2.txt" not in first.data
    assert b"siguiente" in first.data

    second = client.get("/files?per_page=2&page=2")
    assert b"archivo2.txt" in second.data
    assert b"archivo0.txt" not in second.data


def test_sync_walks_in_the_configured_order(tmp_path):
    """An interrupted first sync must leave a bit of everything backed up, so
    the walk order is part of the durability story, not just an internal detail."""
    service, data_dir, _ = make_service(tmp_path)
    years = ["2019", "2020", "2021", "2022"]
    for year in years:
        (data_dir / year).mkdir()
        for index in range(8):
            (data_dir / year / f"foto{index}.jpg").write_text(f"{year}-{index}", encoding="utf-8")

    uploaded_order: list[str] = []
    original = service._upload_release_copy

    def record(*args, **kwargs):
        uploaded_order.append(kwargs["rel_path"])
        return original(*args, **kwargs)

    service._upload_release_copy = record
    assert service.run_sync().ok is True
    assert len(uploaded_order) == 32

    # A quarter of the way in, the backup already spans most of the corpus...
    assert len({path.split("/")[0] for path in uploaded_order[:8]}) >= 3
    # ...and half way in, all of it.
    assert {path.split("/")[0] for path in uploaded_order[:16]} == set(years)

    # Path order, by contrast, would have uploaded only the oldest year first.
    from app.utils import iter_files

    by_path = [
        path.relative_to(data_dir).as_posix() for path in iter_files(data_dir, order="path")
    ]
    assert {path.split("/")[0] for path in by_path[:8]} == {"2019"}
