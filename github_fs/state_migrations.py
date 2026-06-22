from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from .utils import utc_now_iso


def migrate_state(state: dict[str, Any]) -> dict[str, Any]:
    migrated = deepcopy(state)
    _migrate_tasks(migrated)
    migrated.setdefault("files", {})
    migrated.setdefault("github_accounts", {})
    migrated.setdefault("created_at", utc_now_iso())
    _migrate_github_accounts(migrated)
    _migrate_files(migrated)
    return migrated


def migrate_index_file(path: Path) -> dict[str, Any]:
    state = json.loads(path.read_text(encoding="utf-8"))
    migrated = migrate_state(state)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(migrated, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return migrated


def _migrate_tasks(state: dict[str, Any]) -> None:
    tasks = state.setdefault("tasks", {})
    tasks.pop("sync_by_name", None)

    default_tasks = {
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
    }

    for task_name, defaults in default_tasks.items():
        task_state = tasks.setdefault(task_name, {})
        for key, value in defaults.items():
            task_state.setdefault(key, value)


def _migrate_github_accounts(state: dict[str, Any]) -> None:
    github_accounts = state.setdefault("github_accounts", {})
    for account_id, account_state in github_accounts.items():
        account_state.setdefault("account_id", account_id)
        account_state.setdefault("network", "github")
        account_state.setdefault("repositories", {})
        account_state.setdefault("daily_uploads", {})
        account_state.setdefault("last_metadata_refresh_at", None)
        account_state.setdefault("last_upload_at", None)
        account_state.setdefault("available", True)
        account_state.setdefault("unavailable_reason", None)
        account_state.setdefault("unavailable_since", None)
        account_state.setdefault("alerts", [])

        repositories = account_state.setdefault("repositories", {})
        for repository_name, repository_state in repositories.items():
            repository_state.setdefault("name", repository_name)
            repository_state.setdefault("owner", account_state.get("owner"))
            repository_state.setdefault("network", account_state.get("network", "github"))
            repository_state.setdefault("last_known_size_kb", 0)
            repository_state.setdefault("private", True)
            repository_state.setdefault("last_refreshed_at", None)


def _migrate_files(state: dict[str, Any]) -> None:
    files = state.setdefault("files", {})
    for file_id, entry in files.items():
        entry.setdefault("file_id", file_id)
        entry.setdefault("versions", [])
        entry.setdefault("last_verification", None)
        entry.setdefault("last_error", None)
        for version in entry["versions"]:
            _migrate_version(version)


def _migrate_version(version: dict[str, Any]) -> None:
    copies = version.get("copies")
    if not copies:
        copy = _copy_from_legacy_version(version, copy_index=1)
        copies = [copy]
        version["copies"] = copies
        version.setdefault("copy_count_requested", 1)
        version.setdefault("copy_count_completed", 1)
        version.setdefault("replication_complete", True)
        version.setdefault("copy_errors", [])
    else:
        normalized_copies = []
        for index, copy in enumerate(copies, start=1):
            normalized_copies.append(_normalize_copy(copy, version, index))
        version["copies"] = normalized_copies
        version.setdefault("copy_count_requested", len(normalized_copies))
        version.setdefault("copy_count_completed", len(normalized_copies))
        version.setdefault(
            "replication_complete",
            len(normalized_copies) >= int(version.get("copy_count_requested", len(normalized_copies))),
        )
        version.setdefault("copy_errors", [])

    primary_copy = version["copies"][0]
    version.setdefault("network", primary_copy.get("network", "github"))
    version.setdefault("account_id", primary_copy.get("account_id"))
    version.setdefault("repository_owner", primary_copy.get("repository_owner"))
    version.setdefault("repository", primary_copy.get("repository"))
    version.setdefault("branch", primary_copy.get("branch"))
    version.setdefault("manifest_path", primary_copy.get("manifest_path"))
    version.setdefault("manifest_raw_url", primary_copy.get("manifest_raw_url"))
    version.setdefault("commit_sha", primary_copy.get("commit_sha"))
    version.setdefault("uploaded_bytes", sum(int(copy.get("uploaded_bytes", 0)) for copy in version["copies"]))
    version.setdefault("encryption", deepcopy(primary_copy.get("encryption", {})))
    version.setdefault("chunks", deepcopy(primary_copy.get("chunks", [])))


def _copy_from_legacy_version(version: dict[str, Any], *, copy_index: int) -> dict[str, Any]:
    return _normalize_copy(
        {
            "copy_index": copy_index,
            "network": version.get("network", "github"),
            "account_id": version.get("account_id"),
            "repository_owner": version.get("repository_owner"),
            "repository": version.get("repository"),
            "branch": version.get("branch"),
            "manifest_path": version.get("manifest_path"),
            "manifest_raw_url": version.get("manifest_raw_url"),
            "commit_sha": version.get("commit_sha"),
            "uploaded_bytes": version.get("uploaded_bytes", 0),
            "encryption": deepcopy(version.get("encryption", {})),
            "chunks": deepcopy(version.get("chunks", [])),
            "version_id": version.get("version_id"),
            "created_at": version.get("created_at"),
            "file_id": version.get("file_id"),
            "path": version.get("path"),
            "plaintext_sha256": version.get("plaintext_sha256"),
            "ciphertext_sha256": version.get("ciphertext_sha256"),
            "source_sha256": version.get("source_sha256"),
            "size": version.get("size"),
            "mtime_ns": version.get("mtime_ns"),
        },
        version,
        copy_index,
    )


def _normalize_copy(copy: dict[str, Any], version: dict[str, Any], copy_index: int) -> dict[str, Any]:
    normalized = deepcopy(copy)
    normalized.setdefault("copy_index", copy_index)
    normalized.setdefault("network", copy.get("network", version.get("network", "github")))
    normalized.setdefault("account_id", copy.get("account_id", version.get("account_id")))
    normalized.setdefault("repository_owner", copy.get("repository_owner", version.get("repository_owner")))
    normalized.setdefault("repository", copy.get("repository", version.get("repository")))
    normalized.setdefault("branch", copy.get("branch", version.get("branch")))
    normalized.setdefault("manifest_path", copy.get("manifest_path", version.get("manifest_path")))
    normalized.setdefault("manifest_raw_url", copy.get("manifest_raw_url", version.get("manifest_raw_url")))
    normalized.setdefault("commit_sha", copy.get("commit_sha", version.get("commit_sha")))
    normalized.setdefault("uploaded_bytes", copy.get("uploaded_bytes", 0))
    normalized.setdefault("version_id", copy.get("version_id", version.get("version_id")))
    normalized.setdefault("created_at", copy.get("created_at", version.get("created_at")))
    normalized.setdefault("file_id", copy.get("file_id", version.get("file_id")))
    normalized.setdefault("path", copy.get("path", version.get("path")))
    normalized.setdefault("plaintext_sha256", copy.get("plaintext_sha256", version.get("plaintext_sha256")))
    normalized.setdefault("ciphertext_sha256", copy.get("ciphertext_sha256", version.get("ciphertext_sha256")))
    normalized.setdefault("source_sha256", copy.get("source_sha256", version.get("source_sha256")))
    normalized.setdefault("size", copy.get("size", version.get("size")))
    normalized.setdefault("mtime_ns", copy.get("mtime_ns", version.get("mtime_ns")))
    normalized.setdefault("encryption", deepcopy(copy.get("encryption", version.get("encryption", {}))))
    normalized.setdefault("chunks", deepcopy(copy.get("chunks", version.get("chunks", []))))
    return normalized
