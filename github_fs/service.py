from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import AppConfig, RuntimeSecrets
from .crypto import StreamingAESGCMDecryptor, chunk_bytes, encrypt_bytes
from .github_api import GitHubClient, GitHubSettings
from .state import StateManager
from .utils import iter_files, rel_path_str, sha256_bytes, sha256_file, stable_file_id, utc_now_compact, utc_now_iso


class ServiceError(Exception):
    pass


@dataclass
class TaskResult:
    ok: bool
    summary: dict[str, Any]
    error: str | None = None


class AppService:
    def __init__(self, config: AppConfig, secrets: RuntimeSecrets, state_manager: StateManager):
        self.config = config
        self.secrets = secrets
        self.state_manager = state_manager
        self.default_config = {
            "data_dir": str(config.app_data_dir),
            "state_dir": str(config.app_state_dir),
            "repository": config.github_repository,
            "branch": config.github_branch,
            "uploads_prefix": config.github_uploads_prefix,
            "web_host": config.app_web_host,
            "web_port": config.app_web_port,
            "sync_interval_seconds": config.app_sync_interval_seconds,
            "verify_interval_seconds": config.app_verify_interval_seconds,
            "chunk_size_mb": config.github_chunk_size_mb,
        }
        self.github = GitHubClient(
            GitHubSettings(
                token=config.github_token,
                repo=config.github_repository,
                branch=config.github_branch,
                uploads_prefix=config.github_uploads_prefix,
                timeout_s=config.github_timeout_seconds,
                max_retry=config.github_max_retry,
                backoff_s=config.github_backoff_seconds,
            )
        )
        self._task_locks = {"sync": threading.Lock(), "verify": threading.Lock()}

    def get_state(self) -> dict[str, Any]:
        return self.state_manager.snapshot(self.default_config)

    def run_sync(self) -> TaskResult:
        return self._run_task("sync", self._sync_impl)

    def run_verify(self) -> TaskResult:
        return self._run_task("verify", self._verify_impl)

    def _run_task(self, task_name: str, callback) -> TaskResult:
        lock = self._task_locks[task_name]
        if not lock.acquire(blocking=False):
            state = self.get_state()
            return TaskResult(False, state["tasks"][task_name].get("last_summary", {}), "Task already running")

        try:
            state = self.state_manager.load(self.default_config)
            task = state["tasks"][task_name]
            task["running"] = True
            task["last_started_at"] = utc_now_iso()
            task["last_error"] = None
            self.state_manager.save(state)

            result = callback(state)
            state["tasks"][task_name]["running"] = False
            state["tasks"][task_name]["last_finished_at"] = utc_now_iso()
            state["tasks"][task_name]["last_result"] = "success" if result.ok else "error"
            state["tasks"][task_name]["last_error"] = result.error
            state["tasks"][task_name]["last_summary"] = result.summary
            self.state_manager.save(state)
            return result
        except Exception as exc:
            state = self.state_manager.load(self.default_config)
            state["tasks"][task_name]["running"] = False
            state["tasks"][task_name]["last_finished_at"] = utc_now_iso()
            state["tasks"][task_name]["last_result"] = "error"
            state["tasks"][task_name]["last_error"] = str(exc)
            state["tasks"][task_name]["last_summary"] = {}
            self.state_manager.save(state)
            return TaskResult(False, {}, str(exc))
        finally:
            lock.release()

    def _sync_impl(self, state: dict[str, Any]) -> TaskResult:
        if not self.config.app_data_dir.exists():
            raise ServiceError(f"No existe el directorio de datos: {self.config.app_data_dir}")

        files_state = state["files"]
        discovered_paths: set[str] = set()
        changed_files: list[tuple[str, Path, str, int, int, str]] = []

        for file_path in iter_files(self.config.app_data_dir):
            rel_path = rel_path_str(self.config.app_data_dir, file_path)
            discovered_paths.add(rel_path)
            file_id = stable_file_id(rel_path)
            size = file_path.stat().st_size
            mtime_ns = file_path.stat().st_mtime_ns
            source_sha256 = sha256_file(file_path)

            entry = files_state.get(file_id)
            active_version = None
            if entry and entry.get("active_version_id"):
                active_version = next(
                    (version for version in entry.get("versions", []) if version["version_id"] == entry["active_version_id"]),
                    None,
                )

            if active_version and active_version.get("plaintext_sha256") == source_sha256 and entry.get("present"):
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["source_sha256"] = source_sha256
                entry["present"] = True
                continue

            changed_files.append((file_id, file_path, rel_path, size, mtime_ns, source_sha256))

        for file_id, entry in files_state.items():
            if entry["path"] not in discovered_paths:
                entry["present"] = False
                entry["last_seen_at"] = utc_now_iso()

        tree_entries: list[dict[str, Any]] = []
        staged_updates: list[tuple[str, dict[str, Any]]] = []

        for file_id, file_path, rel_path, size, mtime_ns, source_sha256 in changed_files:
            plaintext = file_path.read_bytes()
            encrypted = encrypt_bytes(plaintext, self.secrets.encryption_key_bytes())
            version_id = utc_now_compact()
            remote_prefix = f"{self.config.github_uploads_prefix}/{file_id}/{version_id}"
            version_manifest: dict[str, Any] = {
                "version": 1,
                "file_id": file_id,
                "path": rel_path,
                "version_id": version_id,
                "created_at": utc_now_iso(),
                "plaintext_sha256": encrypted["plaintext_sha256"],
                "ciphertext_sha256": encrypted["ciphertext_sha256"],
                "size": size,
                "mtime_ns": mtime_ns,
                "encryption": {
                    "algorithm": encrypted["algorithm"],
                    "nonce_b64": encrypted["nonce_b64"],
                    "key_id": "state-default",
                },
                "chunks": [],
            }

            for chunk_index, chunk in chunk_bytes(encrypted["ciphertext"], self.config.github_chunk_size_bytes):
                chunk_sha = self.github.create_blob(chunk)
                chunk_path = f"{remote_prefix}/chunk_{chunk_index:04d}.bin"
                tree_entries.append({"path": chunk_path, "mode": "100644", "type": "blob", "sha": chunk_sha})
                version_manifest["chunks"].append(
                    {
                        "index": chunk_index,
                        "path": chunk_path,
                        "raw_url": self.github.raw_url(chunk_path),
                        "sha256": sha256_bytes(chunk),
                        "size": len(chunk),
                    }
                )

            manifest_bytes = json.dumps(version_manifest, ensure_ascii=False, indent=2).encode("utf-8")
            manifest_sha = self.github.create_blob(manifest_bytes)
            manifest_path = f"{remote_prefix}/manifest.json"
            tree_entries.append({"path": manifest_path, "mode": "100644", "type": "blob", "sha": manifest_sha})

            staged_updates.append(
                (
                    file_id,
                    {
                        "path": rel_path,
                        "present": True,
                        "size": size,
                        "mtime_ns": mtime_ns,
                        "source_sha256": source_sha256,
                        "last_seen_at": utc_now_iso(),
                        "version": {
                            "version_id": version_id,
                            "created_at": version_manifest["created_at"],
                            "plaintext_sha256": encrypted["plaintext_sha256"],
                            "ciphertext_sha256": encrypted["ciphertext_sha256"],
                            "size": size,
                            "manifest_path": manifest_path,
                            "manifest_raw_url": self.github.raw_url(manifest_path),
                            "encryption": version_manifest["encryption"],
                            "chunks": version_manifest["chunks"],
                        },
                    },
                )
            )

        commit_sha = self.github.commit_tree(
            tree_entries,
            f"github-fs sync {utc_now_iso()} ({len(changed_files)} files)",
        )

        for file_id, payload in staged_updates:
            entry = files_state.setdefault(
                file_id,
                {
                    "file_id": file_id,
                    "path": payload["path"],
                    "versions": [],
                    "active_version_id": None,
                    "last_verification": None,
                    "last_error": None,
                },
            )
            entry["path"] = payload["path"]
            entry["present"] = payload["present"]
            entry["size"] = payload["size"]
            entry["mtime_ns"] = payload["mtime_ns"]
            entry["source_sha256"] = payload["source_sha256"]
            entry["last_seen_at"] = payload["last_seen_at"]
            version = payload["version"]
            version["commit_sha"] = commit_sha
            entry.setdefault("versions", []).append(version)
            entry["active_version_id"] = version["version_id"]
            entry["last_error"] = None

        summary = {
            "scanned_files": len(discovered_paths),
            "uploaded_files": len(changed_files),
            "commit_sha": commit_sha,
            "missing_files": sum(1 for entry in files_state.values() if not entry.get("present")),
        }
        return TaskResult(True, summary)

    def _verify_impl(self, state: dict[str, Any]) -> TaskResult:
        files_state = state["files"]
        verified = 0
        failures = 0

        for entry in files_state.values():
            if not entry.get("present"):
                continue
            active_version = self._get_active_version(entry)
            if not active_version:
                continue

            local_path = self.config.app_data_dir / entry["path"]
            if not local_path.exists():
                entry["present"] = False
                entry["last_error"] = "Archivo ausente durante la verificacion."
                failures += 1
                continue

            local_sha = sha256_file(local_path)
            try:
                decryptor = StreamingAESGCMDecryptor(
                    self.secrets.encryption_key_bytes(),
                    active_version["encryption"]["nonce_b64"],
                )
                downloaded_chunks = 0
                for chunk in sorted(active_version["chunks"], key=lambda item: item["index"]):
                    data = self.github.fetch_bytes(chunk["raw_url"])
                    if sha256_bytes(data) != chunk["sha256"]:
                        raise ServiceError(f"Chunk corrupto: {entry['path']}#{chunk['index']}")
                    decryptor.update(data)
                    downloaded_chunks += 1
                _, remote_sha = decryptor.finalize()
                if remote_sha != local_sha:
                    raise ServiceError(
                        f"Hash distinto para {entry['path']}: local={local_sha} remoto={remote_sha}"
                    )

                entry["last_verification"] = {
                    "checked_at": utc_now_iso(),
                    "ok": True,
                    "local_sha256": local_sha,
                    "remote_sha256": remote_sha,
                    "version_id": active_version["version_id"],
                    "chunks_checked": downloaded_chunks,
                }
                entry["last_error"] = None
                verified += 1
            except Exception as exc:
                entry["last_verification"] = {
                    "checked_at": utc_now_iso(),
                    "ok": False,
                    "local_sha256": local_sha,
                    "version_id": active_version["version_id"],
                }
                entry["last_error"] = str(exc)
                failures += 1

        self.state_manager.save(state)
        return TaskResult(
            failures == 0,
            {
                "verified_files": verified,
                "failed_files": failures,
                "present_files": sum(1 for entry in files_state.values() if entry.get("present")),
            },
            None if failures == 0 else f"{failures} archivos con error",
        )

    @staticmethod
    def _get_active_version(entry: dict[str, Any]) -> dict[str, Any] | None:
        active_version_id = entry.get("active_version_id")
        if not active_version_id:
            return None
        return next(
            (version for version in entry.get("versions", []) if version["version_id"] == active_version_id),
            None,
        )

    def mark_manual_trigger(self, task_name: str) -> None:
        state = self.state_manager.load(self.default_config)
        state["tasks"][task_name]["last_manual_trigger_at"] = utc_now_iso()
        self.state_manager.save(state)
