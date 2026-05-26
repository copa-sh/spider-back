from __future__ import annotations

import fcntl
import json
import logging
import math
import random
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .config import AppConfig, GitHubAccountConfig, RuntimeSecrets
from .crypto import StreamingAESGCMDecryptor, chunk_bytes, encrypt_bytes
from .github_api import GitHubClient, GitHubError, GitHubSettings, RepositoryInfo
from .state import StateManager
from .utils import (
    iter_files,
    rel_path_str,
    sha256_bytes,
    sha256_file,
    stable_file_id,
    utc_now_compact,
    utc_now_iso,
)


LOGGER = logging.getLogger("github-fs")


class ServiceError(Exception):
    pass


@dataclass
class TaskResult:
    ok: bool
    summary: dict[str, Any]
    error: str | None = None


@dataclass(frozen=True)
class UploadTarget:
    account_id: str
    owner: str
    repository: str
    branch: str
    repository_private: bool


class AppService:
    def __init__(
        self,
        config: AppConfig,
        secrets: RuntimeSecrets,
        state_manager: StateManager,
        chooser: Callable[[list[Any]], Any] | None = None,
        sleep_sampler: Callable[[float, float], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ):
        self.config = config
        self.secrets = secrets
        self.state_manager = state_manager
        self.default_config = {
            "data_dir": str(config.app_data_dir),
            "state_dir": str(config.app_state_dir),
            "github_accounts": [{"account_id": account.account_id, "owner": account.owner} for account in config.github_accounts],
            "branch": config.github_branch,
            "uploads_prefix": config.github_uploads_prefix,
            "repository_prefix": config.github_repository_prefix,
            "repository_private": config.github_repository_private,
            "repository_max_size_kb": config.github_repository_max_size_kb,
            "daily_upload_limit_gb": config.github_account_daily_upload_limit_gb,
            "web_host": config.app_web_host,
            "web_port": config.app_web_port,
            "sync_interval_seconds": config.app_sync_interval_seconds,
            "verify_interval_seconds": config.app_verify_interval_seconds,
            "chunk_size_mb": config.github_chunk_size_mb,
            "upload_sleep_min_seconds": config.github_upload_sleep_min_seconds,
            "upload_sleep_max_seconds": config.github_upload_sleep_max_seconds,
        }
        self.github_clients = {
            account.account_id: GitHubClient(
                GitHubSettings(
                    token=account.token,
                    owner=account.owner,
                    timeout_s=config.github_timeout_seconds,
                    max_retry=config.github_max_retry,
                    backoff_s=config.github_backoff_seconds,
                )
            )
            for account in config.github_accounts
        }
        self.account_by_id = {account.account_id: account for account in config.github_accounts}
        self._task_locks = {"sync": threading.Lock(), "sync_by_name": threading.Lock(), "verify": threading.Lock()}
        self._task_lock_paths = {
            "sync": self.config.app_state_dir / "sync.lock",
            "sync_by_name": self.config.app_state_dir / "sync_by_name.lock",
            "verify": self.config.app_state_dir / "verify.lock",
        }
        self._runtime_unavailable_accounts: set[str] = set()
        self._choose = chooser or random.choice
        self._sleep_sampler = sleep_sampler or random.uniform
        self._sleeper = sleeper or time.sleep

    def get_state(self) -> dict[str, Any]:
        state = self.state_manager.snapshot(self.default_config)
        self._refresh_task_running_flags(state)
        self._augment_state_for_web(state)
        return state

    def run_sync(self) -> TaskResult:
        return self._run_task("sync", lambda state: self._sync_impl(state, full=False))

    def run_full_sync(self) -> TaskResult:
        return self._run_task("sync", lambda state: self._sync_impl(state, full=True))

    def run_sync_by_name(self) -> TaskResult:
        return self._run_task("sync_by_name", self._sync_by_name_impl)

    def run_verify(self) -> TaskResult:
        return self._run_task("verify", self._verify_impl)

    def _run_task(self, task_name: str, callback) -> TaskResult:
        process_lock = self._task_locks[task_name]
        if not process_lock.acquire(blocking=False):
            state = self.get_state()
            LOGGER.info("%s omitido: ya hay otra ejecucion en curso.", task_name)
            return TaskResult(False, state["tasks"][task_name].get("last_summary", {}), "Task already running")

        try:
            with self._acquire_task_file_lock(task_name) as acquired:
                if not acquired:
                    state = self.get_state()
                    LOGGER.info("%s omitido: lock global ocupado por otro proceso.", task_name)
                    return TaskResult(False, state["tasks"][task_name].get("last_summary", {}), "Task already running")

                state = self.state_manager.load(self.default_config)
                task = state["tasks"][task_name]
                task["running"] = True
                task["last_started_at"] = utc_now_iso()
                task["last_error"] = None
                self.state_manager.save(state)
                LOGGER.info("%s iniciado.", task_name)

                result = callback(state)
                state["tasks"][task_name]["running"] = False
                state["tasks"][task_name]["last_finished_at"] = utc_now_iso()
                state["tasks"][task_name]["last_result"] = "success" if result.ok else "error"
                state["tasks"][task_name]["last_error"] = result.error
                state["tasks"][task_name]["last_summary"] = result.summary
                self.state_manager.save(state)
                LOGGER.info("%s finalizado con resultado=%s resumen=%s", task_name, "success" if result.ok else "error", result.summary)
                return result
        except Exception as exc:
            state = self.state_manager.load(self.default_config)
            state["tasks"][task_name]["running"] = False
            state["tasks"][task_name]["last_finished_at"] = utc_now_iso()
            state["tasks"][task_name]["last_result"] = "error"
            state["tasks"][task_name]["last_error"] = str(exc)
            state["tasks"][task_name]["last_summary"] = {}
            self.state_manager.save(state)
            LOGGER.exception("%s fallo con excepcion no controlada.", task_name)
            return TaskResult(False, {}, str(exc))
        finally:
            process_lock.release()

    @contextmanager
    def _acquire_task_file_lock(self, task_name: str):
        lock_path = self._task_lock_paths[task_name]
        lock_path.touch(exist_ok=True)
        with lock_path.open("r+", encoding="utf-8") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return

            try:
                yield True
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _refresh_task_running_flags(self, state: dict[str, Any]) -> None:
        tasks = state.setdefault("tasks", {})
        for task_name in ("sync", "sync_by_name", "verify"):
            task_state = tasks.setdefault(task_name, {})
            task_state["running"] = self._is_task_file_lock_held(task_name)

    def _is_task_file_lock_held(self, task_name: str) -> bool:
        lock_path = self._task_lock_paths[task_name]
        lock_path.touch(exist_ok=True)
        with lock_path.open("r+", encoding="utf-8") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            return False

    def _sync_impl(self, state: dict[str, Any], *, full: bool) -> TaskResult:
        if not self.config.app_data_dir.exists():
            raise ServiceError(f"No existe el directorio de datos: {self.config.app_data_dir}")

        self._ensure_github_accounts_state(state)
        LOGGER.info("sync escaneando directorio=%s modo=%s", self.config.app_data_dir, "full" if full else "light")
        files_state = state["files"]
        discovered_paths: set[str] = set()
        changed_files: list[tuple[str, Path, str, int, int, str]] = []
        scanned_files = 0
        unchanged_files = 0
        last_scan_log_at = time.monotonic()

        for file_path in iter_files(self.config.app_data_dir):
            rel_path = rel_path_str(self.config.app_data_dir, file_path)
            discovered_paths.add(rel_path)
            scanned_files += 1
            file_id = stable_file_id(rel_path)
            stat = file_path.stat()
            size = stat.st_size
            mtime_ns = stat.st_mtime_ns

            entry = files_state.get(file_id)
            active_version = self._get_active_version(entry) if entry else None
            if not full and self._can_trust_persisted_file_state(entry, active_version):
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["present"] = True
                entry["last_seen_at"] = utc_now_iso()
                unchanged_files += 1
                if scanned_files == 1 or scanned_files % 100 == 0 or time.monotonic() - last_scan_log_at >= 10:
                    LOGGER.info(
                        "sync escaneo progreso: revisados=%s pendientes=%s sin_cambios=%s ultimo=%s",
                        scanned_files,
                        len(changed_files),
                        unchanged_files,
                        rel_path,
                    )
                    last_scan_log_at = time.monotonic()
                continue

            source_sha256 = sha256_file(file_path)
            if active_version and active_version.get("plaintext_sha256") == source_sha256 and entry.get("present"):
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["source_sha256"] = source_sha256
                entry["present"] = True
                entry["last_seen_at"] = utc_now_iso()
                unchanged_files += 1
                if scanned_files == 1 or scanned_files % 100 == 0 or time.monotonic() - last_scan_log_at >= 10:
                    LOGGER.info(
                        "sync escaneo progreso: revisados=%s pendientes=%s sin_cambios=%s ultimo=%s",
                        scanned_files,
                        len(changed_files),
                        unchanged_files,
                        rel_path,
                    )
                    last_scan_log_at = time.monotonic()
                continue

            changed_files.append((file_id, file_path, rel_path, size, mtime_ns, source_sha256))
            if scanned_files == 1 or scanned_files % 100 == 0 or time.monotonic() - last_scan_log_at >= 10:
                LOGGER.info(
                    "sync escaneo progreso: revisados=%s pendientes=%s sin_cambios=%s ultimo=%s",
                    scanned_files,
                    len(changed_files),
                    unchanged_files,
                    rel_path,
                )
                last_scan_log_at = time.monotonic()

        LOGGER.info(
            "sync analizado: %s archivos detectados, %s pendientes de subida, %s ya estaban al dia.",
            len(discovered_paths),
            len(changed_files),
            len(discovered_paths) - len(changed_files),
        )

        for entry in files_state.values():
            if entry["path"] not in discovered_paths:
                entry["present"] = False
                entry["last_seen_at"] = utc_now_iso()

        uploaded_files = 0
        failed_files = 0
        uploaded_bytes = 0

        for file_id, file_path, rel_path, size, mtime_ns, source_sha256 in changed_files:
            entry = files_state.setdefault(
                file_id,
                {
                    "file_id": file_id,
                    "path": rel_path,
                    "versions": [],
                    "active_version_id": None,
                    "last_verification": None,
                    "last_error": None,
                },
            )
            try:
                LOGGER.info("sync subiendo archivo path=%s size=%sB", rel_path, size)
                version = self._upload_file_version(state, file_id, file_path, rel_path, size, mtime_ns, source_sha256)
                entry["path"] = rel_path
                entry["present"] = True
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["source_sha256"] = source_sha256
                entry["last_seen_at"] = utc_now_iso()
                entry.setdefault("versions", []).append(version)
                entry["active_version_id"] = version["version_id"]
                entry["last_error"] = None
                uploaded_files += 1
                uploaded_bytes += version["uploaded_bytes"]
                LOGGER.info(
                    "sync archivo subido path=%s cuenta=%s repo=%s version=%s bytes=%s",
                    rel_path,
                    version["account_id"],
                    version["repository"],
                    version["version_id"],
                    version["uploaded_bytes"],
                )
            except Exception as exc:
                entry["path"] = rel_path
                entry["present"] = True
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["source_sha256"] = source_sha256
                entry["last_seen_at"] = utc_now_iso()
                entry["last_error"] = str(exc)
                failed_files += 1
                LOGGER.exception("sync error subiendo path=%s", rel_path)

        self.state_manager.save(state)
        summary = {
            "scanned_files": len(discovered_paths),
            "uploaded_files": uploaded_files,
            "failed_files": failed_files,
            "uploaded_bytes": uploaded_bytes,
            "missing_files": sum(1 for entry in files_state.values() if not entry.get("present")),
        }
        return TaskResult(failed_files == 0, summary, None if failed_files == 0 else f"{failed_files} archivos con error")

    def _sync_by_name_impl(self, state: dict[str, Any]) -> TaskResult:
        if not self.config.app_data_dir.exists():
            raise ServiceError(f"No existe el directorio de datos: {self.config.app_data_dir}")

        self._ensure_github_accounts_state(state)
        LOGGER.info("sync por nombre escaneando directorio=%s", self.config.app_data_dir)
        files_state = state["files"]
        discovered_paths: set[str] = set()
        new_files: list[tuple[str, Path, str, int, int, str]] = []
        scanned_files = 0
        skipped_files = 0
        last_scan_log_at = time.monotonic()

        for file_path in iter_files(self.config.app_data_dir):
            rel_path = rel_path_str(self.config.app_data_dir, file_path)
            discovered_paths.add(rel_path)
            scanned_files += 1
            file_id = stable_file_id(rel_path)
            stat = file_path.stat()
            size = stat.st_size
            mtime_ns = stat.st_mtime_ns

            entry = files_state.get(file_id)
            active_version = self._get_active_version(entry) if entry else None
            if active_version and entry.get("present"):
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["present"] = True
                entry["last_seen_at"] = utc_now_iso()
                skipped_files += 1
                if scanned_files == 1 or scanned_files % 100 == 0 or time.monotonic() - last_scan_log_at >= 10:
                    LOGGER.info(
                        "sync por nombre progreso: revisados=%s nuevos=%s omitidos=%s ultimo=%s",
                        scanned_files,
                        len(new_files),
                        skipped_files,
                        rel_path,
                    )
                    last_scan_log_at = time.monotonic()
                continue

            source_sha256 = sha256_file(file_path)
            new_files.append((file_id, file_path, rel_path, size, mtime_ns, source_sha256))
            if scanned_files == 1 or scanned_files % 100 == 0 or time.monotonic() - last_scan_log_at >= 10:
                LOGGER.info(
                    "sync por nombre progreso: revisados=%s nuevos=%s omitidos=%s ultimo=%s",
                    scanned_files,
                    len(new_files),
                    skipped_files,
                    rel_path,
                )
                last_scan_log_at = time.monotonic()

        LOGGER.info(
            "sync por nombre analizado: %s archivos detectados, %s nuevos pendientes de subida, %s omitidos por existir ya en el catalogo.",
            len(discovered_paths),
            len(new_files),
            skipped_files,
        )

        for entry in files_state.values():
            if entry["path"] not in discovered_paths:
                entry["present"] = False
                entry["last_seen_at"] = utc_now_iso()

        uploaded_files = 0
        failed_files = 0
        uploaded_bytes = 0

        for file_id, file_path, rel_path, size, mtime_ns, source_sha256 in new_files:
            entry = files_state.setdefault(
                file_id,
                {
                    "file_id": file_id,
                    "path": rel_path,
                    "versions": [],
                    "active_version_id": None,
                    "last_verification": None,
                    "last_error": None,
                },
            )
            try:
                LOGGER.info("sync por nombre subiendo archivo path=%s size=%sB", rel_path, size)
                version = self._upload_file_version(state, file_id, file_path, rel_path, size, mtime_ns, source_sha256)
                entry["path"] = rel_path
                entry["present"] = True
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["source_sha256"] = source_sha256
                entry["last_seen_at"] = utc_now_iso()
                entry.setdefault("versions", []).append(version)
                entry["active_version_id"] = version["version_id"]
                entry["last_error"] = None
                uploaded_files += 1
                uploaded_bytes += version["uploaded_bytes"]
                LOGGER.info(
                    "sync por nombre archivo subido path=%s cuenta=%s repo=%s version=%s bytes=%s",
                    rel_path,
                    version["account_id"],
                    version["repository"],
                    version["version_id"],
                    version["uploaded_bytes"],
                )
            except Exception as exc:
                entry["path"] = rel_path
                entry["present"] = True
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["source_sha256"] = source_sha256
                entry["last_seen_at"] = utc_now_iso()
                entry["last_error"] = str(exc)
                failed_files += 1
                LOGGER.exception("sync por nombre error subiendo path=%s", rel_path)

        self.state_manager.save(state)
        summary = {
            "mode": "name-based",
            "scanned_files": len(discovered_paths),
            "skipped_files": skipped_files,
            "uploaded_files": uploaded_files,
            "failed_files": failed_files,
            "uploaded_bytes": uploaded_bytes,
            "missing_files": sum(1 for entry in files_state.values() if not entry.get("present")),
        }
        return TaskResult(failed_files == 0, summary, None if failed_files == 0 else f"{failed_files} archivos con error")

    def _verify_impl(self, state: dict[str, Any]) -> TaskResult:
        self._ensure_github_accounts_state(state)
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
                account_id = self._resolve_version_account_id(active_version)
                client = self._client_for_account(account_id)
                decryptor = StreamingAESGCMDecryptor(
                    self.secrets.encryption_key_bytes(),
                    active_version["encryption"]["nonce_b64"],
                )
                downloaded_chunks = 0
                for chunk in sorted(active_version["chunks"], key=lambda item: item["index"]):
                    data = client.fetch_bytes(chunk["raw_url"])
                    if sha256_bytes(data) != chunk["sha256"]:
                        raise ServiceError(f"Chunk corrupto: {entry['path']}#{chunk['index']}")
                    decryptor.update(data)
                    downloaded_chunks += 1
                _, remote_sha = decryptor.finalize()
                if remote_sha != local_sha:
                    raise ServiceError(f"Hash distinto para {entry['path']}: local={local_sha} remoto={remote_sha}")

                entry["last_verification"] = {
                    "checked_at": utc_now_iso(),
                    "ok": True,
                    "local_sha256": local_sha,
                    "remote_sha256": remote_sha,
                    "version_id": active_version["version_id"],
                    "chunks_checked": downloaded_chunks,
                    "account_id": account_id,
                    "repository": active_version.get("repository"),
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
    def _can_trust_persisted_file_state(
        entry: dict[str, Any] | None,
        active_version: dict[str, Any] | None,
    ) -> bool:
        if not entry or not active_version:
            return False
        if not entry.get("present"):
            return False
        source_sha256 = entry.get("source_sha256")
        if not source_sha256:
            return False
        if active_version.get("plaintext_sha256") != source_sha256:
            return False
        return True

    def _upload_file_version(
        self,
        state: dict[str, Any],
        file_id: str,
        file_path: Path,
        rel_path: str,
        size: int,
        mtime_ns: int,
        source_sha256: str,
    ) -> dict[str, Any]:
        plaintext = file_path.read_bytes()
        encrypted = encrypt_bytes(plaintext, self.secrets.encryption_key_bytes())
        chunk_items = list(chunk_bytes(encrypted["ciphertext"], self.config.github_chunk_size_bytes))
        estimated_upload_bytes = len(encrypted["ciphertext"]) + self._estimate_manifest_bytes(rel_path, len(chunk_items))
        target = self._allocate_upload_target(state, estimated_upload_bytes)
        client = self._client_for_account(target.account_id)
        LOGGER.info(
            "sync destino elegido path=%s cuenta=%s owner=%s repo=%s chunks=%s estimado=%sB",
            rel_path,
            target.account_id,
            target.owner,
            target.repository,
            len(chunk_items),
            estimated_upload_bytes,
        )
        client.ensure_branch_initialized(target.owner, target.repository, target.branch)

        version_id = utc_now_compact()
        remote_prefix = f"{self.config.github_uploads_prefix}/{file_id}/{version_id}"
        tree_entries: list[dict[str, Any]] = []
        chunks_payload: list[dict[str, Any]] = []
        uploaded_bytes = 0

        for chunk_index, chunk in chunk_items:
            LOGGER.info(
                "sync subiendo chunk path=%s chunk=%s/%s size=%sB repo=%s",
                rel_path,
                chunk_index + 1,
                len(chunk_items),
                len(chunk),
                target.repository,
            )
            chunk_sha = client.create_blob(target.owner, target.repository, chunk)
            self._sleep_after_upload()
            chunk_path = f"{remote_prefix}/chunk_{chunk_index:04d}.bin"
            tree_entries.append({"path": chunk_path, "mode": "100644", "type": "blob", "sha": chunk_sha})
            chunks_payload.append(
                {
                    "index": chunk_index,
                    "path": chunk_path,
                    "raw_url": client.raw_url(target.owner, target.repository, target.branch, chunk_path),
                    "sha256": sha256_bytes(chunk),
                    "size": len(chunk),
                    "repository": target.repository,
                    "repository_owner": target.owner,
                    "account_id": target.account_id,
                }
            )
            uploaded_bytes += len(chunk)

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
            "source_sha256": source_sha256,
            "repository_owner": target.owner,
            "repository": target.repository,
            "branch": target.branch,
            "account_id": target.account_id,
            "encryption": {
                "algorithm": encrypted["algorithm"],
                "nonce_b64": encrypted["nonce_b64"],
                "key_id": "state-default",
            },
            "chunks": chunks_payload,
        }

        manifest_bytes = json.dumps(version_manifest, ensure_ascii=False, indent=2).encode("utf-8")
        self._assert_target_capacity(state, target, uploaded_bytes + len(manifest_bytes))
        LOGGER.info("sync subiendo manifest path=%s size=%sB repo=%s", rel_path, len(manifest_bytes), target.repository)
        manifest_sha = client.create_blob(target.owner, target.repository, manifest_bytes)
        self._sleep_after_upload()
        manifest_path = f"{remote_prefix}/manifest.json"
        tree_entries.append({"path": manifest_path, "mode": "100644", "type": "blob", "sha": manifest_sha})
        uploaded_bytes += len(manifest_bytes)

        commit_sha = client.commit_tree(
            target.owner,
            target.repository,
            target.branch,
            tree_entries,
            f"github-fs sync {utc_now_iso()} ({rel_path})",
        )
        LOGGER.info("sync commit creado path=%s repo=%s commit=%s", rel_path, target.repository, commit_sha)

        self._record_uploaded_bytes(state, target.account_id, uploaded_bytes)
        self._bump_repository_size(state, target.account_id, target.repository, uploaded_bytes)
        return {
            "version_id": version_id,
            "created_at": version_manifest["created_at"],
            "plaintext_sha256": encrypted["plaintext_sha256"],
            "ciphertext_sha256": encrypted["ciphertext_sha256"],
            "size": size,
            "manifest_path": manifest_path,
            "manifest_raw_url": client.raw_url(target.owner, target.repository, target.branch, manifest_path),
            "encryption": version_manifest["encryption"],
            "chunks": chunks_payload,
            "commit_sha": commit_sha,
            "account_id": target.account_id,
            "repository_owner": target.owner,
            "repository": target.repository,
            "branch": target.branch,
            "uploaded_bytes": uploaded_bytes,
        }

    def _allocate_upload_target(self, state: dict[str, Any], estimated_upload_bytes: int) -> UploadTarget:
        eligible_accounts: list[GitHubAccountConfig] = []
        today = self._today_bucket()
        estimated_upload_kb = math.ceil(estimated_upload_bytes / 1024)

        for account in self.config.github_accounts:
            account_state = self._account_state(state, account.account_id, owner=account.owner)
            if account.account_id in self._runtime_unavailable_accounts:
                continue
            used_today = int(account_state["daily_uploads"].get(today, 0))
            if used_today + estimated_upload_bytes > self.config.github_account_daily_upload_limit_bytes:
                continue
            eligible_accounts.append(account)

        if not eligible_accounts:
            raise ServiceError("Ninguna cuenta GitHub tiene cuota diaria disponible para esta subida.")

        remaining_accounts = list(eligible_accounts)
        last_error: ServiceError | None = None
        while remaining_accounts:
            account = self._choose(remaining_accounts)
            remaining_accounts.remove(account)
            try:
                return self._allocate_upload_target_for_account(state, account, estimated_upload_kb)
            except ServiceError as exc:
                last_error = exc
                if account.account_id in self._runtime_unavailable_accounts:
                    LOGGER.warning(
                        "sync cuenta no disponible, probando otra cuenta cuenta=%s owner=%s motivo=%s",
                        account.account_id,
                        account.owner,
                        exc,
                    )
                    continue
                raise

        if last_error:
            raise last_error
        raise ServiceError("No se pudo seleccionar una cuenta GitHub para esta subida.")

    def _allocate_upload_target_for_account(
        self,
        state: dict[str, Any],
        account: GitHubAccountConfig,
        estimated_upload_kb: int,
    ) -> UploadTarget:
        account_state = self._account_state(state, account.account_id, owner=account.owner)

        if account.pinned_repository:
            repo_info = self._refresh_repository_info(state, account, account.pinned_repository)
            if repo_info.size_kb + estimated_upload_kb > self.config.github_repository_max_size_kb:
                raise ServiceError(f"El repositorio legado {account.owner}/{account.pinned_repository} excede el limite.")
            return UploadTarget(account.account_id, account.owner, account.pinned_repository, self.config.github_branch, True)

        repositories = self._refresh_managed_repositories(state, account)
        eligible_repositories = [
            repo
            for repo in repositories
            if repo.size_kb + estimated_upload_kb <= self.config.github_repository_max_size_kb
        ]
        if eligible_repositories:
            repo = self._choose(eligible_repositories)
            return UploadTarget(account.account_id, account.owner, repo.name, self.config.github_branch, repo.private)

        repo = self._create_next_repository(state, account)
        return UploadTarget(account.account_id, account.owner, repo.name, self.config.github_branch, repo.private)

    def _assert_target_capacity(self, state: dict[str, Any], target: UploadTarget, upload_bytes: int) -> None:
        account_state = self._account_state(state, target.account_id, owner=target.owner)
        today = self._today_bucket()
        used_today = int(account_state["daily_uploads"].get(today, 0))
        if used_today + upload_bytes > self.config.github_account_daily_upload_limit_bytes:
            raise ServiceError(f"La cuenta {target.account_id} ha superado su cuota diaria.")

        repo_state = account_state["repositories"].get(target.repository)
        current_size_kb = int(repo_state.get("last_known_size_kb", 0)) if repo_state else 0
        if current_size_kb + math.ceil(upload_bytes / 1024) > self.config.github_repository_max_size_kb:
            raise ServiceError(f"El repositorio {target.owner}/{target.repository} supera el limite configurado.")

    def _refresh_managed_repositories(self, state: dict[str, Any], account: GitHubAccountConfig) -> list[RepositoryInfo]:
        client = self._client_for_account(account.account_id)
        repositories = client.list_managed_repositories(account.owner, self.config.github_repository_prefix)
        account_state = self._account_state(state, account.account_id, owner=account.owner)
        known = account_state["repositories"]
        seen = set()
        for repo in repositories:
            seen.add(repo.name)
            repo_state = known.setdefault(repo.name, {})
            repo_state["name"] = repo.name
            repo_state["owner"] = repo.owner
            repo_state["last_known_size_kb"] = repo.size_kb
            repo_state["private"] = repo.private
            repo_state["last_refreshed_at"] = utc_now_iso()
        for repo_name in list(known):
            if repo_name not in seen and repo_name.startswith(self.config.github_repository_prefix):
                known[repo_name].setdefault("name", repo_name)
        account_state["last_metadata_refresh_at"] = utc_now_iso()
        return repositories

    def _refresh_repository_info(self, state: dict[str, Any], account: GitHubAccountConfig, repository: str) -> RepositoryInfo:
        client = self._client_for_account(account.account_id)
        info = client.get_repository(account.owner, repository)
        repo_state = self._account_state(state, account.account_id, owner=account.owner)["repositories"].setdefault(repository, {})
        repo_state["name"] = repository
        repo_state["owner"] = account.owner
        repo_state["last_known_size_kb"] = info.size_kb
        repo_state["private"] = info.private
        repo_state["last_refreshed_at"] = utc_now_iso()
        return info

    def _create_next_repository(self, state: dict[str, Any], account: GitHubAccountConfig) -> RepositoryInfo:
        account_state = self._account_state(state, account.account_id, owner=account.owner)
        existing_names = set(account_state["repositories"].keys())
        next_index = 1
        while True:
            candidate = f"{self.config.github_repository_prefix}-{next_index:04d}"
            if candidate not in existing_names:
                break
            next_index += 1
        client = self._client_for_account(account.account_id)
        LOGGER.info("sync creando repositorio nuevo cuenta=%s owner=%s repo=%s", account.account_id, account.owner, candidate)
        try:
            info = client.create_repository(account.owner, candidate, self.config.github_repository_private)
        except GitHubError as exc:
            message = str(exc)
            if "HTTP 403" in message and "Resource not accessible by personal access token" in message:
                alert_message = (
                    f"El token GitHub de la cuenta {account.account_id} no puede crear repositorios en {account.owner}. "
                    "Usa un token con permiso para administrarlos o configura un repositorio fijo existente."
                )
                self._mark_account_unavailable(
                    state,
                    account,
                    code="repository_creation_forbidden",
                    message=alert_message,
                )
                raise ServiceError(alert_message) from exc
            raise ServiceError(
                f"No se pudo crear el repositorio {account.owner}/{candidate} para la cuenta {account.account_id}: {message}"
            ) from exc
        repo_state = account_state["repositories"].setdefault(candidate, {})
        repo_state["name"] = candidate
        repo_state["owner"] = account.owner
        repo_state["last_known_size_kb"] = info.size_kb
        repo_state["private"] = info.private
        repo_state["last_refreshed_at"] = utc_now_iso()
        return info

    def _mark_account_unavailable(
        self,
        state: dict[str, Any],
        account: GitHubAccountConfig,
        *,
        code: str,
        message: str,
    ) -> None:
        account_state = self._account_state(state, account.account_id, owner=account.owner)
        detected_at = utc_now_iso()
        alerts = account_state.setdefault("alerts", [])
        existing = next((item for item in alerts if item.get("code") == code), None)
        payload = {
            "code": code,
            "message": message,
            "detected_at": detected_at,
            "level": "error",
            "needs_user_action": True,
        }
        if existing:
            existing.update(payload)
        else:
            alerts.append(payload)
        account_state["available"] = False
        account_state["unavailable_reason"] = message
        account_state["unavailable_since"] = detected_at
        self._runtime_unavailable_accounts.add(account.account_id)

    def _record_uploaded_bytes(self, state: dict[str, Any], account_id: str, uploaded_bytes: int) -> None:
        account_state = self._account_state(state, account_id)
        today = self._today_bucket()
        account_state["daily_uploads"][today] = int(account_state["daily_uploads"].get(today, 0)) + uploaded_bytes
        account_state["last_upload_at"] = utc_now_iso()

    def _bump_repository_size(self, state: dict[str, Any], account_id: str, repository: str, uploaded_bytes: int) -> None:
        account_state = self._account_state(state, account_id)
        repo_state = account_state["repositories"].setdefault(repository, {})
        repo_state["last_known_size_kb"] = int(repo_state.get("last_known_size_kb", 0)) + math.ceil(uploaded_bytes / 1024)
        repo_state["last_refreshed_at"] = utc_now_iso()

    def _estimate_manifest_bytes(self, rel_path: str, chunk_count: int) -> int:
        return 2048 + len(rel_path.encode("utf-8")) + chunk_count * 512

    def _sleep_after_upload(self) -> None:
        if self.config.github_upload_sleep_max_seconds <= 0:
            return
        duration = self._sleep_sampler(
            self.config.github_upload_sleep_min_seconds,
            self.config.github_upload_sleep_max_seconds,
        )
        if duration > 0:
            self._sleeper(duration)

    def _client_for_account(self, account_id: str) -> GitHubClient:
        client = self.github_clients.get(account_id)
        if not client:
            raise ServiceError(f"No existe cliente GitHub para la cuenta {account_id}.")
        return client

    def _resolve_version_account_id(self, version: dict[str, Any]) -> str:
        account_id = version.get("account_id")
        if account_id:
            return account_id
        if "legacy" in self.account_by_id:
            return "legacy"
        raise ServiceError("La version no indica account_id y no hay configuracion legacy disponible.")

    def _ensure_github_accounts_state(self, state: dict[str, Any]) -> None:
        github_accounts = state.setdefault("github_accounts", {})
        for account in self.config.github_accounts:
            github_accounts.setdefault(
                account.account_id,
                {
                    "account_id": account.account_id,
                    "owner": account.owner,
                    "repositories": {},
                    "daily_uploads": {},
                    "last_metadata_refresh_at": None,
                    "last_upload_at": None,
                    "available": True,
                    "unavailable_reason": None,
                    "unavailable_since": None,
                    "alerts": [],
                },
            )

    def _account_state(self, state: dict[str, Any], account_id: str, owner: str | None = None) -> dict[str, Any]:
        github_accounts = state.setdefault("github_accounts", {})
        payload = github_accounts.setdefault(
            account_id,
            {
                "account_id": account_id,
                "owner": owner or self.account_by_id.get(account_id, GitHubAccountConfig(account_id, "", "")).owner,
                "repositories": {},
                "daily_uploads": {},
                "last_metadata_refresh_at": None,
                "last_upload_at": None,
                "available": True,
                "unavailable_reason": None,
                "unavailable_since": None,
                "alerts": [],
            },
        )
        if owner:
            payload["owner"] = owner
        payload.setdefault("repositories", {})
        payload.setdefault("daily_uploads", {})
        payload.setdefault("available", True)
        payload.setdefault("unavailable_reason", None)
        payload.setdefault("unavailable_since", None)
        payload.setdefault("alerts", [])
        return payload

    def _augment_state_for_web(self, state: dict[str, Any]) -> None:
        self._ensure_github_accounts_state(state)
        today = self._today_bucket()
        summaries = []
        for account in self.config.github_accounts:
            account_state = self._account_state(state, account.account_id, owner=account.owner)
            repositories = sorted(account_state["repositories"].values(), key=lambda item: item.get("name", ""))
            summaries.append(
                {
                    "account_id": account.account_id,
                    "owner": account.owner,
                    "uploaded_today_bytes": int(account_state["daily_uploads"].get(today, 0)),
                    "daily_limit_bytes": self.config.github_account_daily_upload_limit_bytes,
                    "repositories": repositories,
                    "available": bool(account_state.get("available", True)) and account.account_id not in self._runtime_unavailable_accounts,
                    "unavailable_reason": account_state.get("unavailable_reason"),
                    "unavailable_since": account_state.get("unavailable_since"),
                    "alerts": list(account_state.get("alerts", [])),
                }
            )
        state["github_account_summaries"] = summaries
        state["github_account_alerts"] = [
            {
                "account_id": summary["account_id"],
                "owner": summary["owner"],
                "available": summary["available"],
                "unavailable_reason": summary["unavailable_reason"],
                "unavailable_since": summary["unavailable_since"],
                "alerts": summary["alerts"],
            }
            for summary in summaries
            if summary["alerts"] or not summary["available"]
        ]

    @staticmethod
    def _today_bucket() -> str:
        return utc_now_iso().split("T", 1)[0]

    @staticmethod
    def _get_active_version(entry: dict[str, Any] | None) -> dict[str, Any] | None:
        if not entry:
            return None
        active_version_id = entry.get("active_version_id")
        if not active_version_id:
            return None
        return next((version for version in entry.get("versions", []) if version["version_id"] == active_version_id), None)

    def mark_manual_trigger(self, task_name: str) -> None:
        state = self.state_manager.load(self.default_config)
        state["tasks"][task_name]["last_manual_trigger_at"] = utc_now_iso()
        self.state_manager.save(state)
