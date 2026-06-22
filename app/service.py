from __future__ import annotations

import fcntl
import base64
import json
import logging
import math
import random
import threading
import time
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

try:
    import pysqlite3 as sqlite3
except ImportError:  # pragma: no cover - fallback for local environments
    import sqlite3

from .config import AppConfig, GitHubAccountConfig, RuntimeSecrets
from .crypto import StreamingAESGCMDecryptor, chunk_bytes, encrypt_bytes, encrypt_bytes_with_nonce
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


LOGGER = logging.getLogger("spider-back")


class ServiceError(Exception):
    pass


class NoAvailableAccountsError(ServiceError):
    """Raised when no GitHub accounts have daily upload quota available."""


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
            "github_accounts": [
                {"account_id": account.account_id, "owner": account.owner, "network": "github"}
                for account in config.github_accounts
            ],
            "branch": config.github_branch,
            "uploads_prefix": config.github_uploads_prefix,
            "repository_prefix": config.github_repository_prefix,
            "repository_private": config.github_repository_private,
            "repository_max_size_kb": config.github_repository_max_size_kb,
            "daily_upload_limit_gb": config.github_account_daily_upload_limit_gb,
            "copy_count": config.copy_count,
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
        self._task_locks = {"sync": threading.Lock(), "verify": threading.Lock()}
        self._task_lock_paths = {
            "sync": self.config.app_state_dir / "sync.lock",
            "verify": self.config.app_state_dir / "verify.lock",
        }
        self._upload_index_path = self.config.app_state_dir / "upload_index.sqlite3"
        self._runtime_unavailable_accounts: set[str] = set()
        self._choose = chooser or random.choice
        self._sleep_sampler = sleep_sampler or random.uniform
        self._sleeper = sleeper or time.sleep
        # live in-memory state while a task is running (used by web UI to reflect progress)
        self._live_state: dict[str, Any] | None = None
        self._live_state_lock = threading.RLock()
        # cada cuantos archivos volcar el estado a disco durante una sync
        self._state_flush_every = 10

    def get_state(self) -> dict[str, Any]:
        # If a task is running, prefer the live in-memory state so the web UI
        # can show progress (new files/versions) before they're persisted.
        with self._live_state_lock:
            live = deepcopy(self._live_state) if self._live_state is not None else None

        if live is not None:
            state = live
        else:
            state = self.state_manager.snapshot(self.default_config)

        self._refresh_task_running_flags(state)
        self._augment_state_for_web(state)
        return state

    def run_sync(self) -> TaskResult:
        return self._run_task("sync", lambda state: self._sync_impl(state, full=False))

    def run_full_sync(self) -> TaskResult:
        return self._run_task("sync", lambda state: self._sync_impl(state, full=True))

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

                # expose the in-memory state for the web UI while the task runs
                with self._live_state_lock:
                    self._live_state = state

                try:
                    result = callback(state)

                    state["tasks"][task_name]["running"] = False
                    state["tasks"][task_name]["last_finished_at"] = utc_now_iso()
                    state["tasks"][task_name]["last_result"] = "success" if result.ok else "error"
                    state["tasks"][task_name]["last_error"] = result.error
                    state["tasks"][task_name]["last_summary"] = result.summary
                    self.state_manager.save(state)
                    LOGGER.info("%s finalizado con resultado=%s resumen=%s", task_name, "success" if result.ok else "error", result.summary)
                    return result
                finally:
                    with self._live_state_lock:
                        self._live_state = None
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
        for task_name in ("sync", "verify"):
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
        
        scanned_files = 0
        unchanged_files = 0
        synced_or_reused_files = 0
        uploaded_files = 0
        failed_files = 0
        uploaded_bytes = 0
        last_scan_log_at = time.monotonic()
        processed_since_flush = 0

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
            
            # 1. Sin cambios
            if self._is_file_already_synced(entry, active_version, size, mtime_ns, full=full):
                entry["size"] = size
                entry["mtime_ns"] = mtime_ns
                entry["present"] = True
                entry["last_seen_at"] = utc_now_iso()
                unchanged_files += 1
                self._log_scan_progress(scanned_files, failed_files, unchanged_files + synced_or_reused_files, rel_path, last_scan_log_at)
                continue

            source_sha256 = sha256_file(file_path)
            
            # 2. Mismo hash, pero replicación incompleta (reanudar)
            if active_version and active_version.get("plaintext_sha256") == source_sha256 and entry.get("present"):
                entry.update({"size": size, "mtime_ns": mtime_ns, "source_sha256": source_sha256, "present": True, "last_seen_at": utc_now_iso()})
                
                if active_version.get("replication_complete", True):
                    unchanged_files += 1
                    self._log_scan_progress(scanned_files, failed_files, unchanged_files + synced_or_reused_files, rel_path, last_scan_log_at)
                    continue

                # Subida inmediata
                uploaded_files, failed_files, uploaded_bytes, processed_since_flush = self._process_upload(
                    state, files_state, file_id, file_path, rel_path, size, mtime_ns, source_sha256, active_version,
                    uploaded_files, failed_files, uploaded_bytes, processed_since_flush
                )
                self._log_scan_progress(scanned_files, failed_files, unchanged_files + synced_or_reused_files, rel_path, last_scan_log_at)
                continue

            # 3. Hash ya existe en otro lado (reutilizar)
            existing_version = self._lookup_uploaded_version(source_sha256)
            if existing_version is not None:
                existing_version = self._normalize_version(existing_version)
                if not existing_version.get("replication_complete", True):
                    # Subida inmediata para completar
                    uploaded_files, failed_files, uploaded_bytes, processed_since_flush = self._process_upload(
                        state, files_state, file_id, file_path, rel_path, size, mtime_ns, source_sha256, existing_version,
                        uploaded_files, failed_files, uploaded_bytes, processed_since_flush
                    )
                    self._log_scan_progress(scanned_files, failed_files, unchanged_files + synced_or_reused_files, rel_path, last_scan_log_at)
                    continue
                    
                entry = files_state.setdefault(file_id, {"file_id": file_id, "path": rel_path, "versions": [], "active_version_id": None, "last_verification": None, "last_error": None})
                self._apply_existing_version_entry(entry, rel_path=rel_path, size=size, mtime_ns=mtime_ns, source_sha256=source_sha256, version=existing_version)
                if existing_version.get("replication_complete", True):
                    self._mark_uploaded_copy_seen(source_sha256)
                synced_or_reused_files += 1
                self._log_scan_progress(scanned_files, failed_files, unchanged_files + synced_or_reused_files, rel_path, last_scan_log_at)
                continue

            # 4. Archivo completamente nuevo (Subida inmediata)
            uploaded_files, failed_files, uploaded_bytes, processed_since_flush = self._process_upload(
                state, files_state, file_id, file_path, rel_path, size, mtime_ns, source_sha256, None,
                uploaded_files, failed_files, uploaded_bytes, processed_since_flush
            )
            self._log_scan_progress(scanned_files, failed_files, unchanged_files + synced_or_reused_files, rel_path, last_scan_log_at)

        # Esto se ejecuta solo si el escaneo termina con éxito
        LOGGER.info("sync analizado: %s archivos detectados, %s subidos/reutilizados, %s sin cambios.", len(discovered_paths), uploaded_files + synced_or_reused_files, unchanged_files)
        
        # Marcado de archivos eliminados (ahora es seguro porque terminamos de escanear)
        for entry in files_state.values():
            if entry["path"] not in discovered_paths:
                entry["present"] = False
                entry["last_seen_at"] = utc_now_iso()

        self.state_manager.save(state) # Guardado final
        summary = {
            "scanned_files": len(discovered_paths),
            "uploaded_files": uploaded_files,
            "reused_files": synced_or_reused_files,
            "failed_files": failed_files,
            "uploaded_bytes": uploaded_bytes,
            "missing_files": sum(1 for entry in files_state.values() if not entry.get("present")),
        }
        return TaskResult(failed_files == 0, summary, None if failed_files == 0 else f"{failed_files} archivos con error")
    
    def _log_scan_progress(self, scanned, failed, ok, path, last_log_time):
        if scanned == 1 or scanned % 100 == 0 or time.monotonic() - last_log_time >= 10:
            LOGGER.info("sync progreso: revisados=%s errores=%s al_dia=%s ultimo=%s", scanned, failed, ok, path)
            last_log_time = time.monotonic()
        return last_log_time

    def _process_upload(self, state, files_state, file_id, file_path, rel_path, size, mtime_ns, source_sha256, resume_version, up_files, fail_files, up_bytes, flush_count):
        entry = files_state.setdefault(file_id, {"file_id": file_id, "path": rel_path, "versions": [], "active_version_id": None, "last_verification": None, "last_error": None})
        try:
            LOGGER.info("sync %sarchivo path=%s size=%sB", "completando " if resume_version else "subiendo ", rel_path, size)
            version = self._upload_file_version(state, file_id, file_path, rel_path, size, mtime_ns, source_sha256, resume_version=resume_version)
            
            entry.update({"path": rel_path, "present": True, "size": size, "mtime_ns": mtime_ns, "source_sha256": source_sha256, "last_seen_at": utc_now_iso()})
            existing_versions = entry.setdefault("versions", [])
            existing_index = next((idx for idx, item in enumerate(existing_versions) if item.get("version_id") == version["version_id"]), None)
            if existing_index is None:
                existing_versions.append(version)
            else:
                existing_versions[existing_index] = version
            entry["active_version_id"] = version["version_id"]
            entry["last_verification"] = None
            
            if version.get("replication_complete", True):
                entry["last_error"] = None
                up_files += 1
                self._record_uploaded_version(source_sha256, version)
            else:
                entry["last_error"] = version.get("copy_errors", [{}])[-1].get("error") if version.get("copy_errors") else "La version no se pudo replicar completamente."
                fail_files += 1
                
            up_bytes += version["uploaded_bytes"]
            LOGGER.info("sync archivo %s path=%s cuenta=%s repo=%s version=%s bytes=%s", "replicado" if version.get("replication_complete") else "parcial", rel_path, version["account_id"], version["repository"], version["version_id"], version["uploaded_bytes"])

        except NoAvailableAccountsError as exc:
            entry["last_error"] = str(exc)
            self.state_manager.save(state)
            raise  # Relanzamos para que el bucle principal lo capture y devuelva el TaskResult(False)
        except Exception as exc:
            entry.update({"path": rel_path, "present": True, "size": size, "mtime_ns": mtime_ns, "source_sha256": source_sha256, "last_seen_at": utc_now_iso(), "last_error": str(exc)})
            fail_files += 1
            LOGGER.exception("sync error subiendo path=%s", rel_path)

        flush_count += 1
        if flush_count >= self._state_flush_every:
            self.state_manager.save(state)
            flush_count = 0

        return up_files, fail_files, up_bytes, flush_count
    
    def _verify_impl(self, state: dict[str, Any]) -> TaskResult:
        self._ensure_github_accounts_state(state)
        files_state = state["files"]
        verified = 0
        failures = 0
        copies_verified = 0
        copies_failed = 0
        copies_skipped = 0

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
            version = self._normalize_version(active_version)
            # Verify EVERY copy, not just the primary one: replication only buys
            # durability if each copy is independently checked. Each copy is
            # dispatched by its own network so the check works for GitHub today
            # and additional backends (e.g. Telegram) as their clients are wired.
            copy_results: list[dict[str, Any]] = []
            for copy in version.get("copies", []):
                try:
                    detail = self._verify_copy(entry["path"], copy, local_sha)
                    copy_results.append(detail)
                    if detail.get("skipped"):
                        copies_skipped += 1
                    else:
                        copies_verified += 1
                except Exception as exc:
                    copy_results.append(
                        {
                            "ok": False,
                            "copy_index": copy.get("copy_index"),
                            "network": copy.get("network", "github"),
                            "account_id": copy.get("account_id"),
                            "repository": copy.get("repository"),
                            "error": str(exc),
                        }
                    )
                    copies_failed += 1

            ok_copies = [detail for detail in copy_results if detail.get("ok")]
            failed_copies = [detail for detail in copy_results if not detail.get("ok") and not detail.get("skipped")]
            # A file is verified only if no copy failed AND at least one copy was
            # actually checked (a version we cannot verify at all is not "ok").
            file_ok = not failed_copies and bool(ok_copies)
            primary = ok_copies[0] if ok_copies else (copy_results[0] if copy_results else {})

            entry["last_verification"] = {
                "checked_at": utc_now_iso(),
                "ok": file_ok,
                "local_sha256": local_sha,
                "remote_sha256": primary.get("remote_sha256") if file_ok else None,
                "version_id": version["version_id"],
                "account_id": primary.get("account_id"),
                "network": primary.get("network", "github"),
                "repository": primary.get("repository"),
                "chunks_checked": primary.get("chunks_checked"),
                "copies_total": len(copy_results),
                "copies_verified": len(ok_copies),
                "copies_failed": len(failed_copies),
                "copies": copy_results,
            }
            if file_ok:
                entry["last_error"] = None
                verified += 1
            else:
                entry["last_error"] = (
                    failed_copies[0].get("error")
                    if failed_copies
                    else "No se pudo verificar ninguna copia de la version."
                )
                failures += 1

        self.state_manager.save(state)
        return TaskResult(
            failures == 0,
            {
                "verified_files": verified,
                "failed_files": failures,
                "present_files": sum(1 for entry in files_state.values() if entry.get("present")),
                "copies_verified": copies_verified,
                "copies_failed": copies_failed,
                "copies_skipped": copies_skipped,
            },
            None if failures == 0 else f"{failures} archivos con error",
        )

    def _verify_copy(self, rel_path: str, copy: dict[str, Any], local_sha: str) -> dict[str, Any]:
        """Verify a single copy of a version against the local file hash.

        Returns a detail dict. A copy on a network without a configured client
        is reported as ``skipped`` (not failed) so GitHub-only deployments keep
        passing while leaving a clear record that the copy was not checked.
        """
        network = copy.get("network", "github")
        copy_index = copy.get("copy_index")
        if network != "github":
            return {
                "ok": False,
                "skipped": True,
                "copy_index": copy_index,
                "network": network,
                "account_id": copy.get("account_id"),
                "repository": copy.get("repository"),
                "error": f"Verificacion no implementada para la red '{network}'.",
            }

        account_id = copy.get("account_id")
        client = self._client_for_account(account_id)
        decryptor = StreamingAESGCMDecryptor(
            self.secrets.encryption_key_bytes(),
            copy["encryption"]["nonce_b64"],
        )
        downloaded_chunks = 0
        for chunk in sorted(copy.get("chunks", []), key=lambda item: item["index"]):
            data = client.fetch_bytes(chunk["raw_url"])
            if sha256_bytes(data) != chunk["sha256"]:
                raise ServiceError(f"Chunk corrupto: {rel_path}#{chunk['index']} (copia {copy_index})")
            decryptor.update(data)
            downloaded_chunks += 1
        _, remote_sha = decryptor.finalize()
        if remote_sha != local_sha:
            raise ServiceError(
                f"Hash distinto para {rel_path} (copia {copy_index}): local={local_sha} remoto={remote_sha}"
            )
        return {
            "ok": True,
            "copy_index": copy_index,
            "network": network,
            "account_id": account_id,
            "repository": copy.get("repository"),
            "remote_sha256": remote_sha,
            "chunks_checked": downloaded_chunks,
        }

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
        if not active_version.get("replication_complete", True):
            return False
        return True

    def _apply_existing_version_entry(
        self,
        entry: dict[str, Any],
        *,
        rel_path: str,
        size: int,
        mtime_ns: int,
        source_sha256: str,
        version: dict[str, Any],
    ) -> None:
        version = self._normalize_version(version)
        entry["path"] = rel_path
        entry["present"] = True
        entry["size"] = size
        entry["mtime_ns"] = mtime_ns
        entry["source_sha256"] = source_sha256
        entry["last_seen_at"] = utc_now_iso()
        entry.setdefault("versions", [])
        if not any(item.get("version_id") == version.get("version_id") for item in entry["versions"]):
            entry["versions"].append(version)
        entry["active_version_id"] = version["version_id"]
        entry["last_verification"] = None
        entry["last_error"] = None

    def _is_file_already_synced(
        self,
        entry: dict[str, Any] | None,
        active_version: dict[str, Any] | None,
        size: int,
        mtime_ns: int,
        *,
        full: bool,
    ) -> bool:
        # A light (non-full) sync TRUSTS the persisted state: if we already hold a
        # present, fully-replicated version for this file we skip it without
        # re-hashing — and without re-uploading even if size/mtime changed on
        # disk. Modifications are intentionally only picked up by a full sync,
        # which forces a re-hash (returns False below). This keeps incremental
        # syncs cheap and is the behaviour asserted by
        # test_sync_trusts_persisted_version_until_full_sync.
        if full:
            return False
        if not self._can_trust_persisted_file_state(entry, active_version):
            return False
        # Only "synced" once the requested number of copies (COPY_COUNT) has been
        # made. An under-replicated version must fall through so sync can add the
        # remaining copies on the next pass.
        if not self._is_version_replication_complete(active_version):
            return False
        return True

    @staticmethod
    def distinct_account_copy_count(version: dict[str, Any] | None) -> int:
        """Number of distinct accounts holding a copy of this version.

        Two copies under the same account count as ONE — replication only buys
        durability when copies live on different accounts/networks.
        """
        if not version:
            return 0
        seen: set[tuple[str, str]] = set()
        for copy in version.get("copies", []):
            account_id = copy.get("account_id")
            if not account_id:
                continue
            seen.add((copy.get("network", "github"), account_id))
        return len(seen)

    def _is_version_replication_complete(self, version: dict[str, Any] | None) -> bool:
        if not version:
            return False
        requested = int(version.get("copy_count_requested", self.config.copy_count))
        return self.distinct_account_copy_count(version) >= requested

    @contextmanager
    def _upload_index_connection(self):
        self._upload_index_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._upload_index_path, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uploaded_versions (
                    source_sha256 TEXT PRIMARY KEY,
                    version_json TEXT NOT NULL,
                    first_uploaded_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    copy_count INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _lookup_uploaded_version(self, source_sha256: str) -> dict[str, Any] | None:
        with self._upload_index_connection() as conn:
            row = conn.execute(
                "SELECT version_json FROM uploaded_versions WHERE source_sha256 = ?",
                (source_sha256,),
            ).fetchone()
        if not row:
            return None
        return self._normalize_version(json.loads(row[0]))

    def _record_uploaded_version(self, source_sha256: str, version: dict[str, Any]) -> None:
        payload = json.dumps(version, ensure_ascii=False)
        now = utc_now_iso()
        with self._upload_index_connection() as conn:
            conn.execute(
                """
                INSERT INTO uploaded_versions (
                    source_sha256, version_json, first_uploaded_at, last_seen_at, copy_count
                ) VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(source_sha256) DO UPDATE SET
                    version_json = excluded.version_json,
                    last_seen_at = excluded.last_seen_at
                """,
                (source_sha256, payload, now, now),
            )

    def _mark_uploaded_copy_seen(self, source_sha256: str) -> None:
        now = utc_now_iso()
        with self._upload_index_connection() as conn:
            conn.execute(
                """
                UPDATE uploaded_versions
                SET last_seen_at = ?, copy_count = copy_count + 1
                WHERE source_sha256 = ?
                """,
                (now, source_sha256),
            )

    @staticmethod
    def _version_copies(version: dict[str, Any]) -> list[dict[str, Any]]:
        copies = version.get("copies")
        if copies:
            return [deepcopy(copy) for copy in copies]

        legacy_copy = {
            "copy_index": 1,
            "network": version.get("network", "github"),
            "version_id": version.get("version_id"),
            "created_at": version.get("created_at"),
            "file_id": version.get("file_id"),
            "path": version.get("path"),
            "plaintext_sha256": version.get("plaintext_sha256"),
            "ciphertext_sha256": version.get("ciphertext_sha256"),
            "size": version.get("size"),
            "mtime_ns": version.get("mtime_ns"),
            "source_sha256": version.get("source_sha256"),
            "repository_owner": version.get("repository_owner"),
            "repository": version.get("repository"),
            "branch": version.get("branch"),
            "account_id": version.get("account_id"),
            "encryption": deepcopy(version.get("encryption", {})),
            "chunks": deepcopy(version.get("chunks", [])),
            "manifest_path": version.get("manifest_path"),
            "manifest_raw_url": version.get("manifest_raw_url"),
            "commit_sha": version.get("commit_sha"),
            "uploaded_bytes": version.get("uploaded_bytes", 0),
        }
        return [legacy_copy]

    @classmethod
    def _normalize_version(cls, version: dict[str, Any]) -> dict[str, Any]:
        normalized = deepcopy(version)
        copies = cls._version_copies(normalized)
        for index, copy in enumerate(copies, start=1):
            copy.setdefault("copy_index", index)
            copy.setdefault("network", copy.get("network", normalized.get("network", "github")))
            copy.setdefault("version_id", normalized.get("version_id"))
            copy.setdefault("created_at", normalized.get("created_at"))
            copy.setdefault("file_id", normalized.get("file_id"))
            copy.setdefault("path", normalized.get("path"))
            copy.setdefault("plaintext_sha256", normalized.get("plaintext_sha256"))
            copy.setdefault("ciphertext_sha256", normalized.get("ciphertext_sha256"))
            copy.setdefault("size", normalized.get("size"))
            copy.setdefault("mtime_ns", normalized.get("mtime_ns"))
            copy.setdefault("source_sha256", normalized.get("source_sha256"))
            copy.setdefault("repository_owner", normalized.get("repository_owner"))
            copy.setdefault("repository", normalized.get("repository"))
            copy.setdefault("branch", normalized.get("branch"))
            copy.setdefault("account_id", normalized.get("account_id"))
            copy.setdefault("encryption", deepcopy(copy.get("encryption", normalized.get("encryption", {}))))
            copy.setdefault("chunks", deepcopy(copy.get("chunks", normalized.get("chunks", []))))
            copy.setdefault("manifest_path", normalized.get("manifest_path"))
            copy.setdefault("manifest_raw_url", normalized.get("manifest_raw_url"))
            copy.setdefault("commit_sha", normalized.get("commit_sha"))
            copy.setdefault("uploaded_bytes", int(copy.get("uploaded_bytes", 0)))
        normalized["copies"] = copies
        primary = copies[0]
        normalized.setdefault("network", primary.get("network", "github"))
        normalized.setdefault("account_id", primary.get("account_id"))
        normalized.setdefault("repository_owner", primary.get("repository_owner"))
        normalized.setdefault("repository", primary.get("repository"))
        normalized.setdefault("branch", primary.get("branch"))
        normalized.setdefault("manifest_path", primary.get("manifest_path"))
        normalized.setdefault("manifest_raw_url", primary.get("manifest_raw_url"))
        normalized.setdefault("commit_sha", primary.get("commit_sha"))
        normalized.setdefault("encryption", deepcopy(primary.get("encryption", {})))
        normalized.setdefault("chunks", deepcopy(primary.get("chunks", [])))
        normalized["copy_count_requested"] = int(normalized.get("copy_count_requested", len(copies)))
        normalized["copy_count_completed"] = int(normalized.get("copy_count_completed", len(copies)))
        normalized["replication_complete"] = bool(
            normalized.get("replication_complete", normalized["copy_count_completed"] >= normalized["copy_count_requested"])
        )
        normalized.setdefault("copy_errors", [])
        normalized["uploaded_bytes"] = int(normalized.get("uploaded_bytes", sum(int(copy.get("uploaded_bytes", 0)) for copy in copies)))
        return normalized

    def _build_copy_manifest(
        self,
        *,
        file_id: str,
        rel_path: str,
        version_id: str,
        version_created_at: str,
        encrypted: dict[str, Any],
        size: int,
        mtime_ns: int,
        source_sha256: str,
        target: UploadTarget,
        chunk_items: list[tuple[int, bytes]],
        copy_index: int,
        copy_count: int,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
        client = self._client_for_account(target.account_id)
        remote_prefix = f"{self.config.github_uploads_prefix}/{file_id}/{version_id}"
        tree_entries: list[dict[str, Any]] = []
        chunks_payload: list[dict[str, Any]] = []
        uploaded_bytes = 0

        for chunk_position, (chunk_index, chunk) in enumerate(chunk_items, start=1):
            LOGGER.info(
                "sync subiendo chunk path=%s copia=%s/%s chunk=%s/%s size=%sB repo=%s",
                rel_path,
                copy_index,
                copy_count,
                chunk_position,
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
                    "network": "github",
                }
            )
            uploaded_bytes += len(chunk)

        manifest_payload: dict[str, Any] = {
            "version": 1,
            "file_id": file_id,
            "path": rel_path,
            "version_id": version_id,
            "created_at": version_created_at,
            "plaintext_sha256": encrypted["plaintext_sha256"],
            "ciphertext_sha256": encrypted["ciphertext_sha256"],
            "size": size,
            "mtime_ns": mtime_ns,
            "source_sha256": source_sha256,
            "repository_owner": target.owner,
            "repository": target.repository,
            "branch": target.branch,
            "account_id": target.account_id,
            "network": "github",
            "copy_index": copy_index,
            "copy_count_requested": copy_count,
            "encryption": {
                "algorithm": encrypted["algorithm"],
                "nonce_b64": encrypted["nonce_b64"],
                "key_id": "state-default",
            },
            "chunks": chunks_payload,
        }
        return manifest_payload, tree_entries, uploaded_bytes

    def _upload_version_copy(
        self,
        state: dict[str, Any],
        *,
        file_id: str,
        rel_path: str,
        size: int,
        mtime_ns: int,
        source_sha256: str,
        version_id: str,
        version_created_at: str,
        encrypted: dict[str, Any],
        chunk_items: list[tuple[int, bytes]],
        target: UploadTarget,
        copy_index: int,
        copy_count: int,
    ) -> dict[str, Any]:
        client = self._client_for_account(target.account_id)
        LOGGER.info(
            "sync destino elegido path=%s copia=%s/%s cuenta=%s owner=%s repo=%s chunks=%s",
            rel_path,
            copy_index,
            copy_count,
            target.account_id,
            target.owner,
            target.repository,
            len(chunk_items),
        )
        try:
            client.ensure_branch_initialized(target.owner, target.repository, target.branch)
            manifest_payload, tree_entries, uploaded_bytes = self._build_copy_manifest(
                file_id=file_id,
                rel_path=rel_path,
                version_id=version_id,
                version_created_at=version_created_at,
                encrypted=encrypted,
                size=size,
                mtime_ns=mtime_ns,
                source_sha256=source_sha256,
                target=target,
                chunk_items=chunk_items,
                copy_index=copy_index,
                copy_count=copy_count,
            )
            self._assert_target_capacity(state, target, uploaded_bytes + self._estimate_manifest_bytes(rel_path, len(chunk_items)))
            manifest_bytes = json.dumps(manifest_payload, ensure_ascii=False, indent=2).encode("utf-8")
            LOGGER.info(
                "sync subiendo manifest path=%s copia=%s/%s size=%sB repo=%s",
                rel_path,
                copy_index,
                copy_count,
                len(manifest_bytes),
                target.repository,
            )
            manifest_sha = client.create_blob(target.owner, target.repository, manifest_bytes)
            self._sleep_after_upload()
            remote_prefix = f"{self.config.github_uploads_prefix}/{file_id}/{version_id}"
            manifest_path = f"{remote_prefix}/manifest.json"
            tree_entries.append({"path": manifest_path, "mode": "100644", "type": "blob", "sha": manifest_sha})
            uploaded_bytes += len(manifest_bytes)
            commit_sha = client.commit_tree(
                target.owner,
                target.repository,
                target.branch,
                tree_entries,
                f"spider-back sync {utc_now_iso()} ({rel_path})",
            )
            LOGGER.info(
                "sync commit creado path=%s copia=%s/%s repo=%s commit=%s",
                rel_path,
                copy_index,
                copy_count,
                target.repository,
                commit_sha,
            )
        except GitHubError as exc:
            if self._handle_account_access_error(state, self.account_by_id[target.account_id], exc):
                raise ServiceError(
                    f"La cuenta {target.account_id} ha sido retirada del pool activo por permisos insuficientes."
                ) from exc
            raise

        self._record_uploaded_bytes(state, target.account_id, uploaded_bytes)
        self._bump_repository_size(state, target.account_id, target.repository, uploaded_bytes)
        return {
            "copy_index": copy_index,
            "network": "github",
            "version_id": version_id,
            "created_at": manifest_payload["created_at"],
            "plaintext_sha256": encrypted["plaintext_sha256"],
            "ciphertext_sha256": encrypted["ciphertext_sha256"],
            "size": size,
            "mtime_ns": mtime_ns,
            "source_sha256": source_sha256,
            "manifest_path": manifest_path,
            "manifest_raw_url": client.raw_url(target.owner, target.repository, target.branch, manifest_path),
            "encryption": manifest_payload["encryption"],
            "chunks": manifest_payload["chunks"],
            "commit_sha": commit_sha,
            "account_id": target.account_id,
            "repository_owner": target.owner,
            "repository": target.repository,
            "branch": target.branch,
            "uploaded_bytes": uploaded_bytes,
        }

    def _upload_file_version(
        self,
        state: dict[str, Any],
        file_id: str,
        file_path: Path,
        rel_path: str,
        size: int,
        mtime_ns: int,
        source_sha256: str,
        resume_version: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        plaintext = file_path.read_bytes()
        base_version = self._normalize_version(resume_version) if resume_version else None
        if base_version:
            nonce_b64 = base_version["encryption"]["nonce_b64"]
            nonce_padding = "=" * (-len(nonce_b64) % 4)
            nonce = base64.urlsafe_b64decode(nonce_b64 + nonce_padding)
            encrypted = encrypt_bytes_with_nonce(plaintext, self.secrets.encryption_key_bytes(), nonce)
            version_id = base_version["version_id"]
            version_created_at = base_version.get("created_at", utc_now_iso())
            copy_count = int(base_version.get("copy_count_requested", self.config.copy_count))
            used_account_ids = {copy.get("account_id") for copy in base_version.get("copies", []) if copy.get("account_id")}
            copies: list[dict[str, Any]] = [deepcopy(copy) for copy in base_version.get("copies", [])]
            copy_errors = list(base_version.get("copy_errors", []))
        else:
            encrypted = encrypt_bytes(plaintext, self.secrets.encryption_key_bytes())
            version_id = utc_now_compact()
            version_created_at = utc_now_iso()
            copy_count = self.config.copy_count
            used_account_ids = set()
            copies = []
            copy_errors = []

        chunk_items = list(chunk_bytes(encrypted["ciphertext"], self.config.github_chunk_size_bytes))
        estimated_upload_bytes = len(encrypted["ciphertext"]) + self._estimate_manifest_bytes(rel_path, len(chunk_items))
        remaining_accounts = list(account for account in self.config.github_accounts if account.account_id not in used_account_ids)

        while len(copies) < copy_count and remaining_accounts:
            try:
                target = self._allocate_upload_target(state, estimated_upload_bytes, excluded_account_ids=used_account_ids)
            except NoAvailableAccountsError:
                break
            if target.account_id in used_account_ids:
                remaining_accounts = [account for account in remaining_accounts if account.account_id != target.account_id]
                continue
            try:
                copy = self._upload_version_copy(
                    state,
                    file_id=file_id,
                    rel_path=rel_path,
                    size=size,
                    mtime_ns=mtime_ns,
                    source_sha256=source_sha256,
                    version_id=version_id,
                    version_created_at=version_created_at,
                    encrypted=encrypted,
                    chunk_items=chunk_items,
                    target=target,
                    copy_index=len(copies) + 1,
                    copy_count=copy_count,
                )
                copies.append(copy)
                used_account_ids.add(target.account_id)
                remaining_accounts = [account for account in remaining_accounts if account.account_id != target.account_id]
            except NoAvailableAccountsError:
                break
            except ServiceError as exc:
                copy_errors.append(
                    {
                        "copy_index": len(copies) + 1,
                        "account_id": target.account_id,
                        "network": "github",
                        "error": str(exc),
                    }
                )
                used_account_ids.add(target.account_id)
                remaining_accounts = [account for account in remaining_accounts if account.account_id != target.account_id]
                continue

        if not copies:
            raise NoAvailableAccountsError("Ninguna cuenta GitHub tiene cuota diaria disponible para esta subida.")

        primary_copy = copies[0]
        version: dict[str, Any] = {
            "version": 2,
            "file_id": file_id,
            "path": rel_path,
            "version_id": version_id,
            "created_at": version_created_at,
            "plaintext_sha256": encrypted["plaintext_sha256"],
            "ciphertext_sha256": encrypted["ciphertext_sha256"],
            "size": size,
            "mtime_ns": mtime_ns,
            "source_sha256": source_sha256,
            "network": "github",
            "copy_count_requested": copy_count,
            "copy_count_completed": len(copies),
            "replication_complete": len(copies) >= copy_count,
            "copy_errors": copy_errors if len(copies) < copy_count else [],
            "copies": copies,
            "account_id": primary_copy["account_id"],
            "repository_owner": primary_copy["repository_owner"],
            "repository": primary_copy["repository"],
            "branch": primary_copy["branch"],
            "manifest_path": primary_copy["manifest_path"],
            "manifest_raw_url": primary_copy["manifest_raw_url"],
            "commit_sha": primary_copy["commit_sha"],
            "encryption": primary_copy["encryption"],
            "chunks": primary_copy["chunks"],
            "uploaded_bytes": sum(int(copy.get("uploaded_bytes", 0)) for copy in copies),
        }
        version = self._normalize_version(version)
        return version

    def _allocate_upload_target(self, state: dict[str, Any], estimated_upload_bytes: int, excluded_account_ids: set[str] | None = None) -> UploadTarget:
        eligible_accounts: list[GitHubAccountConfig] = []
        today = self._today_bucket()
        estimated_upload_kb = math.ceil(estimated_upload_bytes / 1024)
        excluded_account_ids = excluded_account_ids or set()

        for account in self.config.github_accounts:
            if account.account_id in excluded_account_ids:
                continue
            account_state = self._account_state(state, account.account_id, owner=account.owner)
            if account.account_id in self._runtime_unavailable_accounts:
                continue
            used_today = int(account_state["daily_uploads"].get(today, 0))
            if used_today + estimated_upload_bytes > self.config.github_account_daily_upload_limit_bytes:
                continue
            eligible_accounts.append(account)

        if not eligible_accounts:
            raise NoAvailableAccountsError("Ninguna cuenta GitHub tiene cuota diaria disponible para esta subida.")

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
        # A fresh/empty repository must always accept at least one upload: a
        # single version's manifest + chunks cannot be split across repos, so
        # rejecting it here would make the data unstorable (and breaks repo
        # rollover when a small per-repo cap is smaller than the upload). Only
        # enforce the cap once the repo already holds data — that is what drives
        # allocation to roll over to (or create) the next repo.
        if current_size_kb > 0 and current_size_kb + math.ceil(upload_bytes / 1024) > self.config.github_repository_max_size_kb:
            raise ServiceError(f"El repositorio {target.owner}/{target.repository} supera el limite configurado.")

    def _refresh_managed_repositories(self, state: dict[str, Any], account: GitHubAccountConfig) -> list[RepositoryInfo]:
        client = self._client_for_account(account.account_id)
        try:
            repositories = client.list_managed_repositories(account.owner, self.config.github_repository_prefix)
        except GitHubError as exc:
            if self._handle_account_access_error(state, account, exc):
                raise ServiceError(
                    f"La cuenta {account.account_id} ha sido retirada del pool activo por permisos insuficientes."
                ) from exc
            raise
        account_state = self._account_state(state, account.account_id, owner=account.owner)
        known = account_state["repositories"]
        seen = set()
        for repo in repositories:
            seen.add(repo.name)
            repo_state = known.setdefault(repo.name, {})
            repo_state["name"] = repo.name
            repo_state["owner"] = repo.owner
            repo_state["network"] = "github"
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
        try:
            info = client.get_repository(account.owner, repository)
        except GitHubError as exc:
            if self._handle_account_access_error(state, account, exc):
                raise ServiceError(
                    f"La cuenta {account.account_id} ha sido retirada del pool activo por permisos insuficientes."
                ) from exc
            raise
        repo_state = self._account_state(state, account.account_id, owner=account.owner)["repositories"].setdefault(repository, {})
        repo_state["name"] = repository
        repo_state["owner"] = account.owner
        repo_state["network"] = "github"
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
            if self._handle_account_access_error(state, account, exc):
                raise ServiceError(
                    f"La cuenta {account.account_id} ha sido retirada del pool activo por permisos insuficientes."
                ) from exc
            message = str(exc)
            raise ServiceError(
                f"No se pudo crear el repositorio {account.owner}/{candidate} para la cuenta {account.account_id}: {message}"
            ) from exc
        repo_state = account_state["repositories"].setdefault(candidate, {})
        repo_state["name"] = candidate
        repo_state["owner"] = account.owner
        repo_state["network"] = "github"
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

    def _handle_account_access_error(self, state: dict[str, Any], account: GitHubAccountConfig, exc: Exception) -> bool:
        message = str(exc)
        if "HTTP 403" not in message or "Resource not accessible by personal access token" not in message:
            return False
        alert_message = (
            f"La cuenta {account.account_id} ha sido retirada del pool activo: el token no puede acceder o "
            f"administrar recursos en {account.owner}. Revisa permisos o sustituye la cuenta."
        )
        self._mark_account_unavailable(
            state,
            account,
            code="personal_access_token_forbidden",
            message=alert_message,
        )
        return True

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
        version = self._normalize_version(version)
        account_id = version.get("account_id")
        if account_id:
            return account_id
        copies = version.get("copies") or []
        if copies:
            return copies[0].get("account_id")
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
                    "network": "github",
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
                "network": "github",
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
        payload.setdefault("network", "github")
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
                    "network": account_state.get("network", "github"),
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
