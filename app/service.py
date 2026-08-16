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

from .config import AppConfig, GitHubAccountConfig, TelegramAccountConfig, RuntimeSecrets
from .crypto import StreamingAESGCMDecryptor, chunk_bytes, encrypt_bytes, encrypt_bytes_with_nonce
from .github_api import GitHubClient, GitHubError, GitHubSettings, RepositoryInfo
from .registry import (
    STATUS_COMPLETE,
    STATUS_ERROR,
    STATUS_UPLOADING,
    STORAGE_BLOB,
    STORAGE_RELEASE,
    Registry,
    distinct_account_copy_count,
    import_legacy_files,
    version_storage,
)
from .telegram_api import TelegramClient, TelegramError, TelegramSettings
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

LEGACY_IMPORT_FLAG = "index_json_files_migrated"


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
                    content_requests_per_hour=config.github_content_requests_per_hour,
                    content_requests_per_minute=config.github_content_requests_per_minute,
                    max_concurrency=config.github_max_concurrency,
                )
            )
            for account in config.github_accounts
        }
        self.account_by_id = {account.account_id: account for account in config.github_accounts}
        self.telegram_clients = {
            acc.account_id: TelegramClient(
                TelegramSettings(
                    api_id=acc.api_id,
                    api_hash=acc.api_hash,
                    phone_number=acc.phone,
                    session_name=acc.account_id,
                    timeout_s=config.tg_timeout_seconds,
                    max_retry=config.tg_max_retry,
                    backoff_s=config.tg_backoff_seconds,
                    session_dir=str(config.app_state_dir),
                )
            )
            for acc in config.telegram_accounts
        }
        self.telegram_account_by_id = {acc.account_id: acc for acc in config.telegram_accounts}
        self._task_locks = {"sync": threading.Lock(), "verify": threading.Lock()}
        self._task_lock_paths = {
            "sync": self.config.app_state_dir / "sync.lock",
            "verify": self.config.app_state_dir / "verify.lock",
        }
        self._upload_index_path = self.config.app_state_dir / "upload_index.sqlite3"
        self.registry = Registry(self._upload_index_path)
        self._runtime_unavailable_accounts: set[str] = set()
        self._choose = chooser or random.choice
        self._sleep_sampler = sleep_sampler or random.uniform
        self._sleeper = sleeper or time.sleep
        # live in-memory state while a task is running (used by web UI to reflect progress)
        self._live_state: dict[str, Any] | None = None
        self._live_state_lock = threading.RLock()
        self._import_legacy_file_map()

    def _import_legacy_file_map(self) -> None:
        """Move ``index.json["files"]`` into the registry, once.

        index.json was rewritten in full on every save and re-parsed on every
        page load; with tens of thousands of files that dominated startup. The
        map now lives in SQLite and index.json keeps only config, tasks and
        accounts. Guarded by a meta flag so it runs at most once per state dir.
        """
        if self.registry.get_meta(LEGACY_IMPORT_FLAG) == "1":
            return
        state = self.state_manager.load(self.default_config)
        legacy_files = state.get("files") or {}
        if legacy_files:
            imported = import_legacy_files(legacy_files, self.registry)
            LOGGER.info("registro: %s archivos importados desde index.json", imported)
        if "files" in state:
            state.pop("files", None)
            self.state_manager.save(state)
        self.registry.set_meta(LEGACY_IMPORT_FLAG, "1")

    def get_state(self) -> dict[str, Any]:
        # If a task is running, prefer the live in-memory state so the web UI
        # can show progress before it is persisted. The file map is NOT part of
        # this any more: it lives in the registry and is queried on demand.
        with self._live_state_lock:
            live = deepcopy(self._live_state) if self._live_state is not None else None

        if live is not None:
            state = live
        else:
            state = self.state_manager.snapshot(self.default_config)

        self._refresh_task_running_flags(state)
        self._augment_state_for_web(state)
        return state

    def get_stats(self) -> dict[str, Any]:
        """Aggregate file counters, computed in SQL rather than in Python."""
        return self.registry.stats(copy_count_target=self.config.copy_count)

    def list_files(self, *, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        rows = self.registry.list_files(limit=limit, offset=offset)
        return [
            {
                "file_id": row.file_id,
                "path": row.rel_path,
                "present": row.present,
                "status": row.status,
                "version_id": row.version_id,
                "last_error": row.last_error,
                "last_verified_at": row.last_verified_at,
                "last_verification_ok": row.last_verification_ok,
            }
            for row in rows
        ]

    def get_file_detail(self, file_id: str) -> dict[str, Any] | None:
        row = self.registry.get_file(file_id)
        if row is None:
            return None
        return {
            "file_id": row.file_id,
            "path": row.rel_path,
            "present": row.present,
            "size": row.size,
            "status": row.status,
            "source_sha256": row.source_sha256,
            "active_version_id": row.version_id,
            "last_error": row.last_error,
            "last_verification": self.registry.verification_detail(file_id),
            "versions": self.registry.list_versions(file_id),
        }

    def run_sync(self) -> TaskResult:
        return self._run_task("sync", self._sync_impl)

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

                self._reset_rate_limit_stats()
                try:
                    result = callback(state)
                    self._log_rate_limit_summary(task_name)

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

    def _sync_impl(self, state: dict[str, Any]) -> TaskResult:
        """Upload what is new or changed, and nothing else.

        There is one sync mode. The old pair forced a bad trade: the light sync
        trusted the persisted version and never re-hashed, so it did not detect
        modifications at all; the only way to notice a change was the full sync,
        which re-hashed every file. Comparing ``(size, mtime_ns)`` against the
        registry makes a single mode both fast and correct — an unchanged file
        is skipped without being hashed or even opened, so the startup cost is
        proportional to the number of changes rather than to the total volume.
        """
        if not self.config.app_data_dir.exists():
            raise ServiceError(f"No existe el directorio de datos: {self.config.app_data_dir}")

        self._ensure_github_accounts_state(state)
        run = self.registry.next_counter("sync_run")
        LOGGER.info("sync escaneando directorio=%s run=%s", self.config.app_data_dir, run)

        counters = {
            "scanned": 0,
            "unchanged": 0,
            "metadata_only": 0,
            "reused": 0,
            "uploaded": 0,
            "failed": 0,
            "uploaded_bytes": 0,
        }
        last_log_at = time.monotonic()

        for file_path in iter_files(self.config.app_data_dir):
            rel_path = rel_path_str(self.config.app_data_dir, file_path)
            file_id = stable_file_id(rel_path)
            try:
                stat = file_path.stat()
            except OSError as exc:
                LOGGER.warning("sync no se pudo inspeccionar path=%s: %s", rel_path, exc)
                continue
            size = stat.st_size
            mtime_ns = stat.st_mtime_ns
            counters["scanned"] += 1

            row = self.registry.get_file(file_id)

            # 1. Unchanged: one indexed lookup, then skip without hashing and
            #    without touching the file. This is the fast startup.
            if (
                row is not None
                and row.status == STATUS_COMPLETE
                and row.present
                and row.size == size
                and row.mtime_ns == mtime_ns
            ):
                self.registry.touch_file(file_id, size=size, mtime_ns=mtime_ns, seen_run=run)
                counters["unchanged"] += 1
                last_log_at = self._log_scan_progress(counters, rel_path, last_log_at)
                continue

            source_sha256 = sha256_file(file_path)
            resume_version: dict[str, Any] | None = None

            # 2. Metadata moved but content did not (a touch, a restore, a
            #    re-copy): record the new size/mtime and re-upload nothing.
            if row is not None and row.source_sha256 == source_sha256:
                active = self.registry.get_active_version(file_id)
                if active is not None and self._is_version_replication_complete(active):
                    self.registry.upsert_file(
                        file_id=file_id,
                        rel_path=rel_path,
                        size=size,
                        mtime_ns=mtime_ns,
                        source_sha256=source_sha256,
                        version_id=active["version_id"],
                        status=STATUS_COMPLETE,
                        present=True,
                        last_error=None,
                        seen_run=run,
                    )
                    changed_metadata = row.size != size or row.mtime_ns != mtime_ns
                    counters["metadata_only" if changed_metadata else "unchanged"] += 1
                    last_log_at = self._log_scan_progress(counters, rel_path, last_log_at)
                    continue
                # Same content, but the version never finished replicating.
                resume_version = active

            # 3. This content is already stored under some other path: adopt the
            #    remote version rather than uploading the same bytes twice.
            if resume_version is None:
                existing = self.registry.lookup_uploaded_version(source_sha256)
                if existing is not None:
                    existing = self._normalize_version(existing)
                    if self._is_version_replication_complete(existing):
                        self._adopt_version(
                            file_id=file_id,
                            rel_path=rel_path,
                            size=size,
                            mtime_ns=mtime_ns,
                            source_sha256=source_sha256,
                            version=existing,
                            run=run,
                        )
                        self.registry.mark_uploaded_copy_seen(source_sha256)
                        counters["reused"] += 1
                        last_log_at = self._log_scan_progress(counters, rel_path, last_log_at)
                        continue
                    resume_version = existing

            # 4. New content, or a version whose replication has to be finished.
            self._process_upload(
                state,
                file_id=file_id,
                file_path=file_path,
                rel_path=rel_path,
                size=size,
                mtime_ns=mtime_ns,
                source_sha256=source_sha256,
                resume_version=resume_version,
                run=run,
                counters=counters,
            )
            last_log_at = self._log_scan_progress(counters, rel_path, last_log_at)

        # Only safe now that the walk completed: an aborted walk would flag
        # every path it had not reached yet as missing.
        self.registry.mark_absent_except_run(run)
        self._finalize_sync(state)
        self.state_manager.save(state)

        LOGGER.info(
            "sync terminado: revisados=%s subidos=%s reutilizados=%s solo_metadatos=%s sin_cambios=%s errores=%s",
            counters["scanned"],
            counters["uploaded"],
            counters["reused"],
            counters["metadata_only"],
            counters["unchanged"],
            counters["failed"],
        )
        summary = {
            "scanned_files": counters["scanned"],
            "unchanged_files": counters["unchanged"],
            "metadata_only_files": counters["metadata_only"],
            "uploaded_files": counters["uploaded"],
            "reused_files": counters["reused"],
            "failed_files": counters["failed"],
            "uploaded_bytes": counters["uploaded_bytes"],
            "missing_files": self.registry.stats()["absent"],
        }
        failed = counters["failed"]
        return TaskResult(failed == 0, summary, None if failed == 0 else f"{failed} archivos con error")

    def _finalize_sync(self, state: dict[str, Any]) -> None:
        """Hook for end-of-run remote bookkeeping. Overridden by the Releases backend."""

    def _log_scan_progress(self, counters: dict[str, int], path: str, last_log_at: float) -> float:
        scanned = counters["scanned"]
        if scanned == 1 or scanned % 100 == 0 or time.monotonic() - last_log_at >= 10:
            LOGGER.info(
                "sync progreso: revisados=%s sin_cambios=%s solo_metadatos=%s reutilizados=%s "
                "subidos=%s errores=%s ultimo=%s",
                scanned,
                counters["unchanged"],
                counters["metadata_only"],
                counters["reused"],
                counters["uploaded"],
                counters["failed"],
                path,
            )
            return time.monotonic()
        return last_log_at

    def _adopt_version(
        self,
        *,
        file_id: str,
        rel_path: str,
        size: int,
        mtime_ns: int,
        source_sha256: str,
        version: dict[str, Any],
        run: int,
    ) -> None:
        version = self._normalize_version(version)
        self.registry.save_version(
            file_id,
            version,
            distinct_account_copies=distinct_account_copy_count(version),
            storage=version_storage(version),
        )
        self.registry.upsert_file(
            file_id=file_id,
            rel_path=rel_path,
            size=size,
            mtime_ns=mtime_ns,
            source_sha256=source_sha256,
            version_id=version["version_id"],
            status=STATUS_COMPLETE,
            present=True,
            last_error=None,
            seen_run=run,
        )
        self.registry.clear_verification(file_id)

    def _process_upload(
        self,
        state: dict[str, Any],
        *,
        file_id: str,
        file_path: Path,
        rel_path: str,
        size: int,
        mtime_ns: int,
        source_sha256: str,
        resume_version: dict[str, Any] | None,
        run: int,
        counters: dict[str, int],
    ) -> None:
        # Written before the upload starts so an interrupted run leaves an
        # `uploading` row the next sync can reconcile against the remote.
        self.registry.upsert_file(
            file_id=file_id,
            rel_path=rel_path,
            size=size,
            mtime_ns=mtime_ns,
            source_sha256=source_sha256,
            status=STATUS_UPLOADING,
            present=True,
            last_error=None,
            seen_run=run,
        )
        LOGGER.info(
            "sync %s path=%s size=%sB", "completando" if resume_version else "subiendo", rel_path, size
        )
        try:
            version = self._upload_file_version(
                state,
                file_id,
                file_path,
                rel_path,
                size,
                mtime_ns,
                source_sha256,
                resume_version=resume_version,
            )
        except NoAvailableAccountsError as exc:
            # A file with nowhere to go is one failed file, not a failed run:
            # aborting here used to cancel the whole sync even when other
            # accounts or networks could still take the remaining files.
            self._record_file_failure(file_id, rel_path, size, mtime_ns, source_sha256, str(exc), run)
            counters["failed"] += 1
            LOGGER.error("sync sin destino para path=%s: %s", rel_path, exc)
            return
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the run
            self._record_file_failure(file_id, rel_path, size, mtime_ns, source_sha256, str(exc), run)
            counters["failed"] += 1
            LOGGER.exception("sync error subiendo path=%s", rel_path)
            return

        complete = bool(version.get("replication_complete", True))
        copy_errors = version.get("copy_errors") or []
        last_error = None
        if not complete:
            last_error = (
                copy_errors[-1].get("error")
                if copy_errors
                else "La version no se pudo replicar completamente."
            )

        self.registry.save_version(
            file_id,
            version,
            distinct_account_copies=distinct_account_copy_count(version),
            storage=version_storage(version),
        )
        self.registry.upsert_file(
            file_id=file_id,
            rel_path=rel_path,
            size=size,
            mtime_ns=mtime_ns,
            source_sha256=source_sha256,
            version_id=version["version_id"],
            status=STATUS_COMPLETE if complete else STATUS_ERROR,
            present=True,
            last_error=last_error,
            seen_run=run,
        )
        # A new active version invalidates any verification of the old one.
        self.registry.clear_verification(file_id)

        if complete:
            counters["uploaded"] += 1
            self.registry.record_uploaded_version(source_sha256, version)
        else:
            counters["failed"] += 1
        counters["uploaded_bytes"] += int(version.get("uploaded_bytes", 0))
        LOGGER.info(
            "sync archivo %s path=%s cuenta=%s repo=%s version=%s bytes=%s",
            "replicado" if complete else "parcial",
            rel_path,
            version.get("account_id"),
            version.get("repository"),
            version["version_id"],
            version.get("uploaded_bytes"),
        )

    def _record_file_failure(
        self,
        file_id: str,
        rel_path: str,
        size: int,
        mtime_ns: int,
        source_sha256: str,
        error: str,
        run: int,
    ) -> None:
        self.registry.upsert_file(
            file_id=file_id,
            rel_path=rel_path,
            size=size,
            mtime_ns=mtime_ns,
            source_sha256=source_sha256,
            status=STATUS_ERROR,
            present=True,
            last_error=error,
            seen_run=run,
        )

    def _verify_impl(self, state: dict[str, Any]) -> TaskResult:
        """Check what is stored remotely. Never writes to /datos or to GitHub.

        Two tiers, because a full re-download of everything is not affordable at
        volume and a pure metadata check is not proof:

        * metadata, every file — presence and size of each remote part. These
          are reads, so they do not consume the content-generating budget.
        * deep, 1 file in every ``VERIFY_DEEP_EVERY_N`` — download, per-part
          sha256, and an AES-GCM close against the local hash. The selection
          rotates on the run counter, so N=100 covers the whole set across 100
          runs with no overlap, and N=1 checks everything on every run.

        Results go only to the local registry.
        """
        self._ensure_github_accounts_state(state)
        run = self.registry.next_counter("verify_run")
        deep_every_n = max(1, int(self.config.verify_deep_every_n))
        remote_assets = self._remote_asset_index()

        verified = 0
        failures = 0
        copies_verified = 0
        copies_failed = 0
        copies_skipped = 0
        deep_files = 0

        for row in self.registry.iter_files(present_only=True):
            version = self.registry.get_active_version(row.file_id)
            if not version:
                continue

            local_path = self.config.app_data_dir / row.rel_path
            if not local_path.exists():
                self.registry.mark_missing(
                    row.file_id, error="Archivo ausente durante la verificacion."
                )
                failures += 1
                continue

            deep = (int(row.file_id, 16) % deep_every_n) == (run % deep_every_n)
            if deep:
                deep_files += 1
            local_sha = sha256_file(local_path) if deep else None
            version = self._normalize_version(version)

            copy_results: list[dict[str, Any]] = []
            for copy in version.get("copies", []):
                try:
                    detail = self._verify_copy(
                        row.rel_path, copy, local_sha, deep=deep, remote_assets=remote_assets
                    )
                    copy_results.append(detail)
                    if detail.get("skipped"):
                        copies_skipped += 1
                    else:
                        copies_verified += 1
                except Exception as exc:  # noqa: BLE001 - reported per copy
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
            failed_copies = [
                detail for detail in copy_results if not detail.get("ok") and not detail.get("skipped")
            ]
            # A file passes only if no copy failed AND at least one copy was
            # actually checked: a version we cannot verify at all is not "ok".
            file_ok = not failed_copies and bool(ok_copies)
            primary = ok_copies[0] if ok_copies else (copy_results[0] if copy_results else {})

            detail_payload = {
                "checked_at": utc_now_iso(),
                "ok": file_ok,
                "depth": "deep" if deep else "metadata",
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
            error = None
            if not file_ok:
                error = (
                    failed_copies[0].get("error")
                    if failed_copies
                    else "No se pudo verificar ninguna copia de la version."
                )
            self.registry.record_verification(
                row.file_id,
                ok=file_ok,
                version_id=version["version_id"],
                detail=detail_payload,
                last_error=error,
            )
            if file_ok:
                verified += 1
            else:
                failures += 1

        self.state_manager.save(state)
        return TaskResult(
            failures == 0,
            {
                "verified_files": verified,
                "failed_files": failures,
                "deep_checked_files": deep_files,
                "deep_every_n": deep_every_n,
                "present_files": self.registry.stats()["present"],
                "copies_verified": copies_verified,
                "copies_failed": copies_failed,
                "copies_skipped": copies_skipped,
            },
            None if failures == 0 else f"{failures} archivos con error",
        )

    def _remote_asset_index(self) -> dict[str, dict[str, int]]:
        """Presence and size of every remote asset, one listing per release.

        Empty while no data has been written through the Releases backend; the
        commit-based layout has no equivalent cheap listing.
        """
        return {}

    def _verify_copy(
        self,
        rel_path: str,
        copy: dict[str, Any],
        local_sha: str | None,
        *,
        deep: bool,
        remote_assets: dict[str, dict[str, int]] | None = None,
    ) -> dict[str, Any]:
        """Verify a single copy of a version against the local file hash.

        Dispatch is by ``copy["storage"]``: ``"release"`` for data written
        through the Releases backend, ``"blob"`` for the commit-based layout,
        which stays readable. A copy on a network without a configured client is
        reported as ``skipped`` (not failed), so GitHub-only deployments keep
        passing while leaving a record that the copy was not checked.
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

        storage = copy.get("storage", STORAGE_BLOB)
        if storage == STORAGE_RELEASE:
            return self._verify_release_copy(
                rel_path, copy, local_sha, deep=deep, remote_assets=remote_assets or {}
            )
        return self._verify_blob_copy(rel_path, copy, local_sha, deep=deep)

    def _verify_blob_copy(
        self, rel_path: str, copy: dict[str, Any], local_sha: str | None, *, deep: bool
    ) -> dict[str, Any]:
        copy_index = copy.get("copy_index")
        if not deep or local_sha is None:
            # The commit-based layout offers no cheap listing to check presence
            # and size against, so an unselected legacy copy is not checked at
            # all rather than reported as if it had been.
            return {
                "ok": False,
                "skipped": True,
                "copy_index": copy_index,
                "network": "github",
                "storage": STORAGE_BLOB,
                "account_id": copy.get("account_id"),
                "repository": copy.get("repository"),
                "error": "Copia legacy no seleccionada para verificacion profunda en esta ronda.",
            }

        client = self._client_for_account(copy.get("account_id"))
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
            "network": "github",
            "storage": STORAGE_BLOB,
            "account_id": copy.get("account_id"),
            "repository": copy.get("repository"),
            "remote_sha256": remote_sha,
            "chunks_checked": downloaded_chunks,
        }

    def _is_version_replication_complete(self, version: dict[str, Any] | None) -> bool:
        if not version:
            return False
        requested = int(version.get("copy_count_requested", self.config.copy_count))
        return distinct_account_copy_count(version) >= requested

    @staticmethod
    def distinct_account_copy_count(version: dict[str, Any] | None) -> int:
        return distinct_account_copy_count(version)

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

    def _upload_telegram_version_copy(
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
        account: TelegramAccountConfig,
        copy_index: int,
        copy_count: int,
    ) -> dict[str, Any]:
        client = self.telegram_clients[account.account_id]
        LOGGER.info(
            "sync telegram destino elegido path=%s copia=%s/%s cuenta=%s chunks=%s",
            rel_path, copy_index, copy_count, account.account_id, len(chunk_items),
        )
        channels = client.list_managed_channels(self.config.tg_channel_prefix)
        if channels:
            channel = channels[0]
        else:
            channel = client.create_channel(f"{self.config.tg_channel_prefix}-0001")

        chunks_data = [chunk for (idx, chunk) in chunk_items]
        chunk_filenames = [f"{file_id}_{version_id}_chunk_{idx:04d}.bin" for (idx, _) in chunk_items]

        tg = client.commit_copy(
            channel.chat_id,
            version_id,
            chunks_data,
            chunk_filenames,
            sleep_after_upload=self._sleep_after_upload,
        )

        self._record_uploaded_bytes(state, account.account_id, tg["uploaded_bytes"])

        return {
            "copy_index": copy_index,
            "network": "telegram",
            "version_id": version_id,
            "created_at": version_created_at,
            "file_id": file_id,
            "path": rel_path,
            "plaintext_sha256": encrypted["plaintext_sha256"],
            "ciphertext_sha256": encrypted["ciphertext_sha256"],
            "size": size,
            "mtime_ns": mtime_ns,
            "source_sha256": source_sha256,
            "account_id": account.account_id,
            "channel_id": channel.chat_id,
            "channel_title": channel.title,
            "manifest_message_id": tg["manifest_message_id"],
            "manifest_file_unique_id": tg["manifest_file_unique_id"],
            "encryption": {
                "algorithm": encrypted["algorithm"],
                "nonce_b64": encrypted["nonce_b64"],
                "key_id": "state-default",
            },
            "chunks": tg["chunks"],
            "uploaded_bytes": tg["uploaded_bytes"],
            "repository_owner": None,
            "repository": None,
            "branch": None,
            "manifest_path": None,
            "manifest_raw_url": None,
            "commit_sha": None,
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
                LOGGER.warning(
                    "sync copia GitHub fallida path=%s cuenta=%s: %s",
                    rel_path, target.account_id, exc,
                )
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

        # Fase 2: colocar copias restantes en cuentas Telegram
        remaining_telegram_accounts = [
            acc for acc in self.config.telegram_accounts
            if acc.account_id not in used_account_ids
        ]
        for tg_account in remaining_telegram_accounts:
            if len(copies) >= copy_count:
                break
            try:
                copy = self._upload_telegram_version_copy(
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
                    account=tg_account,
                    copy_index=len(copies) + 1,
                    copy_count=copy_count,
                )
                copies.append(copy)
                used_account_ids.add(tg_account.account_id)
            except (TelegramError, ServiceError) as exc:
                LOGGER.warning(
                    "sync copia Telegram fallida path=%s cuenta=%s: %s",
                    rel_path, tg_account.account_id, exc,
                )
                copy_errors.append(
                    {
                        "copy_index": len(copies) + 1,
                        "account_id": tg_account.account_id,
                        "network": "telegram",
                        "error": str(exc),
                    }
                )
                used_account_ids.add(tg_account.account_id)

        if not copies:
            # Mensaje honesto: antes siempre se culpaba al cupo de GitHub, aun
            # cuando el verdadero motivo era un fallo en la copia de Telegram.
            # Incluimos los errores reales por cuenta para poder diagnosticar.
            error_details = "; ".join(
                f"{err.get('network')}/{err.get('account_id')}: {err.get('error')}"
                for err in copy_errors
            )
            if error_details:
                message = f"No se pudo subir ninguna copia. Errores por cuenta: {error_details}"
            elif self.config.telegram_accounts:
                message = (
                    "No se pudo subir ninguna copia: ninguna cuenta GitHub tiene cupo "
                    "diario disponible y las cuentas Telegram no aceptaron la subida."
                )
            else:
                message = (
                    "No se pudo subir ninguna copia: ninguna cuenta GitHub tiene cupo "
                    "diario disponible y no hay cuentas Telegram configuradas."
                )
            raise NoAvailableAccountsError(message)

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
            "network": primary_copy.get("network", "github"),
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
        """Artificial throttle, kept only for the Telegram path.

        GitHub uploads no longer sleep here: pacing is now the RateLimiter's job
        (a proactive token bucket), and a fixed per-blob sleep only added dead
        time — ~1.8s per small file with the documented 0.25-1.5s range, on top
        of a budget that is already the binding constraint.
        """
        if self.config.github_upload_sleep_max_seconds <= 0:
            return
        duration = self._sleep_sampler(
            self.config.github_upload_sleep_min_seconds,
            self.config.github_upload_sleep_max_seconds,
        )
        if duration > 0:
            self._sleeper(duration)

    def _reset_rate_limit_stats(self) -> None:
        for client in self.github_clients.values():
            limiter = getattr(client, "rate_limiter", None)
            if limiter is not None:
                limiter.reset_stats()

    def _log_rate_limit_summary(self, task_name: str) -> None:
        """Per-run consumption report.

        GitHub does not document whether uploads to uploads.github.com count
        against the content-generating budget, so the budget has to be tuned
        against measured numbers rather than assumed ones. This is that
        measurement.
        """
        for account_id, client in self.github_clients.items():
            summary = getattr(client, "rate_limit_summary", None)
            if summary is None:
                continue
            LOGGER.info("%s rate-limit resumen cuenta=%s %s", task_name, account_id, summary())

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
        is_telegram = account_id in self.telegram_account_by_id
        default_network = "telegram" if is_telegram else "github"
        default_owner = owner or self.account_by_id.get(account_id, GitHubAccountConfig(account_id, "", "")).owner
        payload = github_accounts.setdefault(
            account_id,
            {
                "account_id": account_id,
                "owner": default_owner,
                "network": default_network,
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
        payload.setdefault("network", default_network)
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

        try:
            import pyrogram  # noqa: F401
            pyrogram_available = True
        except ImportError:
            pyrogram_available = False

        tg_summaries = []
        for acc in self.config.telegram_accounts:
            phone = acc.phone
            phone_display = phone[:4] + "****" + phone[-2:] if len(phone) > 6 else phone
            account_state = state.get("github_accounts", {}).get(acc.account_id, {})
            today = self._today_bucket()
            session_file = self.config.app_state_dir / f"{acc.account_id}.session"
            tg_summaries.append(
                {
                    "account_id": acc.account_id,
                    "phone": phone_display,
                    "api_id": acc.api_id,
                    "network": "telegram",
                    "pyrogram_available": pyrogram_available,
                    "uploaded_today_bytes": int(account_state.get("daily_uploads", {}).get(today, 0)),
                    "available": bool(account_state.get("available", True)),
                    "unavailable_reason": account_state.get("unavailable_reason"),
                    "has_session": session_file.exists(),
                }
            )
        state["telegram_account_summaries"] = tg_summaries
        state["pyrogram_available"] = pyrogram_available

    @staticmethod
    def _today_bucket() -> str:
        return utc_now_iso().split("T", 1)[0]

    def mark_manual_trigger(self, task_name: str) -> None:
        state = self.state_manager.load(self.default_config)
        state["tasks"][task_name]["last_manual_trigger_at"] = utc_now_iso()
        self.state_manager.save(state)
