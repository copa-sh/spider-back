from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import math
import os
import random
import tempfile
import threading
import time
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .asset_format import (
    HEADER_RESERVED,
    PREFIX_SIZE,
    asset_name,
    iter_decrypted_part,
    write_part,
)
from .config import AppConfig, GitHubAccountConfig, TelegramAccountConfig, RuntimeSecrets
from .crypto import StreamingAESGCMDecryptor, chunk_bytes, encrypt_bytes
from .github_api import GitHubClient, GitHubError, GitHubSettings, RepositoryInfo
from .registry import (
    RELEASE_ASSET_LIMIT,
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

# One consolidated manifest per release, not per file: a per-file manifest would
# cost a second content-generating request per file and cancel out half the gain.
MANIFEST_ASSET_NAME = "manifest.spdr"

GCM_TAG_BYTES = 16


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
class ReleaseTarget:
    """An open release with room for more assets."""

    account_id: str
    owner: str
    repository: str
    tag: str
    release_id: int


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
            "sync_order": config.sync_order,
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
        # Release bookkeeping, resolved once per process instead of per file.
        self._release_repositories: dict[str, str] = {}
        self._remote_assets: dict[str, dict[str, Any]] = {}
        self._releases_touched: set[str] = set()
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
        LOGGER.info(
            "sync escaneando directorio=%s run=%s orden=%s",
            self.config.app_data_dir, run, self.config.sync_order,
        )

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

        for file_path in iter_files(self.config.app_data_dir, order=self.config.sync_order):
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
            #    Reuse the version id of an interrupted attempt on the same
            #    content: asset names are derived from it, so matching names are
            #    what let the next run reconcile the parts already uploaded
            #    instead of orphaning them under a fresh id.
            resume_version_id = None
            if resume_version is not None:
                resume_version_id = resume_version["version_id"]
            elif row is not None and row.source_sha256 == source_sha256 and row.version_id:
                resume_version_id = row.version_id

            self._process_upload(
                state,
                file_id=file_id,
                file_path=file_path,
                rel_path=rel_path,
                size=size,
                mtime_ns=mtime_ns,
                source_sha256=source_sha256,
                resume_version=resume_version,
                resume_version_id=resume_version_id,
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
        resume_version_id: str | None,
        run: int,
        counters: dict[str, int],
    ) -> None:
        version_id = resume_version_id or utc_now_compact()
        # Written before the upload starts, version id included: an interrupted
        # run then leaves an `uploading` row the next sync can reconcile against
        # the remote, because the asset names it would use are the same ones.
        self.registry.upsert_file(
            file_id=file_id,
            rel_path=rel_path,
            size=size,
            mtime_ns=mtime_ns,
            source_sha256=source_sha256,
            version_id=version_id,
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
                version_id=version_id,
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

    # ── Releases backend ────────────────────────────────────────────────────
    #
    # One asset per part, at exactly one content-generating request per part,
    # against five per file for the blob+commit path (chunk blob, manifest blob,
    # tree, commit, ref). With a 500/hour secondary limit that path capped the
    # service at ~100 files/hour no matter how fast the network was.
    #
    # The manifest is deliberately NOT per file: one manifest asset per release,
    # rewritten at the end of a sync. A per-file manifest would double the cost
    # back to 2 requests/file and cancel out half the gain.

    def _release_repository(self, state: dict[str, Any], account: GitHubAccountConfig) -> str:
        """The repository that hosts an account's releases, resolved once.

        Releases do not count against repository size, so there is no rollover
        here and no per-file `list_managed_repositories` call — that used to run
        once per file.
        """
        cached = self._release_repositories.get(account.account_id)
        if cached:
            return cached
        stored = self.registry.get_meta(f"release_repo:{account.account_id}")
        if stored:
            self._release_repositories[account.account_id] = stored
            return stored

        if account.pinned_repository:
            repo_name = account.pinned_repository
        else:
            repositories = self._refresh_managed_repositories(state, account)
            repo_name = repositories[0].name if repositories else self._create_next_repository(state, account).name

        self.registry.set_meta(f"release_repo:{account.account_id}", repo_name)
        self._release_repositories[account.account_id] = repo_name
        return repo_name

    def _open_release(self, state: dict[str, Any], account: GitHubAccountConfig) -> ReleaseTarget:
        row = self.registry.open_release_for_account(account.account_id)
        if row is not None and row.release_id is not None and row.repo:
            return ReleaseTarget(account.account_id, row.owner, row.repo, row.tag, int(row.release_id))
        return self._create_next_release(state, account)

    def _create_next_release(self, state: dict[str, Any], account: GitHubAccountConfig) -> ReleaseTarget:
        owner = account.owner
        repo = self._release_repository(state, account)
        client = self._client_for_account(account.account_id)

        for _ in range(64):
            index = self.registry.next_release_index(account.account_id)
            tag = f"{self.config.github_repository_prefix}-{index:04d}"
            while self.registry.get_release(tag) is not None:
                index += 1
                tag = f"{self.config.github_repository_prefix}-{index:04d}"

            try:
                client.ensure_branch_initialized(owner, repo, self.config.github_branch)
                release = client.get_or_create_release(owner, repo, tag)
                assets = client.list_release_assets(owner, repo, int(release["id"]))
            except GitHubError as exc:
                if self._handle_account_access_error(state, account, exc):
                    raise ServiceError(
                        f"La cuenta {account.account_id} ha sido retirada del pool activo por permisos insuficientes."
                    ) from exc
                raise

            # Reconcile against what is actually there: the tag may already hold
            # assets from an interrupted run, and that listing is what makes
            # resume idempotent.
            self._remote_assets[tag] = {item["name"]: item for item in assets}
            full = len(assets) >= RELEASE_ASSET_LIMIT - 1
            self.registry.upsert_release(
                tag=tag,
                account_id=account.account_id,
                owner=owner,
                repo=repo,
                release_id=int(release["id"]),
                asset_count=len(assets),
                sealed=full,
            )
            LOGGER.info(
                "sync release abierta cuenta=%s repo=%s/%s tag=%s assets=%s",
                account.account_id, owner, repo, tag, len(assets),
            )
            if not full:
                return ReleaseTarget(account.account_id, owner, repo, tag, int(release["id"]))

        raise ServiceError(f"No se pudo abrir una release con espacio para la cuenta {account.account_id}.")

    def _remote_asset(self, target: ReleaseTarget, release_tag: str, name: str) -> dict[str, Any] | None:
        cached = self._remote_assets.get(release_tag)
        if cached is None:
            release = self.registry.get_release(release_tag)
            if release is None or release.release_id is None:
                return None
            client = self._client_for_account(target.account_id)
            try:
                assets = client.list_release_assets(
                    release.owner, release.repo, int(release.release_id)
                )
            except GitHubError as exc:
                LOGGER.warning("sync no se pudo listar assets de %s: %s", release_tag, exc)
                return None
            cached = {item["name"]: item for item in assets}
            self._remote_assets[release_tag] = cached
        return cached.get(name)

    def _upload_release_copy(
        self,
        state: dict[str, Any],
        *,
        file_id: str,
        rel_path: str,
        file_path: Path,
        size: int,
        mtime_ns: int,
        source_sha256: str,
        version_id: str,
        version_created_at: str,
        account: GitHubAccountConfig,
        copy_index: int,
        copy_count: int,
    ) -> dict[str, Any]:
        part_size = self.config.github_part_size_bytes
        parts_total = max(1, math.ceil(size / part_size)) if size else 1
        part_payloads: list[dict[str, Any]] = []
        uploaded_bytes = 0

        LOGGER.info(
            "sync destino elegido path=%s copia=%s/%s cuenta=%s partes=%s",
            rel_path, copy_index, copy_count, account.account_id, parts_total,
        )

        with file_path.open("rb") as source:
            for part in range(parts_total):
                target = self._open_release(state, account)
                name = asset_name(file_id, version_id, part)

                reused = self._reuse_existing_part(
                    target, account, file_id, version_id, name, part, parts_total
                )
                if reused is not None:
                    LOGGER.info(
                        "sync parte ya presente path=%s parte=%s/%s asset=%s",
                        rel_path, part + 1, parts_total, name,
                    )
                    part_payloads.append(reused)
                    continue

                source.seek(part * part_size)
                length = min(part_size, size - part * part_size) if size else 0
                with self._encrypted_part_file(
                    file_id=file_id,
                    version_id=version_id,
                    part=part,
                    parts=parts_total,
                    source=source,
                    length=length,
                ) as (tmp_path, info):
                    LOGGER.info(
                        "sync subiendo parte path=%s parte=%s/%s size=%sB release=%s",
                        rel_path, part + 1, parts_total, info.total_bytes, target.tag,
                    )
                    asset = self._upload_asset(state, account, target, name, tmp_path, info.total_bytes)

                uploaded_bytes += info.total_bytes
                asset_size = int(asset.get("size") or info.total_bytes)
                self.registry.upsert_asset(
                    name=name,
                    file_id=file_id,
                    version_id=version_id,
                    part=part,
                    parts=parts_total,
                    release_tag=target.tag,
                    github_asset_id=int(asset["id"]),
                    size=asset_size,
                    sha256=info.part_plaintext_sha256,
                )
                part_payloads.append(
                    {
                        "part": part,
                        "parts": parts_total,
                        "name": name,
                        "asset_id": int(asset["id"]),
                        "release_tag": target.tag,
                        "size": asset_size,
                        "plaintext_bytes": info.plaintext_bytes,
                        "part_plaintext_sha256": info.part_plaintext_sha256,
                        "nonce_b64": info.nonce_b64,
                        "repository": target.repository,
                        "repository_owner": target.owner,
                        "account_id": account.account_id,
                    }
                )
                self._releases_touched.add(target.tag)
                count = self.registry.sync_release_asset_count(target.tag)
                # Seal one short of the 1000-asset ceiling: the last slot is
                # reserved for the release's consolidated manifest.
                if count >= RELEASE_ASSET_LIMIT - 1:
                    LOGGER.info("sync release %s llena (%s assets): sellada", target.tag, count)
                    self.registry.seal_release(target.tag)

        self._record_uploaded_bytes(state, account.account_id, uploaded_bytes)
        primary_release = part_payloads[0]["release_tag"] if part_payloads else None
        return {
            "copy_index": copy_index,
            "network": "github",
            "storage": STORAGE_RELEASE,
            "version_id": version_id,
            "created_at": version_created_at,
            "file_id": file_id,
            "path": rel_path,
            "plaintext_sha256": source_sha256,
            "ciphertext_sha256": None,
            "size": size,
            "mtime_ns": mtime_ns,
            "source_sha256": source_sha256,
            "account_id": account.account_id,
            "repository_owner": account.owner,
            "repository": self._release_repositories.get(account.account_id),
            "branch": None,
            "release_tag": primary_release,
            "parts": part_payloads,
            "chunks": [],
            "encryption": {
                "algorithm": "AES-256-GCM",
                "key_id": "state-default",
                # Each part carries its own nonce in its SPDR1 header, so a part
                # is independently decryptable and verifiable.
                "per_part_nonce": True,
            },
            "manifest_path": None,
            "manifest_raw_url": None,
            "commit_sha": None,
            "uploaded_bytes": uploaded_bytes,
        }

    def _reuse_existing_part(
        self,
        target: ReleaseTarget,
        account: GitHubAccountConfig,
        file_id: str,
        version_id: str,
        name: str,
        part: int,
        parts_total: int,
    ) -> dict[str, Any] | None:
        """Resume an interrupted upload by reconciling against the remote.

        Deterministic asset names are what make this possible: we can ask
        whether the part is already there instead of re-uploading it blindly.
        Both the local row (which holds the part hash) and a non-empty remote
        asset must be present — otherwise we cannot vouch for the content.

        Scoped to this account: every copy of a version uses the same asset
        name, so an unscoped lookup would let one copy adopt another's asset.
        """
        known = self.registry.find_account_asset(
            file_id=file_id, version_id=version_id, part=part, account_id=account.account_id
        )
        if known is None or known.sha256 is None:
            return None
        remote = self._remote_asset(target, known.release_tag, name)
        if remote is None or int(remote.get("size") or 0) <= 0:
            return None
        return {
            "part": part,
            "parts": parts_total,
            "name": name,
            "asset_id": int(remote["id"]),
            "release_tag": known.release_tag,
            "size": int(remote.get("size") or 0),
            "plaintext_bytes": None,
            "part_plaintext_sha256": known.sha256,
            "nonce_b64": None,
            "repository": target.repository,
            "repository_owner": target.owner,
            "account_id": target.account_id,
        }

    def _upload_asset(
        self,
        state: dict[str, Any],
        account: GitHubAccountConfig,
        target: ReleaseTarget,
        name: str,
        path: Path,
        size: int,
    ) -> dict[str, Any]:
        client = self._client_for_account(account.account_id)
        last_error = "desconocido"
        for _ in range(3):
            try:
                with path.open("rb") as body:
                    asset = client.upload_release_asset(
                        target.owner, target.repository, target.release_id, name, body, size
                    )
            except GitHubError as exc:
                if self._handle_account_access_error(state, account, exc):
                    raise ServiceError(
                        f"La cuenta {account.account_id} ha sido retirada del pool activo "
                        "por permisos insuficientes."
                    ) from exc
                raise

            if asset is None:
                # Documented 422: the name is already taken. Deterministic names
                # make this reachable on resume, so delete and re-upload.
                last_error = "nombre duplicado (HTTP 422)"
                self._delete_remote_asset(client, target, name)
                continue

            if int(asset.get("size") or 0) <= 0:
                # A 502 can leave an empty asset behind; the returned size is
                # the only way to notice.
                last_error = "el asset quedo vacio tras la subida"
                LOGGER.warning("sync asset vacio tras subir %s: se elimina y se reintenta", name)
                try:
                    client.delete_release_asset(target.owner, target.repository, int(asset["id"]))
                except GitHubError:
                    LOGGER.exception("sync no se pudo eliminar el asset vacio %s", name)
                self._remote_assets.get(target.tag, {}).pop(name, None)
                continue

            self._remote_assets.setdefault(target.tag, {})[name] = asset
            return asset

        raise ServiceError(f"No se pudo subir el asset {name} a {target.tag}: {last_error}")

    def _delete_remote_asset(self, client: GitHubClient, target: ReleaseTarget, name: str) -> None:
        cached = self._remote_assets.setdefault(target.tag, {})
        asset = cached.get(name)
        if asset is None:
            try:
                for item in client.list_release_assets(target.owner, target.repository, target.release_id):
                    cached[item["name"]] = item
            except GitHubError:
                LOGGER.exception("sync no se pudo listar assets de %s", target.tag)
                return
            asset = cached.get(name)
        if asset is None:
            return
        try:
            client.delete_release_asset(target.owner, target.repository, int(asset["id"]))
        except GitHubError:
            LOGGER.exception("sync no se pudo eliminar el asset duplicado %s", name)
            return
        cached.pop(name, None)
        self.registry.delete_asset(target.tag, name)

    @contextmanager
    def _encrypted_part_file(
        self,
        *,
        file_id: str,
        version_id: str,
        part: int,
        parts: int,
        source: Any = None,
        length: int | None = None,
        payload: bytes | None = None,
    ):
        """Encrypt one part to a temp file and hand back its path.

        The file descriptor becomes the request body, so neither the plaintext
        nor the ciphertext is ever fully in memory. Reading the whole file and
        encrypting it in one shot peaked at ~2x the file size, which does not
        work with ~1 GiB parts.
        """
        tmp_dir = self.config.app_state_dir / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        handle, tmp_name = tempfile.mkstemp(dir=tmp_dir, suffix=".spdr")
        os.close(handle)
        tmp_path = Path(tmp_name)
        try:
            info = write_part(
                destination=tmp_path,
                key=self.secrets.encryption_key_bytes(),
                file_id=file_id,
                version_id=version_id,
                part=part,
                parts=parts,
                source=source,
                length=length,
                payload=payload,
            )
            yield tmp_path, info
        finally:
            tmp_path.unlink(missing_ok=True)

    def _finalize_sync(self, state: dict[str, Any]) -> None:
        for tag in sorted(self._releases_touched):
            try:
                self._write_release_manifest(state, tag)
            except Exception:  # noqa: BLE001 - a manifest failure must not fail the sync
                LOGGER.exception("sync no se pudo escribir el manifiesto de la release %s", tag)
        self._releases_touched.clear()

    def _write_release_manifest(self, state: dict[str, Any], tag: str) -> None:
        """Rewrite a release's consolidated manifest: ~1 request per sync.

        This is the only place the `file_id -> rel_path` mapping exists
        remotely, which is what lets the data be reconstructed after a total
        loss of local state. It is encrypted with the same key as the parts.
        """
        release = self.registry.get_release(tag)
        if release is None or release.release_id is None:
            return
        account = self.account_by_id.get(release.account_id)
        if account is None:
            return

        entries: dict[str, dict[str, Any]] = {}
        for asset in self.registry.assets_for_release(tag):
            entry = entries.setdefault(
                asset.file_id,
                {"version_id": asset.version_id, "parts": asset.parts, "assets": []},
            )
            entry["assets"].append(
                {
                    "name": asset.name,
                    "part": asset.part,
                    "size": asset.size,
                    "part_plaintext_sha256": asset.sha256,
                }
            )
        for file_id, entry in entries.items():
            row = self.registry.get_file(file_id)
            if row is not None:
                entry["path"] = row.rel_path
                entry["size"] = row.size
                entry["source_sha256"] = row.source_sha256
            # Dedup means several paths can share one version; all of them have
            # to be listed or a restore from the manifest alone would lose them.
            entry["paths"] = self.registry.paths_for_version(entry["version_id"])
            entry["assets"].sort(key=lambda item: item["part"])

        payload = json.dumps(
            {
                "version": 1,
                "release_tag": tag,
                "generated_at": utc_now_iso(),
                "files": entries,
            },
            ensure_ascii=False,
        ).encode("utf-8")

        target = ReleaseTarget(
            account.account_id, release.owner, release.repo, tag, int(release.release_id)
        )
        client = self._client_for_account(account.account_id)
        self._delete_remote_asset(client, target, MANIFEST_ASSET_NAME)
        with self._encrypted_part_file(
            file_id="manifest",
            version_id=tag,
            part=0,
            parts=1,
            payload=payload,
        ) as (tmp_path, info):
            self._upload_asset(state, account, target, MANIFEST_ASSET_NAME, tmp_path, info.total_bytes)
        LOGGER.info("sync manifiesto consolidado escrito release=%s archivos=%s", tag, len(entries))

    def _remote_asset_index(self) -> dict[str, dict[str, int]]:
        """Presence and size of every remote asset, one listing per release.

        Listings are reads: they do not touch the content-generating budget.
        """
        index: dict[str, dict[str, int]] = {}
        for release in self.registry.list_releases():
            if release.release_id is None or release.account_id not in self.github_clients:
                continue
            client = self._client_for_account(release.account_id)
            try:
                assets = client.list_release_assets(
                    release.owner, release.repo, int(release.release_id)
                )
            except GitHubError as exc:
                LOGGER.warning("verify no se pudieron listar los assets de %s: %s", release.tag, exc)
                continue
            index[release.tag] = {item["name"]: int(item.get("size") or 0) for item in assets}
        return index

    def _verify_release_copy(
        self,
        rel_path: str,
        copy: dict[str, Any],
        local_sha: str | None,
        *,
        deep: bool,
        remote_assets: dict[str, dict[str, int]],
    ) -> dict[str, Any]:
        copy_index = copy.get("copy_index")
        account_id = copy.get("account_id")
        parts = sorted(copy.get("parts", []), key=lambda item: item["part"])
        if not parts:
            raise ServiceError(f"La copia {copy_index} de {rel_path} no declara ninguna parte.")

        # Metadata tier: presence and size of every part. Catches assets that
        # are missing outright and assets that were truncated.
        problems: list[str] = []
        for part in parts:
            remote_size = (remote_assets.get(part["release_tag"]) or {}).get(part["name"])
            if remote_size is None:
                problems.append(f"{part['name']} ausente")
            elif int(remote_size) != int(part.get("size") or 0):
                problems.append(f"{part['name']} mide {remote_size}B y deberia medir {part.get('size')}B")
        if problems:
            raise ServiceError(
                f"Assets inconsistentes para {rel_path} (copia {copy_index}): {'; '.join(problems)}"
            )

        if not deep or local_sha is None:
            return {
                "ok": True,
                "copy_index": copy_index,
                "network": "github",
                "storage": STORAGE_RELEASE,
                "depth": "metadata",
                "account_id": account_id,
                "repository": copy.get("repository"),
                "parts_checked": len(parts),
            }

        client = self._client_for_account(account_id)
        whole_hasher = hashlib.sha256()
        for part in parts:
            release = self.registry.get_release(part["release_tag"])
            owner = release.owner if release else copy.get("repository_owner")
            repo = release.repo if release else copy.get("repository")
            header, blocks, digest = iter_decrypted_part(
                client.stream_release_asset(owner, repo, int(part["asset_id"])),
                self.secrets.encryption_key_bytes(),
            )
            if int(header.get("part", -1)) != int(part["part"]):
                raise ServiceError(
                    f"Parte descolocada en {rel_path}: la cabecera dice {header.get('part')} "
                    f"y se esperaba {part['part']}"
                )
            for block in blocks:
                whole_hasher.update(block)
            expected = part.get("part_plaintext_sha256")
            if expected and digest.digest != expected:
                raise ServiceError(
                    f"Parte corrupta: {rel_path}#{part['part']} (copia {copy_index})"
                )

        remote_sha = whole_hasher.hexdigest()
        if remote_sha != local_sha:
            raise ServiceError(
                f"Hash distinto para {rel_path} (copia {copy_index}): local={local_sha} remoto={remote_sha}"
            )
        return {
            "ok": True,
            "copy_index": copy_index,
            "network": "github",
            "storage": STORAGE_RELEASE,
            "depth": "deep",
            "account_id": account_id,
            "repository": copy.get("repository"),
            "remote_sha256": remote_sha,
            "chunks_checked": len(parts),
            "parts_checked": len(parts),
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
        version_id: str | None = None,
    ) -> dict[str, Any]:
        base_version = self._normalize_version(resume_version) if resume_version else None
        if base_version:
            version_id = base_version["version_id"]
            version_created_at = base_version.get("created_at", utc_now_iso())
            copy_count = int(base_version.get("copy_count_requested", self.config.copy_count))
            used_account_ids = {
                copy.get("account_id") for copy in base_version.get("copies", []) if copy.get("account_id")
            }
            copies: list[dict[str, Any]] = [deepcopy(copy) for copy in base_version.get("copies", [])]
            copy_errors: list[dict[str, Any]] = list(base_version.get("copy_errors", []))
        else:
            version_id = version_id or utc_now_compact()
            version_created_at = utc_now_iso()
            copy_count = self.config.copy_count
            used_account_ids = set()
            copies = []
            copy_errors = []

        estimated_upload_bytes = self._estimate_encrypted_size(size)

        # Phase 1: GitHub copies, streamed straight into release assets.
        while len(copies) < copy_count:
            try:
                account = self._allocate_release_account(
                    state, estimated_upload_bytes, excluded_account_ids=used_account_ids
                )
            except NoAvailableAccountsError:
                break
            try:
                copy = self._upload_release_copy(
                    state,
                    file_id=file_id,
                    rel_path=rel_path,
                    file_path=file_path,
                    size=size,
                    mtime_ns=mtime_ns,
                    source_sha256=source_sha256,
                    version_id=version_id,
                    version_created_at=version_created_at,
                    account=account,
                    copy_index=len(copies) + 1,
                    copy_count=copy_count,
                )
                copies.append(copy)
            except NoAvailableAccountsError:
                break
            except (ServiceError, GitHubError) as exc:
                LOGGER.warning(
                    "sync copia GitHub fallida path=%s cuenta=%s: %s", rel_path, account.account_id, exc
                )
                copy_errors.append(
                    {
                        "copy_index": len(copies) + 1,
                        "account_id": account.account_id,
                        "network": "github",
                        "error": str(exc),
                    }
                )
            used_account_ids.add(account.account_id)

        # Phase 2: Telegram copies. Out of scope for the Releases work, so this
        # path is unchanged — including the fact that it materialises the file
        # in memory under a single nonce.
        remaining_telegram_accounts = [
            acc for acc in self.config.telegram_accounts if acc.account_id not in used_account_ids
        ]
        if remaining_telegram_accounts and len(copies) < copy_count:
            encrypted = encrypt_bytes(file_path.read_bytes(), self.secrets.encryption_key_bytes())
            chunk_items = list(chunk_bytes(encrypted["ciphertext"], self.config.github_chunk_size_bytes))
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
            # Report the real reason: this used to always blame the GitHub quota
            # even when the actual failure was a Telegram copy.
            error_details = "; ".join(
                f"{err.get('network')}/{err.get('account_id')}: {err.get('error')}" for err in copy_errors
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
            "version": 3,
            "file_id": file_id,
            "path": rel_path,
            "version_id": version_id,
            "created_at": version_created_at,
            "storage": primary_copy.get("storage", STORAGE_RELEASE),
            "plaintext_sha256": primary_copy.get("plaintext_sha256", source_sha256),
            "ciphertext_sha256": primary_copy.get("ciphertext_sha256"),
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
            "repository_owner": primary_copy.get("repository_owner"),
            "repository": primary_copy.get("repository"),
            "branch": primary_copy.get("branch"),
            "manifest_path": primary_copy.get("manifest_path"),
            "manifest_raw_url": primary_copy.get("manifest_raw_url"),
            "commit_sha": primary_copy.get("commit_sha"),
            "encryption": primary_copy.get("encryption", {}),
            "chunks": primary_copy.get("chunks", []),
            "uploaded_bytes": sum(int(copy.get("uploaded_bytes", 0)) for copy in copies),
        }
        return self._normalize_version(version)

    def _estimate_encrypted_size(self, size: int) -> int:
        parts = max(1, math.ceil(size / self.config.github_part_size_bytes)) if size else 1
        return size + parts * (PREFIX_SIZE + HEADER_RESERVED + GCM_TAG_BYTES)

    def _allocate_release_account(
        self,
        state: dict[str, Any],
        estimated_upload_bytes: int,
        excluded_account_ids: set[str] | None = None,
    ) -> GitHubAccountConfig:
        """Pick an account with daily quota left.

        Repository size is no longer part of this decision: releases do not
        count against it, which is what removes GITHUB_REPOSITORY_MAX_SIZE_KB
        and the per-file repository rollover from the write path.
        """
        excluded_account_ids = excluded_account_ids or set()
        today = self._today_bucket()
        eligible: list[GitHubAccountConfig] = []

        for account in self.config.github_accounts:
            if account.account_id in excluded_account_ids:
                continue
            if account.account_id in self._runtime_unavailable_accounts:
                continue
            account_state = self._account_state(state, account.account_id, owner=account.owner)
            used_today = int(account_state["daily_uploads"].get(today, 0))
            if used_today + estimated_upload_bytes > self.config.github_account_daily_upload_limit_bytes:
                continue
            eligible.append(account)

        if not eligible:
            raise NoAvailableAccountsError(
                "Ninguna cuenta GitHub tiene cupo diario disponible para esta subida."
            )
        return self._choose(eligible)

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
