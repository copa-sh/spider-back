"""Local sync registry backed by SQLite.

Before this module the whole file map lived in ``index.json``, which was
rewritten *in full* on every save (every 10 files during a sync) and parsed plus
deep-copied on every web page load. With tens of thousands of files that
dominated startup time, and the cost grew with the total volume rather than with
the number of changes.

The registry replaces that with indexed, row-level access:

* ``files``    — one row per discovered path, keyed by ``file_id``. The
  ``(size, mtime_ns, status)`` triple is what makes an unchanged file skippable
  without hashing it.
* ``versions`` — the uploaded version documents, so the history the web UI shows
  and the copy metadata legacy verification needs both survive.
* ``assets``   — one row per uploaded release asset, the unit of remote storage.
* ``releases`` — the release each asset lives in, with its asset count so
  rollover at 1000 assets is a local decision.
* ``uploaded_versions`` — the pre-existing content-hash dedup table, kept as-is.

``index.json`` keeps only config, tasks and accounts.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

try:
    import pysqlite3 as sqlite3
except ImportError:  # pragma: no cover - fallback for local environments
    import sqlite3

from .utils import utc_now_iso


STATUS_PENDING = "pending"
STATUS_UPLOADING = "uploading"
STATUS_COMPLETE = "complete"
STATUS_ERROR = "error"

STORAGE_BLOB = "blob"
STORAGE_RELEASE = "release"

# GitHub allows up to 1000 assets per release.
RELEASE_ASSET_LIMIT = 1000


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS files (
        file_id TEXT PRIMARY KEY,
        rel_path TEXT NOT NULL,
        size INTEGER NOT NULL,
        mtime_ns INTEGER NOT NULL,
        source_sha256 TEXT,
        version_id TEXT,
        status TEXT NOT NULL,
        present INTEGER NOT NULL DEFAULT 1,
        last_seen_at TEXT,
        last_seen_run INTEGER,
        last_error TEXT,
        last_verified_at TEXT,
        last_verification_ok INTEGER,
        last_verified_version_id TEXT,
        last_verification_json TEXT,
        updated_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS versions (
        file_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        version_json TEXT NOT NULL,
        created_at TEXT,
        copy_count INTEGER NOT NULL DEFAULT 0,
        distinct_account_copies INTEGER NOT NULL DEFAULT 0,
        storage TEXT NOT NULL DEFAULT 'blob',
        PRIMARY KEY (file_id, version_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assets (
        name TEXT NOT NULL,
        file_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        part INTEGER NOT NULL,
        parts INTEGER NOT NULL,
        release_tag TEXT NOT NULL,
        github_asset_id INTEGER,
        size INTEGER,
        sha256 TEXT,
        uploaded_at TEXT,
        -- Keyed on (release_tag, name), not name alone: with COPY_COUNT > 1 the
        -- same deterministic part name exists once per account, and collapsing
        -- them would let one copy adopt another account's asset.
        PRIMARY KEY (release_tag, name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS releases (
        tag TEXT PRIMARY KEY,
        account_id TEXT,
        owner TEXT,
        repo TEXT,
        release_id INTEGER,
        asset_count INTEGER NOT NULL DEFAULT 0,
        sealed INTEGER NOT NULL DEFAULT 0,
        created_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS uploaded_versions (
        source_sha256 TEXT PRIMARY KEY,
        version_json TEXT NOT NULL,
        first_uploaded_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL,
        copy_count INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_files_status ON files(status)",
    "CREATE INDEX IF NOT EXISTS idx_files_present ON files(present)",
    "CREATE INDEX IF NOT EXISTS idx_files_path ON files(rel_path)",
    "CREATE INDEX IF NOT EXISTS idx_assets_file ON assets(file_id)",
    "CREATE INDEX IF NOT EXISTS idx_assets_release ON assets(release_tag)",
    "CREATE INDEX IF NOT EXISTS idx_versions_file ON versions(file_id)",
)


@dataclass(frozen=True)
class FileRow:
    file_id: str
    rel_path: str
    size: int
    mtime_ns: int
    source_sha256: str | None
    version_id: str | None
    status: str
    present: bool
    last_seen_at: str | None
    last_error: str | None
    last_verified_at: str | None
    last_verification_ok: bool | None
    last_verified_version_id: str | None

    @property
    def is_complete(self) -> bool:
        return self.status == STATUS_COMPLETE


@dataclass(frozen=True)
class AssetRow:
    name: str
    file_id: str
    version_id: str
    part: int
    parts: int
    release_tag: str
    github_asset_id: int | None
    size: int | None
    sha256: str | None
    uploaded_at: str | None


@dataclass(frozen=True)
class ReleaseRow:
    tag: str
    account_id: str | None
    owner: str | None
    repo: str | None
    release_id: int | None
    asset_count: int
    sealed: bool
    created_at: str | None

    @property
    def is_full(self) -> bool:
        return self.asset_count >= RELEASE_ASSET_LIMIT


class Registry:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # One connection per thread: the sync task and the web workers touch the
        # registry concurrently, and re-opening per operation would reintroduce
        # a per-file cost we are here to remove.
        self._local = threading.local()
        self._write_lock = threading.RLock()
        self._initialize()

    # ── connection handling ─────────────────────────────────────────────────

    @property
    def connection(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    def _initialize(self) -> None:
        with self._write_lock:
            conn = self.connection
            for statement in SCHEMA_STATEMENTS:
                conn.execute(statement)
            conn.commit()

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def _execute(self, sql: str, params: tuple = ()):  # write helper
        with self._write_lock:
            conn = self.connection
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor

    # ── meta ────────────────────────────────────────────────────────────────

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        self._execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    def next_counter(self, key: str) -> int:
        """Increment and return a counter, used to rotate deep verification."""
        with self._write_lock:
            current = int(self.get_meta(key, "0") or 0)
            nxt = current + 1
            self.set_meta(key, str(nxt))
            return nxt

    def counter(self, key: str) -> int:
        return int(self.get_meta(key, "0") or 0)

    # ── files ───────────────────────────────────────────────────────────────

    def get_file(self, file_id: str) -> FileRow | None:
        row = self.connection.execute("SELECT * FROM files WHERE file_id = ?", (file_id,)).fetchone()
        return _file_row(row) if row else None

    def upsert_file(
        self,
        *,
        file_id: str,
        rel_path: str,
        size: int,
        mtime_ns: int,
        status: str,
        source_sha256: str | None = None,
        version_id: str | None = None,
        present: bool = True,
        last_error: str | None = None,
        last_seen_at: str | None = None,
        seen_run: int | None = None,
    ) -> None:
        now = utc_now_iso()
        self._execute(
            """
            INSERT INTO files (
                file_id, rel_path, size, mtime_ns, source_sha256, version_id,
                status, present, last_seen_at, last_seen_run, last_error, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_id) DO UPDATE SET
                rel_path = excluded.rel_path,
                size = excluded.size,
                mtime_ns = excluded.mtime_ns,
                source_sha256 = COALESCE(excluded.source_sha256, files.source_sha256),
                version_id = COALESCE(excluded.version_id, files.version_id),
                status = excluded.status,
                present = excluded.present,
                last_seen_at = excluded.last_seen_at,
                last_seen_run = COALESCE(excluded.last_seen_run, files.last_seen_run),
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (
                file_id,
                rel_path,
                int(size),
                int(mtime_ns),
                source_sha256,
                version_id,
                status,
                1 if present else 0,
                last_seen_at or now,
                seen_run,
                last_error,
                now,
            ),
        )

    def touch_file(self, file_id: str, *, size: int, mtime_ns: int, seen_run: int | None = None) -> None:
        """Record that an unchanged file was seen, without touching its version."""
        now = utc_now_iso()
        self._execute(
            "UPDATE files SET size = ?, mtime_ns = ?, present = 1, last_seen_at = ?, "
            "last_seen_run = COALESCE(?, last_seen_run), updated_at = ? WHERE file_id = ?",
            (int(size), int(mtime_ns), now, seen_run, now, file_id),
        )

    def set_status(self, file_id: str, status: str, *, last_error: str | None = None) -> None:
        self._execute(
            "UPDATE files SET status = ?, last_error = ?, updated_at = ? WHERE file_id = ?",
            (status, last_error, utc_now_iso(), file_id),
        )

    def mark_absent_except_run(self, run: int) -> int:
        """Flag rows this run's walk did not reach.

        Keyed on the run counter rather than a timestamp: ``last_seen_at`` has
        one-second resolution, so two runs inside the same second would leave
        deleted files marked present. Only safe to call once the walk completed
        — an aborted walk would flag everything it had not reached yet.
        """
        cursor = self._execute(
            "UPDATE files SET present = 0, updated_at = ? "
            "WHERE present = 1 AND (last_seen_run IS NULL OR last_seen_run != ?)",
            (utc_now_iso(), int(run)),
        )
        return cursor.rowcount

    def record_verification(
        self,
        file_id: str,
        *,
        ok: bool,
        version_id: str | None,
        detail: dict[str, Any],
        last_error: str | None = None,
    ) -> None:
        now = utc_now_iso()
        self._execute(
            "UPDATE files SET last_verified_at = ?, last_verification_ok = ?, "
            "last_verified_version_id = ?, last_verification_json = ?, last_error = ?, updated_at = ? "
            "WHERE file_id = ?",
            (
                now,
                1 if ok else 0,
                version_id,
                json.dumps(detail, ensure_ascii=False),
                last_error,
                now,
                file_id,
            ),
        )

    def clear_verification(self, file_id: str) -> None:
        """Drop a stored verification.

        Called when a new active version lands: a verification of the previous
        version says nothing about the new one, and leaving it would let the UI
        report stale data as fresh.
        """
        self._execute(
            "UPDATE files SET last_verified_at = NULL, last_verification_ok = NULL, "
            "last_verified_version_id = NULL, last_verification_json = NULL, updated_at = ? "
            "WHERE file_id = ?",
            (utc_now_iso(), file_id),
        )

    def mark_missing(self, file_id: str, *, error: str | None = None) -> None:
        self._execute(
            "UPDATE files SET present = 0, last_error = ?, updated_at = ? WHERE file_id = ?",
            (error, utc_now_iso(), file_id),
        )

    def iter_files(
        self, *, present_only: bool = False, status: str | None = None
    ) -> Iterator[FileRow]:
        sql = "SELECT * FROM files"
        clauses = []
        params: list[Any] = []
        if present_only:
            clauses.append("present = 1")
        if status:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY rel_path"
        for row in self.connection.execute(sql, tuple(params)):
            yield _file_row(row)

    def list_files(self, *, limit: int, offset: int = 0) -> list[FileRow]:
        rows = self.connection.execute(
            "SELECT * FROM files ORDER BY rel_path LIMIT ? OFFSET ?", (int(limit), int(offset))
        ).fetchall()
        return [_file_row(row) for row in rows]

    def count_files(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) AS n FROM files").fetchone()["n"])

    def verification_detail(self, file_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT last_verification_json FROM files WHERE file_id = ?", (file_id,)
        ).fetchone()
        if not row or not row["last_verification_json"]:
            return None
        return json.loads(row["last_verification_json"])

    def stats(self, *, copy_count_target: int = 1) -> dict[str, Any]:
        """Aggregate counters for the web UI, computed in SQL.

        Deliberately *not* built by materialising every file: that is the cost
        this registry exists to remove.
        """
        conn = self.connection
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(present = 1), 0) AS present,
                COALESCE(SUM(present = 0), 0) AS absent,
                COALESCE(SUM(version_id IS NOT NULL), 0) AS uploaded,
                COALESCE(SUM(last_error IS NOT NULL AND last_error != ''), 0) AS with_error,
                COALESCE(SUM(
                    last_verification_ok = 1
                    AND last_verified_version_id IS NOT NULL
                    AND last_verified_version_id = version_id
                ), 0) AS verified
            FROM files
            """
        ).fetchone()
        versions = conn.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(copy_count), 0) AS copies FROM versions"
        ).fetchone()

        total_files = int(row["total"])
        distribution = []
        for threshold in range(1, max(1, copy_count_target) + 1):
            count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM files
                    JOIN versions ON versions.file_id = files.file_id
                                 AND versions.version_id = files.version_id
                    WHERE versions.distinct_account_copies >= ?
                    """,
                    (threshold,),
                ).fetchone()["n"]
            )
            percent = round(count / total_files * 100, 1) if total_files else 0.0
            distribution.append({"threshold": threshold, "count": count, "percent": percent})

        return {
            "total": total_files,
            "present": int(row["present"]),
            "absent": int(row["absent"]),
            "uploaded": int(row["uploaded"]),
            "with_error": int(row["with_error"]),
            "verified": int(row["verified"]),
            "total_versions": int(versions["total"]),
            "total_copies": int(versions["copies"]),
            "copy_count_target": copy_count_target,
            "copy_distribution": distribution,
        }

    # ── versions ────────────────────────────────────────────────────────────

    def save_version(
        self,
        file_id: str,
        version: dict[str, Any],
        *,
        distinct_account_copies: int,
        storage: str = STORAGE_BLOB,
    ) -> None:
        self._execute(
            """
            INSERT INTO versions (
                file_id, version_id, version_json, created_at, copy_count,
                distinct_account_copies, storage
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(file_id, version_id) DO UPDATE SET
                version_json = excluded.version_json,
                created_at = excluded.created_at,
                copy_count = excluded.copy_count,
                distinct_account_copies = excluded.distinct_account_copies,
                storage = excluded.storage
            """,
            (
                file_id,
                version["version_id"],
                json.dumps(version, ensure_ascii=False),
                version.get("created_at"),
                len(version.get("copies", [])),
                int(distinct_account_copies),
                storage,
            ),
        )

    def get_version(self, file_id: str, version_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT version_json FROM versions WHERE file_id = ? AND version_id = ?",
            (file_id, version_id),
        ).fetchone()
        return json.loads(row["version_json"]) if row else None

    def get_active_version(self, file_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT versions.version_json FROM files
            JOIN versions ON versions.file_id = files.file_id
                         AND versions.version_id = files.version_id
            WHERE files.file_id = ?
            """,
            (file_id,),
        ).fetchone()
        return json.loads(row["version_json"]) if row else None

    def list_versions(self, file_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT version_json FROM versions WHERE file_id = ? ORDER BY created_at, version_id",
            (file_id,),
        ).fetchall()
        return [json.loads(row["version_json"]) for row in rows]

    # ── assets ──────────────────────────────────────────────────────────────

    def upsert_asset(
        self,
        *,
        name: str,
        file_id: str,
        version_id: str,
        part: int,
        parts: int,
        release_tag: str,
        github_asset_id: int | None = None,
        size: int | None = None,
        sha256: str | None = None,
    ) -> None:
        self._execute(
            """
            INSERT INTO assets (
                name, file_id, version_id, part, parts, release_tag,
                github_asset_id, size, sha256, uploaded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(release_tag, name) DO UPDATE SET
                file_id = excluded.file_id,
                version_id = excluded.version_id,
                part = excluded.part,
                parts = excluded.parts,
                github_asset_id = excluded.github_asset_id,
                size = excluded.size,
                sha256 = excluded.sha256,
                uploaded_at = excluded.uploaded_at
            """,
            (
                name,
                file_id,
                version_id,
                int(part),
                int(parts),
                release_tag,
                github_asset_id,
                size,
                sha256,
                utc_now_iso(),
            ),
        )

    def get_asset(self, release_tag: str, name: str) -> AssetRow | None:
        row = self.connection.execute(
            "SELECT * FROM assets WHERE release_tag = ? AND name = ?", (release_tag, name)
        ).fetchone()
        return _asset_row(row) if row else None

    def find_account_asset(
        self, *, file_id: str, version_id: str, part: int, account_id: str
    ) -> AssetRow | None:
        """The part already uploaded for THIS account, if any.

        Scoped by account because every copy of a version uses the same
        deterministic asset name; only the release it lives in distinguishes them.
        """
        row = self.connection.execute(
            """
            SELECT assets.* FROM assets
            JOIN releases ON releases.tag = assets.release_tag
            WHERE assets.file_id = ? AND assets.version_id = ? AND assets.part = ?
              AND releases.account_id = ?
            LIMIT 1
            """,
            (file_id, version_id, int(part), account_id),
        ).fetchone()
        return _asset_row(row) if row else None

    def assets_for_version(self, file_id: str, version_id: str) -> list[AssetRow]:
        rows = self.connection.execute(
            "SELECT * FROM assets WHERE file_id = ? AND version_id = ? ORDER BY part",
            (file_id, version_id),
        ).fetchall()
        return [_asset_row(row) for row in rows]

    def assets_for_release(self, release_tag: str) -> list[AssetRow]:
        rows = self.connection.execute(
            "SELECT * FROM assets WHERE release_tag = ? ORDER BY name", (release_tag,)
        ).fetchall()
        return [_asset_row(row) for row in rows]

    def delete_asset(self, release_tag: str, name: str) -> None:
        self._execute(
            "DELETE FROM assets WHERE release_tag = ? AND name = ?", (release_tag, name)
        )

    # ── releases ────────────────────────────────────────────────────────────

    def upsert_release(
        self,
        *,
        tag: str,
        account_id: str | None,
        owner: str | None,
        repo: str | None,
        release_id: int | None,
        asset_count: int | None = None,
        sealed: bool | None = None,
    ) -> None:
        self._execute(
            """
            INSERT INTO releases (tag, account_id, owner, repo, release_id, asset_count, sealed, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tag) DO UPDATE SET
                account_id = excluded.account_id,
                owner = excluded.owner,
                repo = excluded.repo,
                release_id = COALESCE(excluded.release_id, releases.release_id),
                asset_count = COALESCE(?, releases.asset_count),
                sealed = COALESCE(?, releases.sealed)
            """,
            (
                tag,
                account_id,
                owner,
                repo,
                release_id,
                int(asset_count or 0),
                1 if sealed else 0,
                utc_now_iso(),
                None if asset_count is None else int(asset_count),
                None if sealed is None else (1 if sealed else 0),
            ),
        )

    def get_release(self, tag: str) -> ReleaseRow | None:
        row = self.connection.execute("SELECT * FROM releases WHERE tag = ?", (tag,)).fetchone()
        return _release_row(row) if row else None

    def open_release_for_account(self, account_id: str) -> ReleaseRow | None:
        row = self.connection.execute(
            "SELECT * FROM releases WHERE account_id = ? AND sealed = 0 AND asset_count < ? "
            "ORDER BY tag LIMIT 1",
            (account_id, RELEASE_ASSET_LIMIT),
        ).fetchone()
        return _release_row(row) if row else None

    def list_releases(self, account_id: str | None = None) -> list[ReleaseRow]:
        if account_id:
            rows = self.connection.execute(
                "SELECT * FROM releases WHERE account_id = ? ORDER BY tag", (account_id,)
            ).fetchall()
        else:
            rows = self.connection.execute("SELECT * FROM releases ORDER BY tag").fetchall()
        return [_release_row(row) for row in rows]

    def sync_release_asset_count(self, tag: str) -> int:
        """Recompute a release's asset count from the assets table."""
        with self._write_lock:
            conn = self.connection
            count = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM assets WHERE release_tag = ?", (tag,)
                ).fetchone()["n"]
            )
            conn.execute(
                "UPDATE releases SET asset_count = ?, sealed = CASE WHEN ? >= ? THEN 1 ELSE sealed END "
                "WHERE tag = ?",
                (count, count, RELEASE_ASSET_LIMIT, tag),
            )
            conn.commit()
            return count

    def seal_release(self, tag: str) -> None:
        self._execute("UPDATE releases SET sealed = 1 WHERE tag = ?", (tag,))

    def next_release_index(self, account_id: str) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM releases WHERE account_id = ?", (account_id,)
        ).fetchone()
        return int(row["n"]) + 1

    # ── content-hash dedup (pre-existing table, unchanged semantics) ─────────

    def lookup_uploaded_version(self, source_sha256: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT version_json FROM uploaded_versions WHERE source_sha256 = ?", (source_sha256,)
        ).fetchone()
        return json.loads(row["version_json"]) if row else None

    def record_uploaded_version(self, source_sha256: str, version: dict[str, Any]) -> None:
        now = utc_now_iso()
        self._execute(
            """
            INSERT INTO uploaded_versions (
                source_sha256, version_json, first_uploaded_at, last_seen_at, copy_count
            ) VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(source_sha256) DO UPDATE SET
                version_json = excluded.version_json,
                last_seen_at = excluded.last_seen_at
            """,
            (source_sha256, json.dumps(version, ensure_ascii=False), now, now),
        )

    def mark_uploaded_copy_seen(self, source_sha256: str) -> None:
        self._execute(
            "UPDATE uploaded_versions SET last_seen_at = ?, copy_count = copy_count + 1 "
            "WHERE source_sha256 = ?",
            (utc_now_iso(), source_sha256),
        )


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


def version_storage(version: dict[str, Any] | None) -> str:
    """Which remote layout a version uses.

    Commit-based data written before the Releases backend stays readable, so
    verification has to dispatch on this rather than assume one shape.
    """
    if not version:
        return STORAGE_BLOB
    for copy in version.get("copies", []):
        if copy.get("storage") == STORAGE_RELEASE:
            return STORAGE_RELEASE
    return version.get("storage", STORAGE_BLOB)


def import_legacy_files(files: dict[str, Any], registry: Registry) -> int:
    """Import an ``index.json`` file map into the registry.

    Lossless on purpose: the version documents are carried over verbatim so
    commit-based copies stay verifiable through their ``raw_url``.
    """
    imported = 0
    for file_id, entry in files.items():
        rel_path = entry.get("path")
        if not rel_path:
            continue
        versions = entry.get("versions", []) or []
        active_version_id = entry.get("active_version_id")
        active = next(
            (item for item in versions if item.get("version_id") == active_version_id), None
        )
        status = STATUS_COMPLETE if active else (STATUS_ERROR if entry.get("last_error") else STATUS_PENDING)
        registry.upsert_file(
            file_id=file_id,
            rel_path=rel_path,
            size=int(entry.get("size") or 0),
            mtime_ns=int(entry.get("mtime_ns") or 0),
            source_sha256=entry.get("source_sha256"),
            version_id=active_version_id,
            status=status,
            present=bool(entry.get("present", True)),
            last_error=entry.get("last_error"),
            last_seen_at=entry.get("last_seen_at"),
        )
        for version in versions:
            registry.save_version(
                file_id,
                version,
                distinct_account_copies=distinct_account_copy_count(version),
                storage=version_storage(version),
            )
        verification = entry.get("last_verification")
        if verification:
            registry.record_verification(
                file_id,
                ok=bool(verification.get("ok")),
                version_id=verification.get("version_id"),
                detail=verification,
                last_error=entry.get("last_error"),
            )
        imported += 1
    return imported


def _file_row(row) -> FileRow:
    return FileRow(
        file_id=row["file_id"],
        rel_path=row["rel_path"],
        size=int(row["size"]),
        mtime_ns=int(row["mtime_ns"]),
        source_sha256=row["source_sha256"],
        version_id=row["version_id"],
        status=row["status"],
        present=bool(row["present"]),
        last_seen_at=row["last_seen_at"],
        last_error=row["last_error"],
        last_verified_at=row["last_verified_at"],
        last_verification_ok=None if row["last_verification_ok"] is None else bool(row["last_verification_ok"]),
        last_verified_version_id=row["last_verified_version_id"],
    )


def _asset_row(row) -> AssetRow:
    return AssetRow(
        name=row["name"],
        file_id=row["file_id"],
        version_id=row["version_id"],
        part=int(row["part"]),
        parts=int(row["parts"]),
        release_tag=row["release_tag"],
        github_asset_id=row["github_asset_id"],
        size=row["size"],
        sha256=row["sha256"],
        uploaded_at=row["uploaded_at"],
    )


def _release_row(row) -> ReleaseRow:
    return ReleaseRow(
        tag=row["tag"],
        account_id=row["account_id"],
        owner=row["owner"],
        repo=row["repo"],
        release_id=row["release_id"],
        asset_count=int(row["asset_count"]),
        sealed=bool(row["sealed"]),
        created_at=row["created_at"],
    )
