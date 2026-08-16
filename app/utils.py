from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def parse_iso_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def add_seconds_iso(value: str, seconds: int) -> str:
    return (parse_iso_datetime(value) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(chunk_size), b""):
            digest.update(block)
    return digest.hexdigest()

def iter_files(root: Path) -> Iterable[Path]:
    """Walk ``root`` streaming, in a deterministic order.

    The previous implementation materialised the whole tree into a list and
    shuffled it. That cost memory proportional to the file count and destroyed
    both locality and resumability: an interrupted sync restarted in a different
    order and could not pick up where it left off. Sorting each directory's
    entries keeps the walk stable across runs and lets the caller stream.
    """
    yield from _iter_files(root)


def _iter_files(directory: Path) -> Iterable[Path]:
    try:
        entries = sorted(directory.iterdir(), key=lambda path: path.name)
    except (NotADirectoryError, PermissionError, FileNotFoundError):
        return
    for path in entries:
        if path.is_symlink() and path.is_dir():
            # Don't follow directory symlinks: rglob didn't either, and a cycle
            # would make the walk non-terminating.
            continue
        if path.is_dir():
            yield from _iter_files(path)
        elif path.is_file():
            yield path


def rel_path_str(root: Path, file_path: Path) -> str:
    return file_path.relative_to(root).as_posix()


def stable_file_id(rel_path: str) -> str:
    return hashlib.sha256(rel_path.encode("utf-8")).hexdigest()[:16]
