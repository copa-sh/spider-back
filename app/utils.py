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

SYNC_ORDER_SPREAD = "spread"
SYNC_ORDER_PATH = "path"
SYNC_ORDERS = (SYNC_ORDER_SPREAD, SYNC_ORDER_PATH)


def iter_files(root: Path, *, order: str = SYNC_ORDER_SPREAD) -> Iterable[Path]:
    """Walk ``root`` in one of two deterministic orders.

    ``spread`` (default) visits files ordered by ``stable_file_id``, i.e. by a
    hash of the relative path. That distributes the walk uniformly over the whole
    tree, so a sync that is interrupted half way through leaves a bit of
    everything backed up rather than only the alphabetically-first region — which
    on a date-organised corpus means only the oldest files. This is the property
    the previous ``random.shuffle`` provided, kept here without its two costs: the
    order is now stable across runs, so progress is predictable and reproducible.

    ``path`` visits files in directory order and streams, holding nothing but the
    current directory's entries. It has the best locality and the lowest memory,
    at the price of concentrating partial coverage in one region of the tree.

    Both orders are deterministic. The previous implementation reshuffled on every
    run, which made an interrupted sync unreproducible.
    """
    if order == SYNC_ORDER_PATH:
        yield from _iter_files(root)
        return
    if order != SYNC_ORDER_SPREAD:
        raise ValueError(f"Orden de escaneo desconocido: {order!r}")

    # A globally uniform order cannot be produced while streaming: the whole set
    # of paths has to be known before it can be ordered. Only the relative paths
    # are held (not Path objects), which is a few MB for 100k files.
    relative_paths = [rel_path_str(root, path) for path in _iter_files(root)]
    relative_paths.sort(key=stable_file_id)
    for rel_path in relative_paths:
        yield root / rel_path


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
