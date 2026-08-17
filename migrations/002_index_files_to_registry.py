"""Move ``index.json["files"]`` into the SQLite registry.

The application performs this import automatically on the first start after the
upgrade (see ``AppService._import_legacy_file_map``); this script exists to run
it explicitly — for a dry run before a deploy, or to migrate a state directory
that is not the one the service will boot with.

Usage:
    python migrations/002_index_files_to_registry.py /state
    python migrations/002_index_files_to_registry.py /state --dry-run
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.registry import Registry, import_legacy_files
from app.state_migrations import migrate_state


LEGACY_IMPORT_FLAG = "index_json_files_migrated"


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa index.json['files'] al registro SQLite")
    parser.add_argument("state_dir", type=Path, help="Directorio de estado (APP_STATE_DIR)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Cuenta lo que se importaria sin escribir nada",
    )
    args = parser.parse_args()

    index_path = args.state_dir / "index.json"
    if not index_path.exists():
        print(f"No existe {index_path}: nada que migrar.")
        return 0

    state = migrate_state(json.loads(index_path.read_text(encoding="utf-8")))
    legacy_files = state.get("files") or {}
    if args.dry_run:
        versions = sum(len(entry.get("versions", [])) for entry in legacy_files.values())
        print(f"Se importarian {len(legacy_files)} archivos y {versions} versiones.")
        return 0

    registry = Registry(args.state_dir / "upload_index.sqlite3")
    imported = import_legacy_files(legacy_files, registry)
    registry.set_meta(LEGACY_IMPORT_FLAG, "1")

    # index.json keeps only config, tasks and accounts from here on.
    state.pop("files", None)
    tmp_path = index_path.with_name(f"{index_path.name}.tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(index_path)

    print(f"Migrados {imported} archivos al registro SQLite. index.json ya no contiene 'files'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
