from __future__ import annotations

import argparse
from pathlib import Path

from github_fs.state_migrations import migrate_index_file


def main() -> int:
    parser = argparse.ArgumentParser(description="Migra index.json para network y copies")
    parser.add_argument("index_path", type=Path, help="Ruta al index.json a migrar")
    args = parser.parse_args()

    migrated = migrate_index_file(args.index_path)
    print(
        "Migrado correctamente: "
        f"{args.index_path} "
        f"(files={len(migrated.get('files', {}))}, accounts={len(migrated.get('github_accounts', {}))})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
