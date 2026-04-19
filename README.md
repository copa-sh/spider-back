Sube y descarga directorios completos a un repositorio GitHub usando blobs/chunks
almacenados en un único commit por lote.

Requisitos mínimos:
  pip install requests

Variables de entorno (.env o entorno del sistema):
  GITHUB_TOKEN=ghp_...
  GITHUB_REPOSITORY=owner/repo
  GITHUB_BRANCH=main

Opcionales:
  GITHUB_UPLOADS_PREFIX=storage
  GITHUB_CHUNK_SIZE_MB=24
  GITHUB_TIMEOUT_SECONDS=300
  GITHUB_MAX_RETRY=3
  GITHUB_BACKOFF_SECONDS=2
  KEEP_DOWNLOADED_FILES=true

Uso:
  python github_storage_sync.py upload /ruta/directorio
  python github_storage_sync.py download https://raw.githubusercontent.com/owner/repo/main/storage/<batch_id>/storage.json

Formato de storage.json:
  {
    "version": 1,
    "batch_id": "...",
    "created_at": "...",
    "repo": "owner/repo",
    "branch": "main",
    "chunk_size_mb": 24,
    "files": [
      {
        "path": "subdir/file.txt",
        "size": 123,
        "sha256": "...",
        "chunks": [
          {"path": "storage/<batch_id>/subdir/file.txt/chunk_0000", "raw_url": "...", "sha256": "...", "size": 1048576}
        ]
      }
    ]
  }
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import requests

API_BASE_URL = "https://api.github.com"
RETRYABLE_HTTP_STATUS_CODES = {401, 409, 422, 429, 500, 502, 503, 504}
DEFAULT_UPLOADS_PREFIX = "storage"
DEFAULT_CHUNK_SIZE_MB = 24
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_MAX_RETRY = 3
DEFAULT_BACKOFF_SECONDS = 2
DEFAULT_BRANCH = "main"
DEFAULT_STORAGE_FILENAME = "storage.json"


class PublisherError(Exception):
    pass


# -------------------------
# .env loading (sin dependencias)
# -------------------------

def load_dotenv_file(path: str = ".env") -> None:
    """Carga variables desde un archivo .env si existe.

    Formato soportado:
      KEY=VALUE
      KEY="VALUE"
      export KEY=VALUE
      # comentarios
    """
    p = Path(path)
    if not p.exists():
        return

    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


# -------------------------
# Utilidades
# -------------------------

def _validate_repository_format(repo: str) -> None:
    if repo.count("/") != 1:
        raise PublisherError(
            "GITHUB_REPOSITORY debe tener el formato exacto 'owner/repo'."
        )
    owner, repo_name = repo.split("/", 1)
    if not owner.strip() or not repo_name.strip():
        raise PublisherError("GITHUB_REPOSITORY no puede tener owner o repo vacíos.")


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(chunk_size), b""):
            h.update(block)
    return h.hexdigest()


def _iter_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*")):
        if path.is_file():
            yield path


def _rel_path_str(root: Path, file_path: Path) -> str:
    return file_path.relative_to(root).as_posix()


def _safe_join(base: Path, rel: str) -> Path:
    candidate = (base / rel).resolve()
    base_resolved = base.resolve()
    if base_resolved not in candidate.parents and candidate != base_resolved:
        raise PublisherError(f"Ruta insegura detectada: {rel}")
    return candidate


def _request_with_retry(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout_s: int = DEFAULT_TIMEOUT_SECONDS,
    max_retry: int = DEFAULT_MAX_RETRY,
    backoff_s: int = DEFAULT_BACKOFF_SECONDS,
    **kwargs,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retry):
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers or {},
                timeout=timeout_s,
                **kwargs,
            )
            if response.status_code in (200, 201):
                return response
            if response.status_code in RETRYABLE_HTTP_STATUS_CODES and attempt < max_retry - 1:
                wait_s = backoff_s * (2**attempt)
                print(f"Reintentando tras HTTP {response.status_code} en {wait_s}s: {method} {url}", flush=True)
                time.sleep(wait_s)
                continue
            raise PublisherError(f"HTTP {response.status_code} en {method} {url}: {response.text[:500]}")
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt == max_retry - 1:
                raise PublisherError(f"Fallo de red tras {max_retry} intentos: {exc}") from exc
            wait_s = backoff_s * (2**attempt)
            print(f"Problema de conexión. Reintentando en {wait_s}s...", flush=True)
            time.sleep(wait_s)
    raise PublisherError(f"Retries exhausted: {last_exc}")


# -------------------------
# GitHub provider
# -------------------------

@dataclass
class GitHubSettings:
    token: str
    repo: str
    branch: str = DEFAULT_BRANCH
    uploads_prefix: str = DEFAULT_UPLOADS_PREFIX
    chunk_size_mb: int = DEFAULT_CHUNK_SIZE_MB
    timeout_s: int = DEFAULT_TIMEOUT_SECONDS
    max_retry: int = DEFAULT_MAX_RETRY
    backoff_s: int = DEFAULT_BACKOFF_SECONDS
    keep_downloaded_files: bool = True


class GitHubDataProvider:
    def __init__(self, settings: GitHubSettings):
        _validate_repository_format(settings.repo)
        self.settings = settings
        self.headers = {
            "Authorization": f"token {settings.token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def _url(self, path: str) -> str:
        return f"{API_BASE_URL}/repos/{self.settings.repo}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        return _request_with_retry(
            method=method,
            url=self._url(path),
            headers=self.headers,
            timeout_s=self.settings.timeout_s,
            max_retry=self.settings.max_retry,
            backoff_s=self.settings.backoff_s,
            **kwargs,
        )

    def ensure_repository_initialized(self) -> None:
        ref_url = self._url(f"git/ref/heads/{self.settings.branch}")
        response = requests.get(ref_url, headers=self.headers, timeout=30)
        if response.status_code == 200:
            return
        if response.status_code not in (404, 409):
            raise PublisherError(
                f"No se pudo inspeccionar la rama '{self.settings.branch}' (HTTP {response.status_code})."
            )

        init_path = ".github-storage-init"
        init_content = f"Initialized at {datetime.now(timezone.utc).isoformat()}\n".encode()
        payload = {
            "message": f"Initialize branch '{self.settings.branch}' for storage sync",
            "content": base64.b64encode(init_content).decode(),
            "branch": self.settings.branch,
        }
        init_response = requests.put(
            self._url(f"contents/{init_path}"),
            headers=self.headers,
            timeout=30,
            json=payload,
        )
        if init_response.status_code in (200, 201):
            print(f"Rama '{self.settings.branch}' inicializada.", flush=True)
            return
        raise PublisherError(
            f"No se pudo inicializar el repo. HTTP {init_response.status_code}: {init_response.text[:500]}"
        )

    def create_blob(self, payload: bytes) -> str:
        response = self._request(
            "POST",
            "git/blobs",
            json={"content": base64.b64encode(payload).decode(), "encoding": "base64"},
        )
        return response.json()["sha"]

    def branch_info(self) -> Tuple[Optional[str], Optional[str]]:
        response = requests.get(
            self._url(f"git/ref/heads/{self.settings.branch}"),
            headers=self.headers,
            timeout=30,
        )
        if response.status_code in (404, 409):
            return None, None
        if response.status_code != 200:
            raise PublisherError(f"No se pudo leer la rama: HTTP {response.status_code}")

        commit_sha = response.json()["object"]["sha"]
        commit_response = requests.get(
            self._url(f"git/commits/{commit_sha}"),
            headers=self.headers,
            timeout=30,
        )
        if commit_response.status_code != 200:
            raise PublisherError(f"No se pudo leer el commit: HTTP {commit_response.status_code}")
        return commit_sha, commit_response.json()["tree"]["sha"]

    def create_tree(self, entries: List[Dict], base_tree: Optional[str]) -> str:
        payload = {"tree": entries}
        if base_tree:
            payload["base_tree"] = base_tree
        response = self._request("POST", "git/trees", json=payload)
        return response.json()["sha"]

    def create_commit(self, tree_sha: str, parent_sha: Optional[str], message: str) -> str:
        payload = {"message": message, "tree": tree_sha, "parents": [parent_sha] if parent_sha else []}
        response = self._request("POST", "git/commits", json=payload)
        return response.json()["sha"]

    def update_ref(self, commit_sha: str) -> None:
        ref_path = f"git/refs/heads/{self.settings.branch}"
        response = requests.get(self._url(ref_path), headers=self.headers, timeout=30)
        if response.status_code == 200:
            self._request("PATCH", ref_path, json={"sha": commit_sha, "force": False})
        else:
            self._request(
                "POST",
                "git/refs",
                json={"ref": f"refs/heads/{self.settings.branch}", "sha": commit_sha},
            )

    def raw_url(self, path: str) -> str:
        return f"https://raw.githubusercontent.com/{self.settings.repo}/{self.settings.branch}/{path}"

    def browse_url(self, path: str) -> str:
        return f"https://github.com/{self.settings.repo}/blob/{self.settings.branch}/{path}"


# -------------------------
# Upload
# -------------------------

def _read_file_chunks(path: Path, chunk_size: int) -> Iterable[bytes]:
    with path.open("rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            yield block


def upload_directory(root_dir: Path, settings: GitHubSettings) -> Dict:
    if not root_dir.exists() or not root_dir.is_dir():
        raise PublisherError(f"El directorio no existe o no es válido: {root_dir}")

    provider = GitHubDataProvider(settings)
    provider.ensure_repository_initialized()

    chunk_size = max(1, settings.chunk_size_mb) * 1024 * 1024
    if settings.chunk_size_mb > 95:
        print("Chunk size > 95 MB no es recomendable para GitHub; usando 95 MB.", flush=True)
        chunk_size = 95 * 1024 * 1024

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    prefix = f"{settings.uploads_prefix}/{batch_id}"
    print(f"Subiendo {root_dir} a {settings.repo}:{settings.branch}", flush=True)
    print(f"Batch ID: {batch_id}", flush=True)

    tree_entries: List[Dict] = []
    storage: Dict = {
        "version": 1,
        "batch_id": batch_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo": settings.repo,
        "branch": settings.branch,
        "uploads_prefix": settings.uploads_prefix,
        "chunk_size_mb": settings.chunk_size_mb,
        "files": [],
    }

    files = list(_iter_files(root_dir))
    if not files:
        raise PublisherError(f"No se encontraron archivos dentro de: {root_dir}")

    for file_path in files:
        rel_path = _rel_path_str(root_dir, file_path)
        file_size = file_path.stat().st_size
        file_sha = _sha256_file(file_path)
        total_chunks = max(1, math.ceil(file_size / chunk_size))
        chunks_meta: List[Dict] = []

        print(f"\nArchivo: {rel_path} ({file_size} bytes, {total_chunks} chunks)", flush=True)
        chunk_index = 0
        for block in _read_file_chunks(file_path, chunk_size):
            chunk_name = f"chunk_{chunk_index:04d}"
            chunk_git_path = f"{prefix}/{quote(rel_path, safe='')}/{chunk_name}"
            chunk_sha = provider.create_blob(block)
            tree_entries.append(
                {"path": chunk_git_path, "mode": "100644", "type": "blob", "sha": chunk_sha}
            )
            chunk_meta = {
                "path": chunk_git_path,
                "raw_url": provider.raw_url(chunk_git_path),
                "sha256": _sha256_bytes(block),
                "size": len(block),
                "index": chunk_index,
            }
            chunks_meta.append(chunk_meta)
            print(f"  chunk {chunk_index + 1}/{total_chunks} -> {chunk_sha[:8]}", flush=True)
            chunk_index += 1

        storage["files"].append(
            {
                "path": rel_path,
                "size": file_size,
                "sha256": file_sha,
                "chunks": chunks_meta,
            }
        )

    storage_json = json.dumps(storage, ensure_ascii=False, indent=2).encode("utf-8")
    storage_blob_sha = provider.create_blob(storage_json)
    storage_git_path = f"{prefix}/{DEFAULT_STORAGE_FILENAME}"
    tree_entries.append(
        {"path": storage_git_path, "mode": "100644", "type": "blob", "sha": storage_blob_sha}
    )

    head_commit_sha, base_tree_sha = provider.branch_info()
    tree_sha = provider.create_tree(tree_entries, base_tree_sha)
    commit_sha = provider.create_commit(
        tree_sha=tree_sha,
        parent_sha=head_commit_sha,
        message=f"Upload directory snapshot {root_dir.name} ({batch_id})",
    )
    provider.update_ref(commit_sha)

    storage_url = provider.raw_url(storage_git_path)
    browse_storage_url = provider.browse_url(storage_git_path)

    print("\nSubida completada.", flush=True)
    print(f"storage.json: {storage_url}", flush=True)
    print(f"GitHub browse: {browse_storage_url}", flush=True)
    print(f"Commit: {commit_sha}", flush=True)

    return {
        "batch_id": batch_id,
        "storage_url": storage_url,
        "browse_storage_url": browse_storage_url,
        "commit_sha": commit_sha,
        "storage": storage,
    }


# -------------------------
# Download + verify
# -------------------------

def _fetch_bytes(url: str, headers: Optional[Dict[str, str]], timeout_s: int, max_retry: int, backoff_s: int) -> bytes:
    response = _request_with_retry(
        "GET",
        url,
        headers=headers,
        timeout_s=timeout_s,
        max_retry=max_retry,
        backoff_s=backoff_s,
    )
    return response.content


def download_from_storage(storage_url: str, output_dir: Optional[Path], settings: GitHubSettings) -> Dict:
    target_dir = (output_dir or Path.cwd() / "downloaded_storage").resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    headers: Dict[str, str] = {}
    if settings.token:
        headers["Authorization"] = f"token {settings.token}"

    storage_bytes = _fetch_bytes(
        storage_url,
        headers=headers,
        timeout_s=settings.timeout_s,
        max_retry=settings.max_retry,
        backoff_s=settings.backoff_s,
    )
    try:
        storage = json.loads(storage_bytes.decode("utf-8"))
    except Exception as exc:
        raise PublisherError("storage.json debe ser JSON UTF-8 válido.") from exc

    if storage.get("version") != 1:
        raise PublisherError(f"Versión no soportada en storage.json: {storage.get('version')}")

    files = storage.get("files", [])
    if not files:
        raise PublisherError("storage.json no contiene archivos.")

    manifest_base_dir = Path(storage_url.split("/storage.json", 1)[0])
    downloaded = []

    for entry in files:
        rel_path = entry["path"]
        expected_file_hash = entry["sha256"]
        chunks = entry.get("chunks", [])
        if not chunks:
            raise PublisherError(f"El archivo {rel_path} no tiene chunks.")

        final_path = _safe_join(target_dir, rel_path)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        temp_path = final_path.with_suffix(final_path.suffix + ".part")
        if temp_path.exists():
            temp_path.unlink()

        with temp_path.open("wb") as dest:
            for idx, chunk in enumerate(chunks):
                chunk_url = chunk.get("raw_url")
                if not chunk_url:
                    # fallback: reconstruir si no existe raw_url
                    chunk_path = chunk.get("path")
                    if not chunk_path:
                        raise PublisherError(f"Chunk sin raw_url ni path para {rel_path}")
                    chunk_url = f"{manifest_base_dir}/{chunk_path}"

                data = _fetch_bytes(
                    chunk_url,
                    headers=headers,
                    timeout_s=settings.timeout_s,
                    max_retry=settings.max_retry,
                    backoff_s=settings.backoff_s,
                )
                if _sha256_bytes(data) != chunk.get("sha256"):
                    raise PublisherError(
                        f"Hash inválido en chunk {idx} del archivo {rel_path}."
                    )
                dest.write(data)
                print(f"Descargado chunk {idx + 1}/{len(chunks)} de {rel_path}", flush=True)

        downloaded_hash = _sha256_file(temp_path)
        if downloaded_hash != expected_file_hash:
            temp_path.unlink(missing_ok=True)
            raise PublisherError(
                f"Integridad fallida para {rel_path}: esperado {expected_file_hash}, obtenido {downloaded_hash}"
            )

        temp_path.rename(final_path)
        downloaded.append(str(final_path))
        print(f"Verificado: {rel_path}", flush=True)

    if not settings.keep_downloaded_files:
        # En este modo no borramos automáticamente porque el objetivo es recuperar archivos.
        # Se deja el flag disponible para integrarlo en flujos más avanzados.
        pass

    print("\nDescarga e integridad completadas correctamente.", flush=True)
    return {"storage": storage, "output_dir": str(target_dir), "downloaded_files": downloaded}


# -------------------------
# CLI
# -------------------------

def _read_settings_from_env() -> GitHubSettings:
    load_dotenv_file()

    token = os.environ.get("GITHUB_TOKEN", "").strip()
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    branch = os.environ.get("GITHUB_BRANCH", DEFAULT_BRANCH).strip() or DEFAULT_BRANCH
    uploads_prefix = os.environ.get("GITHUB_UPLOADS_PREFIX", DEFAULT_UPLOADS_PREFIX).strip().strip("/") or DEFAULT_UPLOADS_PREFIX
    chunk_size_mb = int(os.environ.get("GITHUB_CHUNK_SIZE_MB", str(DEFAULT_CHUNK_SIZE_MB)))
    timeout_s = int(os.environ.get("GITHUB_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS)))
    max_retry = int(os.environ.get("GITHUB_MAX_RETRY", str(DEFAULT_MAX_RETRY)))
    backoff_s = int(os.environ.get("GITHUB_BACKOFF_SECONDS", str(DEFAULT_BACKOFF_SECONDS)))
    keep_downloaded_files = _bool_env("KEEP_DOWNLOADED_FILES", True)

    if not token:
        raise PublisherError("Falta GITHUB_TOKEN en .env o en el entorno.")
    if not repo:
        raise PublisherError("Falta GITHUB_REPOSITORY en .env o en el entorno.")

    return GitHubSettings(
        token=token,
        repo=repo,
        branch=branch,
        uploads_prefix=uploads_prefix,
        chunk_size_mb=chunk_size_mb,
        timeout_s=timeout_s,
        max_retry=max_retry,
        backoff_s=backoff_s,
        keep_downloaded_files=keep_downloaded_files,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sube y descarga directorios a GitHub usando storage.json.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_upload = sub.add_parser("upload", help="Sube un directorio completo")
    p_upload.add_argument("directory", type=Path, help="Directorio a subir")

    p_download = sub.add_parser("download", help="Descarga desde un storage.json")
    p_download.add_argument("storage_url", help="URL raw de storage.json")
    p_download.add_argument("--output-dir", type=Path, default=None, help="Directorio destino")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        settings = _read_settings_from_env()

        if args.command == "upload":
            result = upload_directory(args.directory.resolve(), settings)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        if args.command == "download":
            result = download_from_storage(args.storage_url, args.output_dir, settings)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0

        parser.error("Comando desconocido")
        return 2

    except PublisherError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrumpido por el usuario.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
