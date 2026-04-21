from __future__ import annotations

import base64
import os
import secrets
from dataclasses import dataclass
from pathlib import Path


DEFAULT_UPLOADS_PREFIX = "storage"
DEFAULT_BRANCH = "main"
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_MAX_RETRY = 3
DEFAULT_BACKOFF_SECONDS = 2
DEFAULT_CHUNK_SIZE_MB = 24
DEFAULT_INTERVAL_SECONDS = 7 * 24 * 60 * 60


class ConfigError(Exception):
    pass


def load_dotenv_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
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


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} debe ser un entero.") from exc
    if parsed <= 0:
        raise ConfigError(f"{name} debe ser mayor que 0.")
    return parsed


def _validate_repository_format(repo: str) -> None:
    if repo.count("/") != 1:
        raise ConfigError("GITHUB_REPOSITORY debe tener el formato 'owner/repo'.")
    owner, name = repo.split("/", 1)
    if not owner or not name:
        raise ConfigError("GITHUB_REPOSITORY no puede tener owner o repo vacios.")


@dataclass(frozen=True)
class AppConfig:
    github_token: str
    github_repository: str
    github_branch: str
    github_uploads_prefix: str
    github_chunk_size_mb: int
    github_timeout_seconds: int
    github_max_retry: int
    github_backoff_seconds: int
    app_data_dir: Path
    app_state_dir: Path
    app_web_host: str
    app_web_port: int
    app_sync_interval_seconds: int
    app_verify_interval_seconds: int
    app_web_pin: str | None
    app_encryption_key: str | None

    @property
    def github_chunk_size_bytes(self) -> int:
        return min(self.github_chunk_size_mb, 95) * 1024 * 1024


@dataclass(frozen=True)
class RuntimeSecrets:
    encryption_key: str
    web_pin: str
    flask_secret_key: str

    def encryption_key_bytes(self) -> bytes:
        padding = "=" * (-len(self.encryption_key) % 4)
        return base64.urlsafe_b64decode(self.encryption_key + padding)


def load_config() -> AppConfig:
    load_dotenv_file()

    github_token = os.environ.get("GITHUB_TOKEN", "").strip()
    github_repository = os.environ.get("GITHUB_REPOSITORY", "").strip()
    github_branch = os.environ.get("GITHUB_BRANCH", DEFAULT_BRANCH).strip() or DEFAULT_BRANCH
    github_uploads_prefix = (
        os.environ.get("GITHUB_UPLOADS_PREFIX", DEFAULT_UPLOADS_PREFIX).strip().strip("/")
        or DEFAULT_UPLOADS_PREFIX
    )

    if not github_token:
        raise ConfigError("Falta GITHUB_TOKEN.")
    if not github_repository:
        raise ConfigError("Falta GITHUB_REPOSITORY.")
    _validate_repository_format(github_repository)

    config = AppConfig(
        github_token=github_token,
        github_repository=github_repository,
        github_branch=github_branch,
        github_uploads_prefix=github_uploads_prefix,
        github_chunk_size_mb=_env_int("GITHUB_CHUNK_SIZE_MB", DEFAULT_CHUNK_SIZE_MB),
        github_timeout_seconds=_env_int("GITHUB_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS),
        github_max_retry=_env_int("GITHUB_MAX_RETRY", DEFAULT_MAX_RETRY),
        github_backoff_seconds=_env_int("GITHUB_BACKOFF_SECONDS", DEFAULT_BACKOFF_SECONDS),
        app_data_dir=Path(os.environ.get("APP_DATA_DIR", "/datos")).resolve(),
        app_state_dir=Path(os.environ.get("APP_STATE_DIR", "/state")).resolve(),
        app_web_host=os.environ.get("APP_WEB_HOST", "0.0.0.0").strip() or "0.0.0.0",
        app_web_port=_env_int("APP_WEB_PORT", 8080),
        app_sync_interval_seconds=_env_int("APP_SYNC_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS),
        app_verify_interval_seconds=_env_int("APP_VERIFY_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS),
        app_web_pin=os.environ.get("APP_WEB_PIN", "").strip() or None,
        app_encryption_key=os.environ.get("APP_ENCRYPTION_KEY", "").strip() or None,
    )

    config.app_state_dir.mkdir(parents=True, exist_ok=True)
    return config


def generate_pin() -> str:
    return "".join(secrets.choice("0123456789") for _ in range(8))


def generate_encryption_key() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")


def generate_flask_secret() -> str:
    return secrets.token_hex(32)

