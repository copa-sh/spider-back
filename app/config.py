from __future__ import annotations

import base64
import os
import re
import secrets
from dataclasses import dataclass
from pathlib import Path


DEFAULT_UPLOADS_PREFIX = "storage"
DEFAULT_BRANCH = "main"
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_MAX_RETRY = 3
DEFAULT_BACKOFF_SECONDS = 2
DEFAULT_CHUNK_SIZE_MB = 24
DEFAULT_COPY_COUNT = 1
DEFAULT_INTERVAL_SECONDS = 7 * 24 * 60 * 60
DEFAULT_REPOSITORY_PREFIX = "model"


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


def _env_int(name: str, default: int | None = None) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        if default is None:
            raise ConfigError(f"Falta {name}.")
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} debe ser un entero.") from exc
    if parsed <= 0:
        raise ConfigError(f"{name} debe ser mayor que 0.")
    return parsed


def _env_float(name: str, default: float | None = None, allow_zero: bool = False) -> float:
    value = os.environ.get(name)
    if value is None or not value.strip():
        if default is None:
            raise ConfigError(f"Falta {name}.")
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ConfigError(f"{name} debe ser un numero.") from exc
    if allow_zero:
        if parsed < 0:
            raise ConfigError(f"{name} no puede ser negativo.")
    elif parsed <= 0:
        raise ConfigError(f"{name} debe ser mayor que 0.")
    return parsed


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ConfigError(f"{name} debe ser un booleano valido.")


def _validate_repository_format(repo: str) -> tuple[str, str]:
    if repo.count("/") != 1:
        raise ConfigError("GITHUB_REPOSITORY debe tener el formato 'owner/repo'.")
    owner, name = repo.split("/", 1)
    if not owner or not name:
        raise ConfigError("GITHUB_REPOSITORY no puede tener owner o repo vacios.")
    return owner, name


@dataclass(frozen=True)
class GitHubAccountConfig:
    account_id: str
    owner: str
    token: str
    pinned_repository: str | None = None


@dataclass(frozen=True)
class TelegramAccountConfig:
    account_id: str
    api_id: int
    api_hash: str
    phone: str
    network: str = "telegram"


@dataclass(frozen=True)
class AppConfig:
    github_accounts: tuple[GitHubAccountConfig, ...]
    github_branch: str
    github_uploads_prefix: str
    github_repository_prefix: str
    github_repository_private: bool
    github_repository_max_size_kb: int
    github_account_daily_upload_limit_gb: float
    # Number of copies of each version, one per distinct account. Network-agnostic
    # (a copy may live on GitHub, Telegram, etc.), hence no `github_` prefix.
    copy_count: int
    github_chunk_size_mb: int
    github_timeout_seconds: int
    github_max_retry: int
    github_backoff_seconds: int
    github_upload_sleep_min_seconds: float
    github_upload_sleep_max_seconds: float
    app_data_dir: Path
    app_state_dir: Path
    app_web_host: str
    app_web_port: int
    app_sync_interval_seconds: int
    app_verify_interval_seconds: int
    app_web_pin: str | None
    app_encryption_key: str | None
    telegram_accounts: tuple[TelegramAccountConfig, ...] = ()
    tg_channel_prefix: str = "spider-model"
    tg_channel_private: bool = True
    tg_timeout_seconds: int = 900
    tg_max_retry: int = DEFAULT_MAX_RETRY
    tg_backoff_seconds: int = DEFAULT_BACKOFF_SECONDS

    @property
    def github_chunk_size_bytes(self) -> int:
        return min(self.github_chunk_size_mb, 95) * 1024 * 1024

    @property
    def github_account_daily_upload_limit_bytes(self) -> int:
        return int(self.github_account_daily_upload_limit_gb * (1024**3))


@dataclass(frozen=True)
class RuntimeSecrets:
    encryption_key: str
    web_pin: str
    flask_secret_key: str

    def encryption_key_bytes(self) -> bytes:
        padding = "=" * (-len(self.encryption_key) % 4)
        return base64.urlsafe_b64decode(self.encryption_key + padding)


def _discover_telegram_accounts() -> tuple[TelegramAccountConfig, ...]:
    accounts: list[TelegramAccountConfig] = []
    indices: set[str] = set()
    pattern = re.compile(r"^TG_ACCOUNT_(\d+)_(API_ID|API_HASH|PHONE)$")
    for key in os.environ:
        match = pattern.match(key)
        if match:
            indices.add(match.group(1))

    for index in sorted(indices, key=int):
        api_id_str = os.environ.get(f"TG_ACCOUNT_{index}_API_ID", "").strip()
        api_hash = os.environ.get(f"TG_ACCOUNT_{index}_API_HASH", "").strip()
        phone = os.environ.get(f"TG_ACCOUNT_{index}_PHONE", "").strip()
        present = [bool(api_id_str), bool(api_hash), bool(phone)]
        if all(present):
            try:
                api_id = int(api_id_str)
            except ValueError as exc:
                raise ConfigError(f"TG_ACCOUNT_{index}_API_ID debe ser un entero.") from exc
            accounts.append(TelegramAccountConfig(
                account_id=f"tg_account_{index}",
                api_id=api_id,
                api_hash=api_hash,
                phone=phone,
            ))
        elif any(present):
            raise ConfigError(
                f"La cuenta Telegram {index} debe definir TG_ACCOUNT_{index}_API_ID, API_HASH y PHONE."
            )

    return tuple(accounts)


def _discover_accounts() -> tuple[GitHubAccountConfig, ...]:
    accounts: list[GitHubAccountConfig] = []
    indices = set()
    pattern = re.compile(r"^GITHUB_ACCOUNT_(\d+)_(TOKEN|OWNER)$")
    for key in os.environ:
        match = pattern.match(key)
        if match:
            indices.add(match.group(1))

    for index in sorted(indices, key=int):
        token = os.environ.get(f"GITHUB_ACCOUNT_{index}_TOKEN", "").strip()
        owner = os.environ.get(f"GITHUB_ACCOUNT_{index}_OWNER", "").strip()
        if token and owner:
            accounts.append(GitHubAccountConfig(account_id=f"account_{index}", owner=owner, token=token))
            continue
        if token or owner:
            raise ConfigError(f"La cuenta GitHub {index} debe definir TOKEN y OWNER.")

    if accounts:
        return tuple(accounts)

    legacy_token = os.environ.get("GITHUB_TOKEN", "").strip()
    legacy_repository = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if legacy_token and legacy_repository:
        owner, repository = _validate_repository_format(legacy_repository)
        return (GitHubAccountConfig("legacy", owner, legacy_token, pinned_repository=repository),)

    raise ConfigError(
        "Falta al menos una cuenta GitHub. Define GITHUB_ACCOUNT_<n>_TOKEN y GITHUB_ACCOUNT_<n>_OWNER."
    )


def load_config() -> AppConfig:
    load_dotenv_file()

    github_branch = os.environ.get("GITHUB_BRANCH", DEFAULT_BRANCH).strip() or DEFAULT_BRANCH
    github_uploads_prefix = (
        os.environ.get("GITHUB_UPLOADS_PREFIX", DEFAULT_UPLOADS_PREFIX).strip().strip("/")
        or DEFAULT_UPLOADS_PREFIX
    )
    github_repository_prefix = (
        os.environ.get("GITHUB_REPOSITORY_PREFIX", DEFAULT_REPOSITORY_PREFIX).strip().strip("-")
        or DEFAULT_REPOSITORY_PREFIX
    )
    github_repository_private = _env_bool("GITHUB_REPOSITORY_PRIVATE", True)
    github_repository_max_size_kb = _env_int("GITHUB_REPOSITORY_MAX_SIZE_KB")
    github_account_daily_upload_limit_gb = _env_float("GITHUB_ACCOUNT_DAILY_UPLOAD_LIMIT_GB")
    # COPY_COUNT is the network-agnostic name. Fall back to the legacy
    # GITHUB_COPY_COUNT so existing deployments keep working.
    if os.environ.get("COPY_COUNT", "").strip():
        copy_count = _env_int("COPY_COUNT", DEFAULT_COPY_COUNT)
    else:
        copy_count = _env_int("GITHUB_COPY_COUNT", DEFAULT_COPY_COUNT)
    github_upload_sleep_min_seconds = _env_float("GITHUB_UPLOAD_SLEEP_MIN_SECONDS", 0.0, allow_zero=True)
    github_upload_sleep_max_seconds = _env_float("GITHUB_UPLOAD_SLEEP_MAX_SECONDS", 0.0, allow_zero=True)
    if github_upload_sleep_min_seconds > github_upload_sleep_max_seconds:
        raise ConfigError("GITHUB_UPLOAD_SLEEP_MIN_SECONDS no puede ser mayor que GITHUB_UPLOAD_SLEEP_MAX_SECONDS.")

    github_accounts = _discover_accounts()
    telegram_accounts = _discover_telegram_accounts()
    tg_channel_prefix = (
        os.environ.get("TG_CHANNEL_PREFIX", "spider-model").strip().strip("-")
        or "spider-model"
    )
    tg_channel_private = _env_bool("TG_CHANNEL_PRIVATE", True)
    tg_timeout_seconds = _env_int("TG_TIMEOUT_SECONDS", 900)
    tg_max_retry = _env_int("TG_MAX_RETRY", DEFAULT_MAX_RETRY)
    tg_backoff_seconds = _env_int("TG_BACKOFF_SECONDS", DEFAULT_BACKOFF_SECONDS)

    # A copy is placed on a distinct account, so COPY_COUNT can never exceed the
    # number of configured accounts across every network.
    total_accounts = len(github_accounts) + len(telegram_accounts)
    if copy_count > total_accounts:
        raise ConfigError("COPY_COUNT no puede ser mayor que el numero de cuentas configuradas (todas las redes).")

    config = AppConfig(
        github_accounts=github_accounts,
        github_branch=github_branch,
        github_uploads_prefix=github_uploads_prefix,
        github_repository_prefix=github_repository_prefix,
        github_repository_private=github_repository_private,
        github_repository_max_size_kb=github_repository_max_size_kb,
        github_account_daily_upload_limit_gb=github_account_daily_upload_limit_gb,
        copy_count=copy_count,
        github_chunk_size_mb=_env_int("GITHUB_CHUNK_SIZE_MB", DEFAULT_CHUNK_SIZE_MB),
        github_timeout_seconds=_env_int("GITHUB_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS),
        github_max_retry=_env_int("GITHUB_MAX_RETRY", DEFAULT_MAX_RETRY),
        github_backoff_seconds=_env_int("GITHUB_BACKOFF_SECONDS", DEFAULT_BACKOFF_SECONDS),
        github_upload_sleep_min_seconds=github_upload_sleep_min_seconds,
        github_upload_sleep_max_seconds=github_upload_sleep_max_seconds,
        app_data_dir=Path(os.environ.get("APP_DATA_DIR", "/datos")).resolve(),
        app_state_dir=Path(os.environ.get("APP_STATE_DIR", "/state")).resolve(),
        app_web_host=os.environ.get("APP_WEB_HOST", "0.0.0.0").strip() or "0.0.0.0",
        app_web_port=_env_int("APP_WEB_PORT", 8080),
        app_sync_interval_seconds=_env_int("APP_SYNC_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS),
        app_verify_interval_seconds=_env_int("APP_VERIFY_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS),
        app_web_pin=os.environ.get("APP_WEB_PIN", "").strip() or None,
        app_encryption_key=os.environ.get("APP_ENCRYPTION_KEY", "").strip() or None,
        telegram_accounts=telegram_accounts,
        tg_channel_prefix=tg_channel_prefix,
        tg_channel_private=tg_channel_private,
        tg_timeout_seconds=tg_timeout_seconds,
        tg_max_retry=tg_max_retry,
        tg_backoff_seconds=tg_backoff_seconds,
    )

    config.app_state_dir.mkdir(parents=True, exist_ok=True)
    return config


def generate_pin() -> str:
    return "".join(secrets.choice("0123456789") for _ in range(8))


def generate_encryption_key() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")


def generate_flask_secret() -> str:
    return secrets.token_hex(32)
