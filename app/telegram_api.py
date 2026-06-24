from __future__ import annotations

import asyncio
import io
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

LOGGER = logging.getLogger("spider-back")

# Pyrogram is an optional dependency: the module must stay importable (and unit
# testable with an injected fake client) on hosts that have not installed the
# MTProto stack. The real client is only required when a TelegramClient actually
# needs to connect.
try:  # pragma: no cover - exercised only where pyrogram is installed
    from pyrogram import Client as _PyrogramClient  # type: ignore
    from pyrogram import errors as _pyrogram_errors  # type: ignore
except ImportError:  # pragma: no cover - fallback for environments without pyrogram
    _PyrogramClient = None
    _pyrogram_errors = None


# Telegram is aggressive about rate limiting; treat transient connection issues
# the same way GitHubClient treats retryable HTTP codes.
RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (ConnectionError, TimeoutError)


class TelegramError(Exception):
    """Error base para la red Telegram."""


class TelegramFloodWaitError(TelegramError):
    """Excepción específica cuando Telegram exige esperar X segundos."""

    def __init__(self, message: str, wait_seconds: int):
        super().__init__(message)
        self.wait_seconds = int(wait_seconds)


@dataclass(frozen=True)
class TelegramSettings:
    """Configuración inmutable para una cuenta de Telegram. Análogo a GitHubSettings."""

    api_id: int
    api_hash: str
    phone_number: str          # Número con prefijo internacional (ej: +34600000000)
    session_name: str          # Nombre del archivo de sesión (ej: 'tg_account_1')
    timeout_s: int
    max_retry: int
    backoff_s: int
    session_dir: str = "."     # Carpeta donde vive el *.session (montado como volumen)


@dataclass(frozen=True)
class ChannelInfo:
    """Análogo a RepositoryInfo en GitHub."""

    chat_id: int               # ID único del canal (ej: -1001234567890)
    title: str                 # Título del canal (ej: 'spider-model-0001')
    is_private: bool


@dataclass(frozen=True)
class UploadedChunkMeta:
    """Metadatos devueltos tras subir un chunk."""

    message_id: int            # ID del mensaje en el canal
    file_unique_id: str        # ID único e inmutable del archivo en Telegram
    size: int


def _is_auth_key_unregistered(exc: Exception) -> bool:
    """Whether ``exc`` is Telegram's revoked/unknown auth-key error (401).

    Raised when the session's auth key is no longer valid — e.g. the session was
    revoked, or regenerated out-of-band (web re-login, fresh ``.session`` mounted)
    while this client still holds the stale key in memory. Matched by class name
    and message so it works without importing ``pyrogram.errors``.
    """
    if type(exc).__name__ == "AuthKeyUnregistered":
        return True
    return "AUTH_KEY_UNREGISTERED" in str(exc).upper()


def _flood_wait_seconds(exc: Exception) -> int | None:
    """Extract the wait time from a Pyrogram FloodWait, tolerating API drift.

    Pyrogram exposes the seconds as ``.value`` (modern) or ``.x`` (legacy).
    """
    for attr in ("value", "x", "seconds"):
        value = getattr(exc, attr, None)
        if isinstance(value, (int, float)):
            return int(value)
    return None


class TelegramClient:
    """MTProto-backed storage client, mirroring the GitHubClient surface.

    Repositories → private channels, commits → a JSON manifest message, and a
    chunk → a document message. Heavy lifting is delegated to Pyrogram, but the
    underlying client is created lazily via ``client_factory`` so tests can inject
    a fake and the module imports without pyrogram installed.
    """

    def __init__(
        self,
        settings: TelegramSettings,
        *,
        client_factory: Callable[[], Any] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ):
        self.settings = settings
        self._client_factory = client_factory or self._default_client_factory
        self._sleeper = sleeper or time.sleep
        self._client: Any = None
        self._authenticated_user_id: Optional[int] = None

    # ── Connection lifecycle ────────────────────────────────────────────────
    def _default_client_factory(self) -> Any:
        if _PyrogramClient is None:
            raise TelegramError(
                "Pyrogram no está instalado: añade 'pyrogram' y 'tgcrypto' para usar el backend Telegram."
            )
        return _PyrogramClient(
            name=self.settings.session_name,
            api_id=self.settings.api_id,
            api_hash=self.settings.api_hash,
            phone_number=self.settings.phone_number,
            workdir=self.settings.session_dir,
        )

    def _ensure_connection(self) -> Any:
        """Asegura que el cliente MTProto está conectado."""
        # Pyrogram's sync wrappers call asyncio.get_event_loop() internally.
        # Non-main threads (e.g. sync-scheduler) have no loop by default — create one.
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("closed")
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())

        if self._client is None:
            self._client = self._client_factory()
        if not getattr(self._client, "is_connected", False):
            connect = getattr(self._client, "connect", None)
            if callable(connect):
                connect()
        return self._client

    def _request(self, action_name: str, callable_func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Ejecuta una operación con reintentos, FloodWait y backoff exponencial.

        Análogo a GitHubClient._request: acotado por settings.max_retry.
        """
        last_exc: Exception | None = None
        auth_recovered = False
        for attempt in range(self.settings.max_retry):
            try:
                self._ensure_connection()
                return callable_func(*args, **kwargs)
            except TelegramFloodWaitError as exc:
                # CRÍTICO: Telegram dice exactamente cuánto esperar. Ignorarlo
                # provoca baneo de IP/cuenta.
                wait_time = exc.wait_seconds + 1
                self._sleeper(wait_time)
                last_exc = exc
                continue
            except RETRYABLE_EXCEPTIONS as exc:
                last_exc = exc
                if attempt == self.settings.max_retry - 1:
                    raise TelegramError(f"Fallo de red en {action_name} tras reintentos: {exc}") from exc
                self._sleeper(self.settings.backoff_s * (2 ** attempt))
            except Exception as exc:  # noqa: BLE001 - normalizamos hacia TelegramError
                flood_seconds = _flood_wait_seconds(exc)
                is_flood = _pyrogram_errors is not None and isinstance(
                    exc, getattr(_pyrogram_errors, "FloodWait", ())
                )
                if is_flood and flood_seconds is not None:
                    self._sleeper(flood_seconds + 1)
                    last_exc = exc
                    continue
                # Self-heal a revoked/stale auth key: the session may have been
                # regenerated on disk (web re-login in another process, or a fresh
                # .session mounted). Drop the cached client once and reconnect so
                # the next attempt reloads the session from disk. Bounded: we only
                # recover once per call, so a genuinely dead session still fails.
                if not auth_recovered and _is_auth_key_unregistered(exc):
                    auth_recovered = True
                    LOGGER.warning(
                        "sesión Telegram inválida en %s (auth key revocada); "
                        "recargando sesión del disco y reintentando",
                        action_name,
                    )
                    self.reset()
                    last_exc = exc
                    continue
                raise TelegramError(f"Error en {action_name}: {exc}") from exc
        raise TelegramError(f"Fallo en {action_name} tras {self.settings.max_retry} intentos: {last_exc}")

    # ── Identity ────────────────────────────────────────────────────────────
    def authenticated_user_id(self) -> int:
        """ID del usuario autenticado (para validaciones)."""
        if self._authenticated_user_id is not None:
            return self._authenticated_user_id

        def _get_me() -> int:
            me = self._client.get_me()
            return int(me.id)

        self._authenticated_user_id = self._request("get_me", _get_me)
        return self._authenticated_user_id

    # ── Channel (≈ repository) management ────────────────────────────────────
    def get_channel(self, chat_id: int) -> ChannelInfo:
        def _get_chat() -> ChannelInfo:
            chat = self._client.get_chat(chat_id)
            return _channel_info_from_chat(chat)

        return self._request("get_channel", _get_chat)

    def list_managed_channels(self, prefix: str) -> list[ChannelInfo]:
        """Canales (tipo channel) administrados cuyo título empieza por ``prefix``.

        Análogo a list_managed_repositories.
        """

        def _fetch_dialogs() -> list[ChannelInfo]:
            channels: list[ChannelInfo] = []
            for dialog in self._client.get_dialogs():
                chat = getattr(dialog, "chat", None)
                if chat is None:
                    continue
                if not _is_channel(chat):
                    continue
                title = getattr(chat, "title", None) or ""
                if not title.startswith(prefix):
                    continue
                channels.append(_channel_info_from_chat(chat))
            channels.sort(key=lambda item: item.title)
            return channels

        return self._request("list_managed_channels", _fetch_dialogs)

    def create_channel(self, title: str) -> ChannelInfo:
        """Crea un canal privado que actúa como 'repositorio'."""

        def _create() -> ChannelInfo:
            chat = self._client.create_channel(title, description="spider-back storage")
            return _channel_info_from_chat(chat)

        return self._request("create_channel", _create)

    # ── Uploads (≈ blobs + commit) ───────────────────────────────────────────
    def upload_chunk(self, chat_id: int, chunk_data: bytes, filename: str) -> UploadedChunkMeta:
        """Sube un fragmento como documento (send_document soporta >20MB)."""

        def _send() -> UploadedChunkMeta:
            # Pyrogram's send_document rejects raw bytes ("Expected a file path as
            # string or a binary (not text) file pointer"); it needs a path or a
            # binary file-like object. Wrap the chunk in an in-memory stream and
            # give it a .name so Pyrogram can derive the upload filename.
            stream = io.BytesIO(chunk_data)
            stream.name = filename
            msg = self._client.send_document(
                chat_id=chat_id,
                document=stream,
                file_name=filename,
                disable_notification=True,
                force_document=True,
            )
            return UploadedChunkMeta(
                message_id=int(msg.id),
                file_unique_id=str(msg.document.file_unique_id),
                size=len(chunk_data),
            )

        return self._request("upload_chunk", _send)

    def upload_manifest(self, chat_id: int, manifest_dict: dict[str, Any]) -> UploadedChunkMeta:
        """Sube el manifiesto JSON (análogo al 'commit' de GitHub)."""
        manifest_json = json.dumps(manifest_dict, ensure_ascii=False, indent=2).encode("utf-8")
        filename = f"manifest_{manifest_dict['version_id']}.json"
        return self.upload_chunk(chat_id, manifest_json, filename)

    def commit_copy(
        self,
        chat_id: int,
        version_id: str,
        chunks_data: list[bytes],
        chunk_filenames: list[str],
        *,
        sleep_after_upload: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """Orquesta la subida de una copia completa y devuelve metadata para index.json.

        1. Sube los chunks. 2. Sube el manifiesto que los enlaza. 3. Devuelve la
        estructura de copia compatible con el esquema network-agnostic de
        spider-back (network='telegram').
        """
        if len(chunks_data) != len(chunk_filenames):
            raise TelegramError("chunks_data y chunk_filenames deben tener la misma longitud.")

        chunks_meta: list[dict[str, Any]] = []
        for index, (data, filename) in enumerate(zip(chunks_data, chunk_filenames)):
            meta = self.upload_chunk(chat_id, data, filename)
            chunks_meta.append(
                {
                    "index": index,
                    "message_id": meta.message_id,
                    "file_unique_id": meta.file_unique_id,
                    "size": meta.size,
                    "network": "telegram",
                }
            )
            if sleep_after_upload is not None:
                sleep_after_upload()

        manifest = {
            "version_id": version_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "chunks": chunks_meta,
        }
        manifest_meta = self.upload_manifest(chat_id, manifest)

        return {
            "network": "telegram",
            "channel_id": chat_id,
            "manifest_message_id": manifest_meta.message_id,
            "manifest_file_unique_id": manifest_meta.file_unique_id,
            "uploaded_bytes": sum(chunk["size"] for chunk in chunks_meta) + manifest_meta.size,
            "chunks": chunks_meta,
        }

    # ── Download (≈ fetch_bytes) ──────────────────────────────────────────────
    def fetch_bytes(self, chat_id: int, message_id: int) -> bytes:
        """Descarga los bytes brutos de un mensaje concreto de un canal."""

        def _download() -> bytes:
            message = self._client.get_messages(chat_id, message_id)
            downloaded = self._client.download_media(message, in_memory=True)
            return _coerce_to_bytes(downloaded)

        return self._request("fetch_bytes", _download)

    def disconnect(self) -> None:
        """Limpieza segura de la sesión MTProto."""
        if self._client is not None and getattr(self._client, "is_connected", False):
            disconnect = getattr(self._client, "disconnect", None)
            if callable(disconnect):
                disconnect()

    def reset(self) -> None:
        """Descarta el cliente cacheado para que se reconstruya desde el .session.

        Tras un re-login por la web, el fichero de sesión en disco cambia; el
        cliente en memoria (posiblemente con una auth key revocada) debe
        recrearse en la próxima operación para tomar la sesión nueva.
        """
        self.disconnect()
        self._client = None
        self._authenticated_user_id = None


def _is_channel(chat: Any) -> bool:
    chat_type = getattr(chat, "type", None)
    if chat_type is None:
        return False
    # Pyrogram exposes ChatType enums; tolerate plain strings too.
    name = getattr(chat_type, "name", None) or str(chat_type)
    return "channel" in name.lower()


def _channel_info_from_chat(chat: Any) -> ChannelInfo:
    return ChannelInfo(
        chat_id=int(getattr(chat, "id")),
        title=getattr(chat, "title", None) or "",
        is_private=not bool(getattr(chat, "username", None)),
    )


def _coerce_to_bytes(downloaded: Any) -> bytes:
    if isinstance(downloaded, (bytes, bytearray)):
        return bytes(downloaded)
    # in_memory=True returns a file-like BytesIO in Pyrogram.
    read = getattr(downloaded, "read", None)
    if callable(read):
        getattr(downloaded, "seek", lambda *_: None)(0)
        return read()
    raise TelegramError("download_media no devolvió bytes ni un objeto leíble.")
