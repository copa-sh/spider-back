### Decisiones de Diseño para Telegram

1. **Por qué no la Bot API (HTTP)**: La Bot API oficial tiene un límite estricto de subida de **50 MB**. Spider-back permite chunks de hasta **95 MB**. Por lo tanto, usar la Bot API provocaría fallos constantes.
2. **La solución: MTProto (Userbot)**: Debemos usar la API de bajo nivel de Telegram (MTProto) mediante librerías como **Pyrogram** o **Telethon**. Estas librerías permiten iniciar sesión con un número de teléfono real (Userbot) o un Bot usando la capa MTProto, elevando el límite de subida a **2 GB** por archivo.
3. **Almacenamiento**: Los "repositorios" en Telegram se traducen como **Canales Privados**. Un canal permite almacenar un historial inmutable de mensajes, ideal para nuestro patrón de *append-only*.
4. **El Manifiesto**: Dado que Telegram no tiene un sistema de "Git Trees" ni "Commits", el análogo es un **mensaje especial (documento JSON)** que se sube al final de cada copia. Este mensaje contiene el mapeo de qué `message_id` corresponde a qué `chunk_index`.
5. **Rate Limiting (FloodWait)**: Telegram es extremadamente agresivo con el rate limiting. El cliente debe capturar específicamente `FloodWaitError` y dormir los segundos exactos que exige la API, además de usar un backoff exponencial genérico para otros errores.

---

### Pseudocódigo: `telegram_api.py`

```python
from __future__ import annotations
import time
import json
import hashlib
from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime, timezone

# Nota: En producción real se usaría `from pyrogram import Client, errors`
# Aquí usamos pseudoclases para ilustrar el flujo.

API_ID = 123456  # Se obtiene en my.telegram.org
API_HASH = "abcdef1234567890..."

class TelegramError(Exception):
    """Error base para la red Telegram."""
    pass

class TelegramFloodWaitError(TelegramError):
    """Excepción específica cuando Telegram exige esperar X segundos."""
    def __init__(self, message: str, wait_seconds: int):
        super().__init__(message)
        self.wait_seconds = wait_seconds

@dataclass(frozen=True)
class TelegramSettings:
    """Configuración inmutable para una cuenta de Telegram."""
    api_id: int
    api_hash: str
    phone_number: str          # Número con prefijo internacional (ej: +34600000000)
    session_name: str          # Nombre del archivo de sesión (ej: 'account_1')
    timeout_s: int
    max_retry: int
    backoff_s: int

@dataclass(frozen=True)
class ChannelInfo:
    """Análogo a RepositoryInfo en GitHub."""
    chat_id: int               # ID único del canal en Telegram (ej: -1001234567890)
    title: str                 # Título del canal (ej: 'spider-model-0001')
    is_private: bool

@dataclass
class UploadedChunkMeta:
    """Metadatos devueltos tras subir un chunk."""
    message_id: int            # ID del mensaje en el canal
    file_unique_id: str        # ID único del archivo en Telegram (inmutable)
    size: int

class TelegramClient:
    def __init__(self, settings: TelegramSettings):
        self.settings = settings
        # En Pyrogram real: self.client = Client(session_name, api_id, api_hash, phone_number)
        self._authenticated_user_id: Optional[int] = None
        self._client = None # Pseudocódigo del cliente MTProto

    def _ensure_connection(self):
        """Asegura que el cliente MTProto está conectado."""
        if not self._client or not self._client.is_connected:
            # self._client.connect()
            pass

    def _request(self, action_name: str, callable_func, *args, **kwargs) -> Any:
        """
        Wrapper de ejecución que maneja reintentos, FloodWait y errores de red.
        Análogo al método _request de GitHubClient.
        """
        last_exc: Exception | None = None
        for attempt in range(self.settings.max_retry):
            try:
                self._ensure_connection()
                return callable_func(*args, **kwargs)
                
            except TelegramFloodWaitError as exc:
                # CRÍTICO: Telegram nos dice exactamente cuánto esperar.
                # Si no respetamos esto, banearán la IP/cuenta.
                wait_time = exc.wait_seconds + 1 
                print(f"[Telegram] FloodWait detectado en {action_name}. Durmiendo {wait_time}s...")
                time.sleep(wait_time)
                continue

            except (ConnectionError, TimeoutError) as exc:
                last_exc = exc
                if attempt == self.settings.max_retry - 1:
                    raise TelegramError(f"Fallo de red en {action_name} tras reintentos: {exc}") from exc
                time.sleep(self.settings.backoff_s * (2 ** attempt))
                
            except Exception as exc:
                # Otros errores (ej. peertls, autorización revocada)
                raise TelegramError(f"Error en {action_name}: {exc}") from exc

        raise TelegramError(f"Fallo desconocido en {action_name}: {last_exc}")

    def authenticated_user_id(self) -> int:
        """Obtiene el ID del usuario autenticado para validaciones."""
        if self._authenticated_user_id:
            return self._authenticated_user_id
        def _get_me():
            # me = self._client.get_me()
            # return me.id
            return 999888777
        self._authenticated_user_id = self._request("get_me", _get_me)
        return self._authenticated_user_id

    def get_channel(self, chat_id: int) -> ChannelInfo:
        """Obtiene información de un canal específico."""
        def _get_chat():
            # chat = self._client.get_chat(chat_id)
            # return ChannelInfo(chat_id=chat.id, title=chat.title, is_private=chat.type == 'private')
            return ChannelInfo(chat_id=chat_id, title="spider-test", is_private=True)
        return self._request("get_channel", _get_chat)

    def list_managed_channels(self, prefix: str) -> list[ChannelInfo]:
        """
        Busca canales donde el usuario es administrador cuyo título empiece por 'prefix'.
        Análogo a list_managed_repositories.
        """
        def _fetch_dialogs():
            channels = []
            # En Pyrogram real: self._client.get_dialogs()
            # Filtramos aquellos donde chat.type es 'channel', el usuario es creador/admin,
            # y chat.title.startswith(prefix)
            
            # Pseudocódigo de iteración:
            # for dialog in self._client.get_dialogs():
            #     chat = dialog.chat
            #     if chat.type == "channel" and chat.title.startswith(prefix):
            #         channels.append(ChannelInfo(chat.id, chat.title, chat.is_private))
            return channels
            
        return self._request("list_managed_channels", _fetch_dialogs)

    def create_channel(self, title: str) -> ChannelInfo:
        """Crea un canal privado para actuar como repositorio."""
        def _create():
            # chat = self._client.create_channel(title, description="Spider-back storage")
            # return ChannelInfo(chat.id, chat.title, True)
            return ChannelInfo(chat_id=-10012345, title=title, is_private=True)
        return self._request("create_channel", _create)

    def upload_chunk(self, chat_id: int, chunk_data: bytes, filename: str) -> UploadedChunkMeta:
        """
        Sube un fragmento de archivo como un documento al canal.
        Se usa 'send_document' en lugar de 'send_message' para soportar >20MB.
        """
        def _send():
            # msg = self._client.send_document(
            #     chat_id=chat_id,
            #     document=chunk_data,
            #     file_name=filename,
            #     disable_notification=True # Para no spamear
            # )
            # return UploadedChunkMeta(
            #     message_id=msg.id,
            #     file_unique_id=msg.document.file_unique_id,
            #     size=len(chunk_data)
            # )
            return UploadedChunkMeta(message_id=101, file_unique_id="abc123", size=len(chunk_data))

        return self._request("upload_chunk", _send)

    def upload_manifest(self, chat_id: int, manifest_dict: dict) -> UploadedChunkMeta:
        """
        Sube el manifiesto JSON de la copia. 
        Este es el análogo al 'commit' en GitHub. Contiene los message_ids de los chunks.
        """
        manifest_json = json.dumps(manifest_dict, indent=2).encode('utf-8')
        filename = f"manifest_{manifest_dict['version_id']}.json"
        return self.upload_chunk(chat_id, manifest_json, filename)

    def commit_copy(self, chat_id: int, version_id: str, chunks_data: list[bytes], chunk_filenames: list[str]) -> dict:
        """
        Orquesta la subida completa de una copia.
        1. Sube todos los chunks.
        2. Crea y sube el manifiesto que los enlaza.
        3. Devuelve la metadata necesaria para index.json.
        """
        chunks_meta = []
        
        # 1. Subir chunks secuenciales
        for i, (data, fname) in enumerate(zip(chunks_data, chunk_filenames)):
            print(f"Subiendo chunk {i+1}/{len(chunks_data)} a Telegram...")
            meta = self.upload_chunk(chat_id, data, fname)
            chunks_meta.append({
                "index": i,
                "message_id": meta.message_id,
                "file_unique_id": meta.file_unique_id,
                "size": meta.size
            })
            
            # Nota: Se podría añadir el sleep configurado por GITHUB_UPLOAD_SLEEP_MIN_SECONDS 
            # (renombrado a UPLOAD_SLEEP en el config genérico) aquí.

        # 2. Preparar y subir manifiesto
        manifest = {
            "version_id": version_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "chunks": chunks_meta
        }
        manifest_meta = self.upload_manifest(chat_id, manifest)

        # 3. Devolver estructura compatible con el estado de Spider-back
        return {
            "network": "telegram",
            "channel_id": chat_id,
            "manifest_message_id": manifest_meta.message_id,
            "manifest_file_unique_id": manifest_meta.file_unique_id,
            "uploaded_bytes": sum(c["size"] for c in chunks_meta) + manifest_meta.size,
            "chunks": [
                {
                    "index": c["index"],
                    "message_id": c["message_id"],
                    "file_unique_id": c["file_unique_id"],
                    "size": c["size"],
                    # En index.json estos campos tienen otro significado para GitHub, 
                    # pero se unifican bajo el esquema de "copies"
                } for c in chunks_meta
            ]
        }

    def fetch_bytes(self, chat_id: int, message_id: int) -> bytes:
        """
        Descarga los bytes brutos de un mensaje específico en un canal.
        Usa el message_id porque es el puntero absoluto en Telegram.
        """
        def _download():
            # En Pyrogram: self._client.download_media(chat_id, message_id, in_memory=True)
            # return bytes_obj
            return b"pseudo_bytes_descargados"
            
        return self._request("fetch_bytes", _download)

    def disconnect(self):
        """Limpieza segura de la sesión MTProto."""
        if self._client and self._client.is_connected:
            # self._client.disconnect()
            pass
```

---

### Impacto en `index.json` (El Contrato de Datos)

Para que el sistema principal de Spider-back entienda este nuevo backend, la estructura de `copies` y `chunks` dentro de `files[<file_id>].versions[<n>]` debe adaptarse ligeramente.

En GitHub, un chunk se referencia así:
```json
{
  "path": "storage/abc123.chunk",
  "raw_url": "https://raw.githubusercontent.com/...",
  "repository": "model-0001",
  "repository_owner": "user"
}
```

En Telegram, la misma entrada en la matriz `chunks` (bajo una copia de red Telegram) se vería así:
```json
{
  "index": 0,
  "message_id": 105,
  "file_unique_id": "AQADBAATx6E4Vg",
  "size": 25165824
}
```

Y el bloque padre `copies` para Telegram tendría esta pinta:
```json
{
  "copy_index": 0,
  "network": "telegram",
  "account_id": "tg_account_1",
  "channel_id": -1001234567890,
  "channel_title": "spider-model-0005",
  "manifest_message_id": 112,
  "uploaded_bytes": 52428800,
  "chunks": [ ... ]
}
```

### Variables de Entorno Requeridas (Nuevas)

Siguiendo el patrón de diseño del documento, las variables para Telegram se añadirían así:

```bash
# Cuenta Telegram 1
TG_ACCOUNT_1_API_ID=12345678
TG_ACCOUNT_1_API_HASH="abcdef1234567890abcdef1234567890"
TG_ACCOUNT_1_PHONE="+34600000000"

# Cuenta Telegram 2 (Opcional, para redundancia)
TG_ACCOUNT_2_API_ID=87654321
TG_ACCOUNT_2_API_HASH="0987654321098765432109876543210"
TG_ACCOUNT_2_PHONE="+34600000001"

# Configuración global de Telegram
TG_CHANNEL_PREFIX="spider-model"  # Análogo a GITHUB_REPOSITORY_PREFIX
TG_CHANNEL_PRIVATE="true"
```

### Consideraciones Críticas de Producción

1. **Sesiones MTProto (`*.session`)**: Al usar Pyrogram/Telethon, la primera vez que arranca el contenedor Docker con un número nuevo, Telegram envía un código SMS. **Spider-back necesitaría una fase de "setup" o inyección de sesión**. La forma más Docker-first de resolver esto es montando el archivo `tg_account_1.session` como volumen en `/state/tg_account_1.session`, generándolo previamente en local y asegurándolo.
2. **Límites de Account**: Telegram permite crear ~500 canales por cuenta. Si `repository_max_size_kb` es pequeño, se pueden agotar los canales rápido. Se debe monitorizar `list_managed_channels`.
3. **Borrado Lógico vs Físico**: Si se elimina un archivo en `/datos`, en GitHub se podría intentar borrar del repositorio (aunque Spider-back no lo hace, es inmutable). En Telegram, dejar los mensajes ahí es lo correcto (inmutabilidad), simplemente se marca `present: false` en `index.json` y se ignora ese `version_id`.
4. **Descarga en Verificación**: El método `fetch_bytes` de Telegram es notablemente más lento que el de GitHub para archivos grandes, debido a cómo MTProto particiona internamente los archivos. El `GITHUB_TIMEOUT_SECONDS` debe tener su análogo `TG_TIMEOUT_SECONDS` con valores más altos (ej. 900s).