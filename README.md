# Spider-back

**Spider-back** es un daemon de respaldo distribuido, cifrado y resiliente que almacena tus datos de forma segura repartiendo fragmentos cifrados entre **GitHub** y/o **Telegram**.

Funciona como una segunda capa de cifrado y fragmentación sobre el contenido de `/datos` (ideal para usar debajo de `gocryptfs`). Trata los archivos locales como bytes opacos, los cifra con AES-256-GCM, los divide en chunks y los distribuye inteligentemente entre los backends configurados.

## Características

- **Backends soportados**: GitHub (múltiples cuentas/repos) y Telegram (múltiples canales privados) — simultáneamente si se desea.
- Cifrado doble: `gocryptfs` (opcional) + AES-256-GCM propio.
- Fragmentación en chunks configurables.
- Detección de cambios por hash + sincronización ligera por nombre (rápida).
- Copias múltiples por versión repartidas entre cuentas GitHub distintas.
- Verificación periódica de integridad reconstruyendo desde los backends remotos.
- Gestión automática de cuotas diarias y creación de repositorios.
- Interfaz web mínima con autenticación por PIN.
- Scheduler integrado para sync y verify automáticos.
- Estado persistente en `/state/index.json`, `/state/secrets.json` y `/state/upload_index.sqlite3`.
- Docker-first.

## Cómo funciona

1. Lee archivos en `/datos` (solo lectura).
2. Cifra cada archivo con AES-256-GCM usando `APP_ENCRYPTION_KEY`.
3. Divide en chunks.
4. Elige una o varias cuentas de cualquier backend (network) (ej: Github, Telegram, etc ...) con cuota disponible para crear las copias de la versión.
5. Sube todos los chunks de cada copia a una sola cuenta y guarda la ubicación en el índice.
6. Periódicamente verifica la integridad descargando y comparando.

## Estado persistente y compatibilidad

La versión actual guarda todo su estado funcional dentro de `APP_STATE_DIR`, que por defecto es `/state`.
Si cambia cualquiera de los archivos descritos aquí, hay impacto directo en compatibilidad con ejecuciones ya existentes.

### Resumen de archivos persistentes

| Archivo | Tipo | Rol |
| --- | --- | --- |
| `index.json` | JSON | Estado canónico de la aplicación: archivos, tareas, cuentas y configuración efectiva. |
| `secrets.json` | JSON | Secretos de ejecución generados o fijados por entorno. |
| `upload_index.sqlite3` | SQLite | Índice de desduplicación y reutilización de versiones ya subidas. |
| `logs/spider-back.log` | Log plano | Registro operacional persistente. No forma parte del contrato de datos. |
| `sync.lock`, `verify.lock` | Lock files | Exclusión mutua entre procesos. No contienen estado lógico. |

### `index.json`

Este es el estado principal. Se crea automáticamente si no existe y se reescribe de forma atómica usando un archivo temporal y `replace()`.

Estructura de primer nivel:

```json
{
  "created_at": "2026-06-22T00:00:00Z",
  "config": {
    "data_dir": "/datos",
    "state_dir": "/state",
    "github_accounts": [
      { "account_id": "account_1", "owner": "tuusuario", "network": "github" }
    ],
    "branch": "main",
    "uploads_prefix": "storage",
    "repository_prefix": "model",
    "repository_private": true,
    "repository_max_size_kb": 524288,
    "daily_upload_limit_gb": 5,
    "copy_count": 1,
    "web_host": "0.0.0.0",
    "web_port": 8080,
    "sync_interval_seconds": 604800,
    "verify_interval_seconds": 604800,
    "chunk_size_mb": 24,
    "upload_sleep_min_seconds": 0,
    "upload_sleep_max_seconds": 0
  },
  "tasks": {
    "sync": {},
    "verify": {}
  },
  "files": {},
  "github_accounts": {}
}
```

Notas importantes:

- `config` es una instantánea de la configuración efectiva cargada al arrancar. Sirve para auditar qué valores quedaron activos en esa instancia.
- `created_at` marca el instante en que el estado fue creado por primera vez.
- `tasks` siempre contiene, como mínimo, `sync` y `verify`.
- `files` es un mapa indexado por `file_id` estable.
- `github_accounts` es un mapa indexado por `account_id`. Cada cuenta conserva su `network` efectivo.

#### `tasks.sync` y `tasks.verify`

Cada tarea persistida contiene exactamente estos campos:

- `last_started_at`
- `last_finished_at`
- `last_result` (`never`, `success` o `error`)
- `last_error`
- `last_summary`
- `last_manual_trigger_at`
- `running`

`running` se refresca también en lectura para reflejar si el lock de proceso está tomado en ese momento.

#### `files`

Cada entrada de `files` representa un archivo local observado bajo `APP_DATA_DIR`.
La clave del mapa es `file_id`, calculado de forma estable a partir de la ruta relativa.

Campos de la entrada de archivo:

- `file_id`
- `path`
- `present`
- `size`
- `mtime_ns`
- `source_sha256`
- `last_seen_at`
- `versions`
- `active_version_id`
- `last_verification`
- `last_error`

Qué significa cada uno:

- `path` guarda la ruta relativa dentro de `APP_DATA_DIR`.
- `present` indica si el archivo sigue existiendo en el escaneo o en la verificación.
- `size` y `mtime_ns` permiten una sincronización ligera sin volver a hashear si el archivo no cambió.
- `source_sha256` es el hash del contenido en claro del archivo local.
- `versions` es el historial de versiones subidas para ese archivo.
- `active_version_id` apunta a la versión actualmente considerada vigente.
- `last_verification` guarda el resultado de la última verificación de esa versión activa.
- `last_error` almacena el último error conocido para ese archivo.

Cada elemento de `versions` es un objeto completo de versión con, como mínimo:

- `version_id`
- `created_at`
- `plaintext_sha256`
- `ciphertext_sha256`
- `size`
- `mtime_ns`
- `source_sha256`
- `repository_owner`
- `repository`
- `branch`
- `account_id`
- `encryption`
- `chunks`
- `commit_sha`
- `uploaded_bytes`
- `copies`
- `copy_count_requested`
- `copy_count_completed`
- `replication_complete`
- `copy_errors`

El bloque `encryption` contiene:

- `algorithm`
- `nonce_b64`
- `key_id`

El bloque `chunks` contiene una lista de fragmentos con:

- `index`
- `path`
- `raw_url`
- `sha256`
- `size`
- `repository`
- `repository_owner`
- `account_id`

El bloque `copies` contiene las replicas completas de una misma version.
Cada copia vive por completo dentro de una sola cuenta GitHub y todos sus trozos se almacenan siempre en esa misma cuenta.
Cada elemento de `copies` incluye, como minimo:

- `copy_index`
- `network`
- `account_id`
- `repository_owner`
- `repository`
- `branch`
- `manifest_path`
- `manifest_raw_url`
- `commit_sha`
- `uploaded_bytes`
- `encryption`
- `chunks`

#### `github_accounts`

Cada cuenta GitHub mantiene su propio subestado:

- `account_id`
- `owner`
- `network`
- `repositories`
- `daily_uploads`
- `last_metadata_refresh_at`
- `last_upload_at`
- `available`
- `unavailable_reason`
- `unavailable_since`
- `alerts`

Dentro de `repositories`, cada repositorio conocido guarda:

- `name`
- `owner`
- `network`
- `last_known_size_kb`
- `private`
- `last_refreshed_at`

#### Reglas de evolución del JSON

- Si `index.json` no existe, se crea con la estructura mínima por defecto.
- Si faltan claves nuevas al cargar una versión vieja, el sistema las rellena con valores por defecto sin romper el resto del estado.
- El estado se va guardando durante `sync` cada cierto número de archivos para no perder progreso intermedio.
- `verify` solo actualiza los campos de verificación y errores, sin reescribir la historia completa de versiones.

### Migraciones manuales

Si tu `index.json` es anterior a este cambio, puedes migrarlo manualmente con:

```bash
python3 migrations/001_add_network_and_copies.py /state/index.json
```

La migración:

- Añade `network=github` a las cuentas y repositorios existentes.
- Envuelve cada version antigua en `copies` con una sola copia.
- Conserva el resto del estado tal cual.

### `secrets.json`

Este archivo contiene secretos de ejecución persistidos. Se escribe automáticamente durante el arranque si faltan valores.

Campos actuales:

- `web_pin`
- `encryption_key`
- `flask_secret_key`
- `updated_at`

Comportamiento:

- Si `APP_WEB_PIN` está vacío y `secrets.json` no lo tiene, se genera un PIN numérico de 8 dígitos.
- Si `APP_ENCRYPTION_KEY` está vacío y `secrets.json` no lo tiene, se genera una clave AES-256 compatible con `urlsafe_b64decode`.
- `flask_secret_key` siempre se genera y se persiste si no existe.
- Si ya hay valores en `secrets.json`, esos valores se reutilizan y no se sobrescriben salvo que el entorno fuerce uno explícito para `web_pin` o `encryption_key`.

### `upload_index.sqlite3`

La base SQLite guarda un índice de deduplicación por hash de contenido original.
No almacena el árbol completo de estado, solo referencias a versiones ya vistas.

Esquema actual:

```sql
CREATE TABLE IF NOT EXISTS uploaded_versions (
    source_sha256 TEXT PRIMARY KEY,
    version_json TEXT NOT NULL,
    first_uploaded_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    copy_count INTEGER NOT NULL DEFAULT 1
)
```

Uso real:

- `source_sha256` identifica de forma única el contenido fuente.
- `version_json` guarda la versión completa tal y como fue subida.
- `first_uploaded_at` registra la primera vez que se vio esa versión.
- `last_seen_at` se actualiza cuando el mismo hash vuelve a aparecer.
- `copy_count` se incrementa cuando otra copia local del mismo contenido reutiliza esa versión.

La conexión SQLite se abre con `WAL` y `synchronous=NORMAL`.
Cuando un archivo local vuelve a aparecer con el mismo `source_sha256`, el sistema intenta reutilizar la versión ya registrada en esta tabla antes de subir de nuevo.

### Variables de entorno

El runtime carga primero `.env` y solo rellena variables que no existan ya en el entorno del proceso.
También acepta líneas con `export VAR=valor`, ignora comentarios y soporta valores entre comillas.

```bash
cp .env.example .env
```

#### Variables de aplicación

| Variable | Requerida | Valor por defecto | Efecto |
| --- | --- | --- | --- |
| `APP_DATA_DIR` | No | `/datos` | Directorio de entrada de archivos a vigilar y sincronizar. |
| `APP_STATE_DIR` | No | `/state` | Directorio donde vive todo el estado persistente. |
| `APP_WEB_HOST` | No | `0.0.0.0` | Host de escucha de la interfaz web. |
| `APP_WEB_PORT` | No | `8080` | Puerto de la interfaz web. |
| `APP_SYNC_INTERVAL_SECONDS` | No | `604800` | Intervalo del scheduler de sync. |
| `APP_VERIFY_INTERVAL_SECONDS` | No | `604800` | Intervalo del scheduler de verify. |
| `APP_WEB_PIN` | No | generado si falta | PIN de acceso a la web. Se persiste en `secrets.json` si no se define. |
| `APP_ENCRYPTION_KEY` | No | generada si falta | Clave de cifrado AES-256-GCM. Se persiste en `secrets.json` si no se define. |

#### Variables GitHub por cuenta

| Variable | Requerida | Valor por defecto | Efecto |
| --- | --- | --- | --- |
| `GITHUB_ACCOUNT_<n>_TOKEN` | Sí, junto con `OWNER` | - | Token de acceso de la cuenta. |
| `GITHUB_ACCOUNT_<n>_OWNER` | Sí, junto con `TOKEN` | - | Usuario u organización propietaria. |
| `GITHUB_TOKEN` | Solo modo legado | - | Token único heredado. Se usa solo si no hay cuentas numeradas. |
| `GITHUB_REPOSITORY` | Solo modo legado | - | Repositorio heredado `owner/repo`. Se usa solo si no hay cuentas numeradas. |

Reglas de descubrimiento:

- Se pueden definir tantas cuentas numeradas como quieras.
- Si existe al menos una cuenta numerada, el modo legado (`GITHUB_TOKEN` + `GITHUB_REPOSITORY`) se ignora.
- Si una cuenta numerada tiene `TOKEN` pero no `OWNER`, o al revés, la carga de configuración falla.

#### Variables GitHub de almacenamiento

| Variable | Requerida | Valor por defecto efectivo | Efecto |
| --- | --- | --- | --- |
| `GITHUB_BRANCH` | No | `main` | Rama destino para los blobs y commits. |
| `GITHUB_UPLOADS_PREFIX` | No | `storage` | Prefijo remoto donde se guardan los objetos subidos. |
| `GITHUB_REPOSITORY_PREFIX` | No | `model` | Prefijo de repositorios gestionados. El valor se normaliza quitando guiones finales. |
| `GITHUB_REPOSITORY_PRIVATE` | No | `true` | Crea repositorios privados por defecto. |
| `GITHUB_REPOSITORY_MAX_SIZE_KB` | Sí | - | Límite máximo de tamaño por repositorio gestionado. |
| `GITHUB_ACCOUNT_DAILY_UPLOAD_LIMIT_GB` | Sí | - | Límite diario de subida por cuenta. |
| `COPY_COUNT` | No | `1` | Número de copias de cada versión. Cada copia se coloca entera en una cuenta distinta (de cualquier red: GitHub, Telegram, etc.). Antes se llamaba `GITHUB_COPY_COUNT`, que sigue aceptándose por compatibilidad. |
| `GITHUB_CHUNK_SIZE_MB` | No | `24` | Tamaño nominal de fragmentación. El código lo recorta a un máximo efectivo de `95 MB`. |
| `GITHUB_TIMEOUT_SECONDS` | No | `300` | Timeout de peticiones GitHub. |
| `GITHUB_MAX_RETRY` | No | `3` | Número de reintentos HTTP. |
| `GITHUB_BACKOFF_SECONDS` | No | `2` | Retardo base entre reintentos. |
| `GITHUB_UPLOAD_SLEEP_MIN_SECONDS` | No | `0` | Límite inferior del sleep entre subidas. |
| `GITHUB_UPLOAD_SLEEP_MAX_SECONDS` | No | `0` | Límite superior del sleep entre subidas. Debe ser mayor o igual que el mínimo. |

Notas de compatibilidad:

- `.env.example` incluye valores de ejemplo más conservadores para `GITHUB_UPLOAD_SLEEP_MIN_SECONDS` y `GITHUB_UPLOAD_SLEEP_MAX_SECONDS`; si no se definen, el runtime no duerme entre subidas.
- `GITHUB_REPOSITORY_PREFIX` se limpia con `strip("-")`, así que `model`, `model-` y `model--` terminan normalizándose al mismo prefijo efectivo.
- `COPY_COUNT` (o el antiguo `GITHUB_COPY_COUNT`) debe ser menor o igual que el número total de cuentas configuradas en todas las redes. Dos copias bajo una misma cuenta cuentan como una sola.
- `GITHUB_CHUNK_SIZE_MB` se interpreta en bytes al generar chunks, pero el tamaño efectivo nunca supera 95 MB por chunk.

#### Variables no implementadas en esta rama

- En este árbol no aparecen variables `TELEGRAM_*` en el runtime ni en los tests.
- Si la versión de producción que quieres comparar usa Telegram, conviene documentar esas variables en un bloque separado para no mezclar contratos distintos.

## Docker

```bash
# Construir e iniciar
docker compose up -d --build
```

Si no definiste `APP_WEB_PIN`, revisa los logs:

```bash
docker compose logs -f spider-back
```

## Interfaz Web

Accede a `http://tu-servidor:8080`

- `GET /login` + `POST /login`
- `/` → Dashboard (últimas ejecuciones, cuotas, estado)
- `/files` → Listado de archivos y versiones
- `/logs` → Logs persistentes
- Acciones manuales: Sync, Full Sync, Verify

## Comandos (desarrollo y mantenimiento)

```bash
# Entorno de desarrollo
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

# Modo desarrollo (recarga automática)
python3 -m spider_back.main web-dev

# Comandos útiles
python3 -m spider_back.main scheduler
python3 -m spider_back.main run-once-sync
python3 -m spider_back.main run-once-full-sync
python3 -m spider_back.main run-once-verify
```

## Estructura de almacenamiento

- **GitHub**: Repositorios automáticos (`model-0001`, `model-0002`, …) con chunks de una copia completa. Cada copia vive entera dentro de una sola cuenta.
- **`/state/index.json`**: historial completo de archivos, versiones y cuentas.
- **`/state/secrets.json`**: PIN web, clave de cifrado y secreto Flask persistidos.
- **`/state/upload_index.sqlite3`**: índice de versiones ya subidas y reutilizadas.

## Seguridad

- La aplicación nunca descifra el contenido local (solo compara bytes cifrados).
- La clave de cifrado y el PIN web se generan automáticamente si no se definen y se guardan en `/state/secrets.json`.
- El índice SQLite evita subir de nuevo contenido ya visto con el mismo `source_sha256`.

## Recomendaciones

- Usa `gocryptfs` en `/datos` para cifrado local fuerte.
- Combina ambos backends para máxima redundancia.
- Monitorea las cuotas diarias para evitar rate limits.

---

**Spider-back** te da un sistema de backup "araña" distribuido, cifrado, verificable y de muy bajo coste usando infraestructuras públicas.
