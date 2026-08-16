# Spider-back

**Spider-back** es un daemon de respaldo distribuido, cifrado y resiliente que almacena tus datos de forma segura repartiendo fragmentos cifrados entre **GitHub** y/o **Telegram**.

Funciona como una segunda capa de cifrado y fragmentación sobre el contenido de `/datos` (ideal para usar debajo de `gocryptfs`). Trata los archivos locales como bytes opacos, los cifra con AES-256-GCM en streaming y los distribuye entre los backends configurados.

## Características

- **Backends soportados**: GitHub (múltiples cuentas/repos) y Telegram (múltiples canales privados) — simultáneamente si se desea.
- Cifrado doble: `gocryptfs` (opcional) + AES-256-GCM propio, con un nonce por parte.
- En GitHub, los datos se guardan como **assets de release**: 1 petición por parte, partes de ~1 GiB.
- **Dos operaciones**: `sync` (solo lo nuevo o modificado) y `verify` (sin escrituras).
- Arranque proporcional al número de cambios, no al volumen total.
- Limitador de peticiones proactivo, por debajo de los límites documentados de GitHub.
- Copias múltiples por versión repartidas entre cuentas distintas.
- Interfaz web mínima con autenticación por PIN.
- Scheduler integrado para sync y verify automáticos.
- Estado persistente en `/state/index.json`, `/state/secrets.json` y `/state/upload_index.sqlite3`.
- Docker-first.

## Cómo funciona

1. Recorre `/datos` (solo lectura) en orden determinista y en streaming.
2. Para cada ruta hace una consulta indexada al registro local. Si `size` y `mtime_ns`
   coinciden y la fila está `complete`, **se salta sin hashear y sin abrir el archivo**.
3. Si cambiaron, calcula el hash. Si el hash coincide con el registrado, solo actualiza
   los metadatos: no vuelve a subir nada.
4. Si el contenido es nuevo, elige una o varias cuentas con cuota disponible.
5. Cifra el archivo en partes de ~1 GiB directamente a disco temporal y sube cada parte
   como un asset de release. Cada parte cuesta **exactamente una petición**.
6. Periódicamente `verify` comprueba metadatos de todo y descarga+hashea 1 de cada N.

### Por qué releases y no blobs

GitHub documenta un límite secundario de **500 peticiones generadoras de contenido por
hora** y 80 por minuto. El flujo anterior gastaba **5 peticiones por archivo**
(`create_blob` del chunk, `create_blob` del manifiesto, `create_tree`, `create_commit`,
`update_ref`), lo que imponía un techo de ~100 archivos/hora: 10.000 archivos ≈ 100 horas.
Ninguna optimización de red podía superarlo, porque el recurso escaso son las peticiones,
no los bytes.

Un asset de release cuesta **1 petición por parte** y admite **2 GiB de binario crudo**,
frente a los 100 MB en base64 (+33%) del API de blobs. Además, un asset existe en cuanto
el API devuelve 201, mientras que un blob sin commit es basura recolectable: no hay
ventana en la que los datos estén subidos pero inalcanzables.

Los datos escritos con el formato anterior (commits) **siguen siendo legibles y
verificables**; no se re-suben.

## Estado persistente y compatibilidad

La versión actual guarda todo su estado funcional dentro de `APP_STATE_DIR`, que por defecto es `/state`.
Si cambia cualquiera de los archivos descritos aquí, hay impacto directo en compatibilidad con ejecuciones ya existentes.

### Resumen de archivos persistentes

| Archivo | Tipo | Rol |
| --- | --- | --- |
| `index.json` | JSON | Configuración efectiva, tareas y cuentas. **Ya no contiene el mapa de archivos.** |
| `secrets.json` | JSON | Secretos de ejecución generados o fijados por entorno. |
| `upload_index.sqlite3` | SQLite | Registro de sincronización: archivos, versiones, assets, releases y desduplicación. |
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
  "github_accounts": {}
}
```

Notas importantes:

- `config` es una instantánea de la configuración efectiva cargada al arrancar. Sirve para auditar qué valores quedaron activos en esa instancia.
- `created_at` marca el instante en que el estado fue creado por primera vez.
- `tasks` siempre contiene, como mínimo, `sync` y `verify`.
- `github_accounts` es un mapa indexado por `account_id`. Cada cuenta conserva su `network` efectivo.
- **`files` ya no vive aquí.** Antes se reescribía el JSON entero en cada guardado (cada 10
  archivos durante una sync) y se parseaba y copiaba en profundidad en cada carga de página
  web; con decenas de miles de archivos eso dominaba el tiempo de arranque. Ahora está en
  `upload_index.sqlite3`, con acceso indexado por fila. Un `index.json` antiguo se importa
  automáticamente al arrancar (una sola vez) y se limpia.

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

#### Registro de archivos (SQLite)

El mapa de archivos vive en `upload_index.sqlite3`. Cada fila de `files` representa un
archivo local observado bajo `APP_DATA_DIR`, con `file_id` (derivado de forma estable de la
ruta relativa) como clave.

Columnas relevantes:

- `rel_path`, `size`, `mtime_ns`: la terna `(size, mtime_ns, status)` es lo que permite
  saltarse un archivo sin hashearlo.
- `source_sha256`: hash del contenido en claro del archivo local.
- `version_id`: la versión vigente. Se escribe **antes** de subir, para que una ejecución
  interrumpida pueda reanudarse contra los mismos nombres de asset.
- `status`: `pending`, `uploading`, `complete` o `error`.
- `present`, `last_seen_at`, `last_seen_run`: presencia. La ausencia se marca por contador
  de ejecución, no por marca de tiempo (que tiene resolución de un segundo).
- `last_error`, `last_verified_at`, `last_verification_ok`, `last_verified_version_id`.

Las versiones subidas se guardan íntegras en la tabla `versions` (una fila por
`file_id` + `version_id`), de modo que el historial que muestra la web y los metadatos que
necesita la verificación de datos legados sobreviven sin cambios.

Cada versión incluye, como mínimo, `version_id`, `created_at`, `storage`,
`plaintext_sha256`, `size`, `mtime_ns`, `source_sha256`, `account_id`, `encryption`,
`copies`, `copy_count_requested`, `copy_count_completed`, `replication_complete`,
`copy_errors` y `uploaded_bytes`.

`storage` distingue los dos formatos remotos y es lo que despacha la verificación:

- `release` (formato actual): cada copia trae `parts`, con `part`, `parts`, `name`,
  `asset_id`, `release_tag`, `size`, `part_plaintext_sha256` y `nonce_b64`.
- `blob` (legado): cada copia trae `chunks`, con `index`, `path`, `raw_url`, `sha256` y
  `size`, más un `encryption.nonce_b64` único para toda la versión.

El bloque `copies` contiene las réplicas completas de una misma versión. Cada copia vive
por entero dentro de una sola cuenta.

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
- `index.json` ya no se reescribe durante el escaneo: `sync` escribe fila a fila en el
  registro SQLite, así que no hay que elegir entre perder progreso y reescribir el estado
  completo cada pocos archivos.
- `verify` solo escribe en el registro local (`last_verified_at` y las diferencias
  encontradas). Nunca escribe en GitHub ni modifica `/datos`.

### Migraciones manuales

Si tu `index.json` es anterior a este cambio, puedes migrarlo manualmente con:

```bash
python3 migrations/001_add_network_and_copies.py /state/index.json
```

La migración:

- Añade `network=github` a las cuentas y repositorios existentes.
- Envuelve cada version antigua en `copies` con una sola copia.
- Conserva el resto del estado tal cual.

Para pasar el mapa de archivos de `index.json` al registro SQLite:

```bash
python3 migrations/002_index_files_to_registry.py /state --dry-run   # cuenta sin escribir
python3 migrations/002_index_files_to_registry.py /state
```

La aplicación hace esta importación **automáticamente** en el primer arranque tras la
actualización (protegida por un flag, así que solo ocurre una vez); el script existe para
ejecutarla de forma explícita. Es sin pérdida: las versiones basadas en commits se copian
tal cual y siguen siendo verificables.

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

El registro local de sincronización. La conexión se abre con `WAL` y
`synchronous=NORMAL`, y se mantiene una conexión por hilo (la tarea de sync y los workers
web lo consultan a la vez).

```sql
CREATE TABLE files (
  file_id TEXT PRIMARY KEY, rel_path TEXT NOT NULL,
  size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
  source_sha256 TEXT, version_id TEXT,
  status TEXT NOT NULL,              -- pending | uploading | complete | error
  present INTEGER NOT NULL DEFAULT 1,
  last_seen_at TEXT, last_seen_run INTEGER, last_error TEXT,
  last_verified_at TEXT, last_verification_ok INTEGER,
  last_verified_version_id TEXT, last_verification_json TEXT, updated_at TEXT);

CREATE TABLE versions (
  file_id TEXT NOT NULL, version_id TEXT NOT NULL,
  version_json TEXT NOT NULL, created_at TEXT,
  copy_count INTEGER NOT NULL DEFAULT 0,
  distinct_account_copies INTEGER NOT NULL DEFAULT 0,
  storage TEXT NOT NULL DEFAULT 'blob',
  PRIMARY KEY (file_id, version_id));

CREATE TABLE assets (
  name TEXT NOT NULL, file_id TEXT NOT NULL, version_id TEXT NOT NULL,
  part INTEGER NOT NULL, parts INTEGER NOT NULL,
  release_tag TEXT NOT NULL, github_asset_id INTEGER,
  size INTEGER, sha256 TEXT, uploaded_at TEXT,
  PRIMARY KEY (release_tag, name));

CREATE TABLE releases (
  tag TEXT PRIMARY KEY, account_id TEXT, owner TEXT, repo TEXT,
  release_id INTEGER, asset_count INTEGER NOT NULL DEFAULT 0,
  sealed INTEGER NOT NULL DEFAULT 0, created_at TEXT);

CREATE TABLE uploaded_versions (
  source_sha256 TEXT PRIMARY KEY, version_json TEXT NOT NULL,
  first_uploaded_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  copy_count INTEGER NOT NULL DEFAULT 1);
```

Notas:

- `assets` se indexa por `(release_tag, name)` y no solo por `name`: con `COPY_COUNT > 1`
  el mismo nombre determinista existe una vez por cuenta, y colapsarlos haría que una copia
  adoptase el asset de otra cuenta.
- `uploaded_versions` es la tabla de desduplicación por hash de contenido, sin cambios:
  cuando un archivo local reaparece con el mismo `source_sha256`, se reutiliza la versión
  ya subida en vez de subirla otra vez.
- Las estadísticas de la web se calculan con agregados SQL, no materializando el mapa.

### Formato de asset remoto (`SPDR1`)

Cada asset es autodescriptivo, para que los datos sobrevivan a la pérdida total del estado
local:

```
"SPDR1" (5B) | header_len (2B, BE) | header JSON (UTF-8) | ciphertext

header = {nonce_b64, file_id, version_id, part, parts, part_plaintext_sha256}
```

- El header **omite deliberadamente `rel_path`**: `file_id` ya es
  `sha256(rel_path)[:16]`, así que no se filtra nada que no se filtrase antes. El mapeo
  `file_id -> rel_path` vive en el manifiesto consolidado, que está cifrado.
- Nombre del asset: `{file_id}-{version_id}-{part:04d}.bin`. Determinista, que es lo que
  permite preguntar si una parte ya existe antes de volver a subirla (reanudación
  idempotente).
- **Un nonce por parte**, aleatorio de 12 bytes y nunca reutilizado con la misma clave.
  Cada parte es descifrable y verificable de forma independiente.
- **Manifiesto consolidado**: un único asset por release (`manifest.spdr`), no uno por
  archivo. Uno por archivo duplicaría el coste a 2 peticiones/archivo y anularía la mitad
  de la ganancia.
- Al llegar a 1000 assets la release se sella y se abre la siguiente
  (`{GITHUB_REPOSITORY_PREFIX}-{NNNN}`). Esto sustituye a la rotación de repositorios y a
  `GITHUB_REPOSITORY_MAX_SIZE_KB` para los datos nuevos.

### Las dos operaciones

**`sync`** — solo lo nuevo o modificado. No existe ya un modo «completo»: la sync ligera
anterior no detectaba modificaciones (confiaba en la versión persistida y no volvía a
hashear), así que la única forma de notar un cambio era la operación más lenta. Comparar
`(size, mtime_ns)` hace que un solo modo sea a la vez rápido y correcto.

**`verify`** — no escribe ni en `/datos` ni en GitHub. Dos niveles:

- *metadatos, todos los archivos*: presencia y tamaño de cada parte, con un listado por
  release. Son lecturas, así que no consumen el presupuesto de peticiones generadoras de
  contenido. Detecta assets ausentes y truncados.
- *profundo, 1 de cada N*: descarga, `sha256` por parte y cierre AES-GCM contra el hash
  local. La selección rota con el contador de ejecución
  (`int(file_id, 16) % N == run % N`), así que `N=100` cubre todo el conjunto en 100
  ejecuciones sin solaparse, y `N=1` lo comprueba todo en cada una.

### Límites de GitHub y control de peticiones

| Límite | Valor documentado |
| --- | --- |
| Primario | 5.000 peticiones/hora (PAT autenticado) |
| Secundario | 80 peticiones generadoras de contenido/minuto, 500/hora, ≤100 concurrentes |
| API de blobs | 100 MB, en base64 (+33% de bytes) |
| Asset de release | 2 GiB, binario crudo; hasta 1000 assets por release |

El limitador (`app/rate_limit.py`) es **proactivo**: mantiene un token bucket por debajo
del techo documentado y bloquea *antes* de emitir una petición que lo superaría, en vez de
reaccionar a un 403. La parte reactiva (`Retry-After`, `x-ratelimit-remaining: 0`,
`403 secondary rate limit`) es la red de seguridad, y baja la concurrencia a 1 en el primer
aviso de límite secundario.

La documentación **no** dice si las subidas a `uploads.github.com` consumen el presupuesto
de contenido, así que eso hay que establecerlo midiendo: se registran las cabeceras de
rate limit de cada respuesta y un resumen por ejecución, y el presupuesto es configurable
para poder ajustarlo a lo medido.

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
| `GITHUB_BRANCH` | No | `main` | Rama que se inicializa en el repositorio anfitrión de las releases. |
| `GITHUB_UPLOADS_PREFIX` | No | `storage` | Prefijo remoto del formato legado (commits). No se usa para releases. |
| `GITHUB_REPOSITORY_PREFIX` | No | `model` | Prefijo de repositorios gestionados y de tags de release. El valor se normaliza quitando guiones finales. |
| `GITHUB_REPOSITORY_PRIVATE` | No | `true` | Crea repositorios privados por defecto. |
| `GITHUB_REPOSITORY_MAX_SIZE_KB` | Sí | - | Solo afecta a datos legados. Los assets de release no cuentan para el tamaño del repositorio. |
| `GITHUB_ACCOUNT_DAILY_UPLOAD_LIMIT_GB` | Sí | - | Límite diario de subida por cuenta. |
| `GITHUB_PART_SIZE_MB` | No | `1024` | Tamaño de parte. Cada parte cuesta exactamente 1 petición generadora de contenido. Se recorta a `1900 MB` (el techo por asset es 2 GiB). |
| `GITHUB_CHUNK_SIZE_MB` | No | `24` | Fragmentación del formato legado. Hoy solo la usa el camino de Telegram. Máximo efectivo `95 MB`. |
| `GITHUB_CONTENT_REQUESTS_PER_HOUR` | No | `450` | Presupuesto proactivo por hora, por debajo del límite documentado de 500. |
| `GITHUB_CONTENT_REQUESTS_PER_MINUTE` | No | `70` | Presupuesto proactivo por minuto, por debajo del límite documentado de 80. |
| `GITHUB_MAX_CONCURRENCY` | No | `3` | Peticiones concurrentes. Baja automáticamente a 1 al primer límite secundario. |
| `GITHUB_TIMEOUT_SECONDS` | No | `300` | Timeout de peticiones GitHub. |
| `GITHUB_MAX_RETRY` | No | `3` | Número de reintentos HTTP. Solo se reintentan errores transitorios (`429`, `500`, `502`, `503`, `504`). |
| `GITHUB_BACKOFF_SECONDS` | No | `2` | Retardo base entre reintentos. |
| `GITHUB_UPLOAD_SLEEP_MIN_SECONDS` | No | `0` | Sleep artificial. Solo aplica al camino de Telegram. |
| `GITHUB_UPLOAD_SLEEP_MAX_SECONDS` | No | `0` | Ídem. Debe ser mayor o igual que el mínimo. |

#### Variables Telegram por cuenta

El backend de Telegram usa MTProto (Pyrogram) para superar el límite de 50 MB de la Bot API y subir hasta ~2 GB por archivo. Los "repositorios" son **canales privados** y cada copia sube los chunks + un manifiesto JSON al canal.

| Variable | Requerida | Valor por defecto | Efecto |
| --- | --- | --- | --- |
| `TG_ACCOUNT_<n>_API_ID` | Sí, junto con `API_HASH` y `PHONE` | - | API ID de Telegram (entero). Se obtiene en my.telegram.org. |
| `TG_ACCOUNT_<n>_API_HASH` | Sí, junto con `API_ID` y `PHONE` | - | API hash de Telegram. |
| `TG_ACCOUNT_<n>_PHONE` | Sí, junto con `API_ID` y `API_HASH` | - | Número con prefijo internacional (ej. `+34600000000`). El `account_id` resultante es `tg_account_<n>`. |

Reglas de descubrimiento:

- Telegram es **opcional**: si no defines ninguna cuenta `TG_ACCOUNT_<n>_*`, el backend Telegram simplemente no se usa y solo se sube a GitHub.
- Cada cuenta numerada debe definir las **tres** variables (`API_ID`, `API_HASH`, `PHONE`); si falta alguna, la carga de configuración falla.
- Para usar Telegram en tiempo de ejecución hay que instalar `pyrogram` (y `tgcrypto`); el cliente solo se importa cuando hay cuentas configuradas.

#### Variables Telegram de almacenamiento

| Variable | Requerida | Valor por defecto efectivo | Efecto |
| --- | --- | --- | --- |
| `TG_CHANNEL_PREFIX` | No | `spider-model` | Prefijo de los canales privados gestionados (análogo a `GITHUB_REPOSITORY_PREFIX`). Se normaliza quitando guiones finales. |
| `TG_CHANNEL_PRIVATE` | No | `true` | Crea canales privados. |
| `TG_TIMEOUT_SECONDS` | No | `900` | Timeout de operaciones MTProto. Más alto que GitHub porque la descarga MTProto de archivos grandes es notablemente más lenta. |
| `TG_MAX_RETRY` | No | `3` | Número de reintentos por operación. |
| `TG_BACKOFF_SECONDS` | No | `2` | Retardo base del backoff exponencial entre reintentos. Los `FloodWait` respetan además el tiempo exacto que exige Telegram. |

Sesiones MTProto:

- La primera vez que un número inicia sesión, Telegram envía un código por SMS/app. El archivo de sesión (`tg_account_<n>.session`) se guarda en `APP_STATE_DIR` (el volumen `/state`).
- **Login desde la web (recomendado):** en el dashboard, cada cuenta Telegram muestra su estado de sesión y un enlace **«iniciar login / re-autenticar»**. Ese flujo hace el handshake completo (`send_code` → `sign_in` → 2FA si aplica) y deja el `.session` listo, sin generarlo a mano. Al completarse, el cliente en ejecución recarga la sesión nueva automáticamente. La sesión previa se aparta a `.session.bak` y se restaura si cancelas o falla el proceso.
- **Alternativa out-of-band:** también puedes generar el `.session` en local (script interactivo de Pyrogram con el mismo `name`/`workdir`) y montarlo para que el contenedor arranque ya autenticado.
- Si una sesión se revoca (`AUTH_KEY_UNREGISTERED`), el dashboard mostrará `sesión=presente` pero las subidas a Telegram fallarán: usa el enlace de re-autenticación para regenerarla.

#### Variables genéricas de redes
> Afectan a todas las redes por igual

| Variable | Requerida | Valor por defecto efectivo | Efecto |
| --- | --- | --- | --- |
| `COPY_COUNT` | No | `1` | Número de copias de cada versión. Cada copia se coloca entera en una cuenta distinta (de cualquier red: GitHub, Telegram, etc.). |
| `VERIFY_DEEP_EVERY_N` | No | `1` | La verificación profunda cubre 1 archivo de cada N por ejecución, rotando. `N=1` lo comprueba todo cada vez. |

Notas de compatibilidad:

- El sleep artificial entre subidas a GitHub se ha eliminado: el ritmo lo marca el limitador proactivo, y un sleep fijo por blob solo añadía tiempo muerto (~1,8 s por archivo pequeño con el rango 0,25–1,5 s del ejemplo anterior). Las variables siguen existiendo para el camino de Telegram.
- `GITHUB_REPOSITORY_PREFIX` se limpia con `strip("-")`, así que `model`, `model-` y `model--` terminan normalizándose al mismo prefijo efectivo.
- `COPY_COUNT` debe ser menor o igual que el número total de cuentas configuradas en todas las redes (GitHub + Telegram). Dos copias bajo una misma cuenta cuentan como una sola.
- `GITHUB_CHUNK_SIZE_MB` solo afecta ya al camino de Telegram, que sigue cifrando el archivo entero en memoria bajo un único nonce (fuera del alcance de este cambio).
- Telegram (v1): se usa **un único canal por cuenta** (`<TG_CHANNEL_PREFIX>-0001`), sin rotación de canales todavía. La verificación (`verify`) marca las copias de Telegram como `skipped` (no `failed`) hasta que se implemente la descarga MTProto.

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
- `/` → Dashboard (últimas ejecuciones, cuotas, estado, estado de sesión Telegram)
- `/files` → Listado de archivos y versiones
- `/logs` → Logs persistentes
- `/telegram/<account_id>/login` → Login interactivo de Telegram (código + 2FA)
- Acciones manuales: Sync y Verify (dos operaciones, no tres)

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
python3 -m spider_back.main run-once-verify
```

## Estructura de almacenamiento

- **GitHub**: un repositorio anfitrión por cuenta con releases sucesivas (`model-0001`,
  `model-0002`, …), cada una con hasta 1000 assets. Cada copia vive entera dentro de una
  sola cuenta.
- **`/state/index.json`**: configuración efectiva, tareas y cuentas.
- **`/state/secrets.json`**: PIN web, clave de cifrado y secreto Flask persistidos.
- **`/state/upload_index.sqlite3`**: el registro de sincronización (archivos, versiones,
  assets, releases y desduplicación).
- **`/state/tmp/`**: partes cifradas en tránsito. Se borran en cuanto se suben.

## Seguridad

- La aplicación nunca descifra el contenido local (solo compara bytes cifrados).
- La clave de cifrado y el PIN web se generan automáticamente si no se definen y se guardan en `/state/secrets.json`.
- El registro SQLite evita subir de nuevo contenido ya visto con el mismo `source_sha256`.
- Los nombres de asset son deterministas y no hay aleatorización de tiempos: es una
  decisión explícita, no un descuido. Lo determinista es además un **requisito** para poder
  reanudar de forma idempotente.
- El nombre de un asset expone `file_id` (que ya es `sha256(rel_path)[:16]`) y `version_id`,
  no la ruta. La ruta solo aparece en el manifiesto consolidado, cifrado con la misma clave.

## Recomendaciones

- Usa `gocryptfs` en `/datos` para cifrado local fuerte.
- Combina ambos backends para máxima redundancia.
- Monitorea las cuotas diarias para evitar rate limits.

---

**Spider-back** te da un sistema de backup "araña" distribuido, cifrado, verificable y de muy bajo coste usando infraestructuras públicas.
