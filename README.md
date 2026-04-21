# github-fs

Daemon que lee `/datos` en solo lectura, cifra archivos, los reparte entre varias cuentas GitHub y verifica periódicamente la integridad reconstruyendo cada versión desde GitHub.

## Qué hace

- Detecta archivos nuevos o modificados por hash.
- Cifra cada archivo con AES-256-GCM y lo divide en chunks.
- Elige aleatoriamente una cuenta GitHub con cuota diaria disponible.
- Elige aleatoriamente un repositorio gestionado por la app con capacidad disponible o crea uno nuevo automáticamente.
- Guarda en `/state/index.json` en qué cuenta y repo quedó cada versión.
- Verifica integridad descargando los chunks cifrados con el token correcto de su cuenta de origen.
- Expone una web mínima con PIN para ver tareas, archivos, cuentas y repos gestionados.

## Variables de entorno

La aplicación ya no usa `GITHUB_REPOSITORY` como configuración principal. Se definen cuentas numeradas:

```env
GITHUB_ACCOUNT_1_TOKEN=ghp_xxx
GITHUB_ACCOUNT_1_OWNER=mi-usuario
GITHUB_ACCOUNT_2_TOKEN=ghp_yyy
GITHUB_ACCOUNT_2_OWNER=mi-org-o-usuario
```

Variables globales relevantes:

- `GITHUB_BRANCH=main`
- `GITHUB_UPLOADS_PREFIX=storage`
- `GITHUB_REPOSITORY_PREFIX=github-fs`
- `GITHUB_REPOSITORY_PRIVATE=true`
- `GITHUB_REPOSITORY_MAX_SIZE_KB=524288`
- `GITHUB_ACCOUNT_DAILY_UPLOAD_LIMIT_GB=5`
- `GITHUB_CHUNK_SIZE_MB=24`
- `GITHUB_TIMEOUT_SECONDS=300`
- `GITHUB_MAX_RETRY=3`
- `GITHUB_BACKOFF_SECONDS=2`
- `GITHUB_UPLOAD_SLEEP_MIN_SECONDS=0.25`
- `GITHUB_UPLOAD_SLEEP_MAX_SECONDS=1.5`
- `APP_DATA_DIR=/datos`
- `APP_STATE_DIR=/state`
- `APP_WEB_HOST=0.0.0.0`
- `APP_WEB_PORT=8080`
- `APP_SYNC_INTERVAL_SECONDS=604800`
- `APP_VERIFY_INTERVAL_SECONDS=604800`
- `APP_WEB_PIN`
- `APP_ENCRYPTION_KEY`

Si `APP_WEB_PIN` o `APP_ENCRYPTION_KEY` no están definidos, se generan automáticamente y se guardan en `/state/secrets.json`.

## Política de almacenamiento remoto

- Los repositorios se crean automáticamente con el patrón `prefijo-0001`, `prefijo-0002`, etc.
- Antes de usar un repo, la app consulta `GET /repos/{owner}/{repo}` y usa `size` como tamaño actual en KB.
- Cada cuenta tiene un cupo diario por fecha UTC, medido en bytes realmente subidos.
- Cada subida remota aplica un sleep aleatorio entre `GITHUB_UPLOAD_SLEEP_MIN_SECONDS` y `GITHUB_UPLOAD_SLEEP_MAX_SECONDS`.
- Una misma ruta puede tener versiones históricas en cuentas y repos distintos.

## Estado persistente

En `/state/index.json` se guardan:

- tareas de sync y verify
- catálogo de archivos y versiones
- bloque `github_accounts` con:
  - owner por cuenta
  - repos gestionados
  - último tamaño conocido por repo
  - buckets diarios de bytes subidos
- ubicación remota completa por versión:
  - `account_id`
  - `repository_owner`
  - `repository`
  - `branch`
  - `manifest_path`
  - `manifest_raw_url`
  - `commit_sha`
  - chunks con `raw_url` y su cuenta asociada

## Docker Compose

1. Crea tu `.env`:

```bash
cp .env.example .env
```

2. Ajusta tus cuentas GitHub, límites y bind mount de datos.

3. Arranca:

```bash
docker compose up -d --build
```

Esto levanta un solo servicio Docker:

- `app`: web WSGI real con `gunicorn`, y el scheduler de `sync` y `verify` corre dentro del proceso master de Gunicorn

4. Si no definiste PIN, consulta logs:

```bash
docker compose logs app
```

## Web

Rutas disponibles:

- `GET /login`
- `POST /login`
- `GET /`
- `GET /files`
- `GET /files/<file_id>`
- `POST /actions/sync`
- `POST /actions/verify`

La home muestra últimas ejecuciones y resumen de cuotas/repos por cuenta.

## Desarrollo

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python3 -m github_fs.main web-dev
```

Comandos auxiliares:

```bash
python3 -m github_fs.main scheduler
python3 -m github_fs.main run-once-sync
python3 -m github_fs.main run-once-verify
```

Produccion WSGI:

```bash
gunicorn -c gunicorn.conf.py -w 4 -b 0.0.0.0:8083 github_fs.web:app
```

Tests:

```bash
python3 -m pytest
```
