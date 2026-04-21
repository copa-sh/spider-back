# github-fs

Servicio persistente para vigilar un volumen montado en solo lectura, cifrar archivos antes de subirlos a GitHub en chunks, y verificar periódicamente la integridad reconstruyendo los datos desde GitHub.

## Qué hace

- Lee un directorio montado como solo lectura, por ejemplo `/mnt/datos/timeline:/datos:ro`.
- Detecta archivos nuevos o modificados y sube nuevas versiones cifradas a GitHub.
- Mantiene un índice persistente local en `/state/index.json`.
- Expone una web mínima con PIN para ver:
  - última sincronización
  - última verificación de integridad
  - resumen de archivos presentes, ausentes y con error
  - historial de versiones por archivo
- Si faltan `APP_WEB_PIN` o `APP_ENCRYPTION_KEY`, los genera automáticamente y los guarda en `/state/secrets.json`.

## Arquitectura

- `/datos`: volumen de entrada solo lectura.
- `/state`: volumen persistente de escritura para estado, secretos y metadatos.
- GitHub: almacena chunks cifrados y un `manifest.json` por versión.

El índice operativo principal vive en `/state/index.json`. GitHub se usa como almacenamiento remoto inmutable de versiones.

## Instalación con Docker Compose

1. Crea tu `.env` a partir del ejemplo:

```bash
cp .env.example .env
```

2. Ajusta al menos estas variables:

```env
GITHUB_TOKEN=ghp_...
GITHUB_REPOSITORY=owner/repo
GITHUB_BRANCH=main
APP_DATA_BIND=/mnt/datos/timeline
APP_WEB_PORT=8080
```

3. Arranca el servicio:

```bash
docker compose up -d --build
```

4. Consulta el PIN generado si no lo fijaste en `.env`:

```bash
docker compose logs app
```

La web quedará en `http://localhost:8080`.

## docker-compose.yml

El proyecto ya incluye esta configuración:

```yaml
services:
  app:
    build: .
    env_file:
      - .env
    ports:
      - "${APP_WEB_PORT:-8080}:8080"
    volumes:
      - ${APP_DATA_BIND:-/mnt/datos/timeline}:${APP_DATA_DIR:-/datos}:ro
      - app-state:${APP_STATE_DIR:-/state}
    restart: unless-stopped
```

## Variables de entorno

Obligatorias:

- `GITHUB_TOKEN`
- `GITHUB_REPOSITORY`

Opcionales relevantes:

- `GITHUB_BRANCH=main`
- `GITHUB_UPLOADS_PREFIX=storage`
- `GITHUB_CHUNK_SIZE_MB=24`
- `GITHUB_TIMEOUT_SECONDS=300`
- `GITHUB_MAX_RETRY=3`
- `GITHUB_BACKOFF_SECONDS=2`
- `APP_DATA_DIR=/datos`
- `APP_STATE_DIR=/state`
- `APP_WEB_HOST=0.0.0.0`
- `APP_WEB_PORT=8080`
- `APP_SYNC_INTERVAL_SECONDS=604800`
- `APP_VERIFY_INTERVAL_SECONDS=604800`
- `APP_WEB_PIN`
- `APP_ENCRYPTION_KEY`

Por defecto, la revisión de nuevos/modificados y la verificación de integridad se ejecutan una vez por semana.

## Estado local

Archivos persistidos en `/state`:

- `index.json`: estado operativo, tareas y catálogo de archivos.
- `secrets.json`: PIN web, clave de cifrado y `secret_key` de Flask.

Cada archivo se guarda con:

- ruta relativa
- presencia actual en el volumen
- tamaño y `mtime_ns`
- `source_sha256`
- versión activa
- historial de versiones
- último resultado de verificación

Cada versión guarda:

- `version_id`
- fecha de creación
- `plaintext_sha256`
- `ciphertext_sha256`
- nonce AES-GCM
- manifiesto remoto
- lista de chunks con URL y hash

## Política de cambios

- Archivo nuevo: se sube.
- Archivo modificado: se crea nueva versión activa.
- Archivo borrado del volumen: se marca como ausente, pero se conserva el historial.

No se borran versiones remotas de GitHub.

## Verificación de integridad

La verificación:

1. Calcula el SHA-256 del archivo actual en `/datos`.
2. Descarga todos los chunks de la versión activa desde GitHub.
3. Comprueba el hash de cada chunk cifrado.
4. Descifra el contenido con AES-256-GCM.
5. Compara el SHA-256 reconstruido con el del volumen.

## Interfaz web

Rutas principales:

- `GET /login`
- `POST /login`
- `GET /`
- `GET /files`
- `GET /files/<file_id>`
- `POST /actions/sync`
- `POST /actions/verify`

La sesión usa cookie firmada de Flask y acceso por PIN.

## Ejecución fuera de Docker

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python3 -m github_fs.main daemon
```

Comandos auxiliares:

```bash
python3 -m github_fs.main run-once-sync
python3 -m github_fs.main run-once-verify
```

## Desarrollo y pruebas

Instalación de dependencias de desarrollo:

```bash
pip install -r requirements-dev.txt
```

Ejecución de tests:

```bash
python3 -m pytest
```
