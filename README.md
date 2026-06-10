# Spider-back

**Spider-back** es un daemon de respaldo distribuido, cifrado y resiliente que almacena tus datos de forma segura repartiendo fragmentos cifrados entre **GitHub** y/o **Telegram**.

Funciona como una segunda capa de cifrado y fragmentación sobre el contenido de `/datos` (ideal para usar debajo de `gocryptfs`). Trata los archivos locales como bytes opacos, los cifra con AES-256-GCM, los divide en chunks y los distribuye inteligentemente entre los backends configurados.

## Características

- **Backends soportados**: GitHub (múltiples cuentas/repos) y Telegram (múltiples canales privados) — simultáneamente si se desea.
- Cifrado doble: `gocryptfs` (opcional) + AES-256-GCM propio.
- Fragmentación en chunks configurables.
- Detección de cambios por hash + sincronización ligera por nombre (rápida).
- Verificación periódica de integridad reconstruyendo desde los backends remotos.
- Gestión automática de cuotas diarias y creación de repositorios.
- Interfaz web mínima con autenticación por PIN.
- Scheduler integrado para sync y verify automáticos.
- Estado persistente en `/state/index.json`.
- Docker-first.

## Cómo funciona

1. Lee archivos en `/datos` (solo lectura).
2. Cifra cada archivo con AES-256-GCM usando `APP_ENCRYPTION_KEY`.
3. Divide en chunks.
4. Elige aleatoriamente un backend (GitHub o Telegram) con cuota disponible.
5. Sube los chunks y guarda la ubicación en el índice.
6. Periódicamente verifica la integridad descargando y comparando.

## Variables de entorno principales

Copia y edita el archivo correspondiente:

```bash
cp .env.example .env
```

### Configuración común

```env
APP_DATA_DIR=/datos
APP_STATE_DIR=/state
APP_WEB_HOST=0.0.0.0
APP_WEB_PORT=8080
APP_WEB_PIN=                  # Se genera automáticamente si no existe
APP_ENCRYPTION_KEY=           # Se genera automáticamente si no existe

APP_SYNC_INTERVAL_SECONDS=604800
APP_VERIFY_INTERVAL_SECONDS=604800
```

### Backend GitHub

```env
# Cuentas (puedes tener tantas como quieras)
GITHUB_ACCOUNT_1_TOKEN=ghp_xxxxxxxxxxxxxxxx
GITHUB_ACCOUNT_1_OWNER=tuusuario
GITHUB_ACCOUNT_2_TOKEN=ghp_yyyyyyyyyyyyyyyyyyyy
GITHUB_ACCOUNT_2_OWNER=tuorg

GITHUB_BRANCH=main
GITHUB_REPOSITORY_PREFIX=spider-back
GITHUB_REPOSITORY_PRIVATE=true
GITHUB_REPOSITORY_MAX_SIZE_KB=524288
GITHUB_ACCOUNT_DAILY_UPLOAD_LIMIT_GB=5
GITHUB_CHUNK_SIZE_MB=24
```

### Backend Telegram

```env
# Obtenidas en https://my.telegram.org
TELEGRAM_API_ID=1234567
TELEGRAM_API_HASH=abcdef0123456789abcdef0123456789

# Canales privados (IDs suelen empezar por -100)
TELEGRAM_CHANNEL_1_ID=-1001234567890
TELEGRAM_CHANNEL_2_ID=-1000987654321

TELEGRAM_MAX_FILE_SIZE_MB=2000
TELEGRAM_CHANNEL_DAILY_UPLOAD_LIMIT_GB=50
TELEGRAM_CHUNK_SIZE_MB=24
```

## Docker

```bash
# Construir e iniciar
docker compose up -d --build

# Primera vez con Telegram (login)
docker compose run --rm app python3 -m spider_back.main telegram-login
```

Si no definiste `APP_WEB_PIN`, revisa los logs:

```bash
docker compose logs -f app
```

## Interfaz Web

Accede a `http://tu-servidor:8080`

- `GET /login` + `POST /login`
- `/` → Dashboard (últimas ejecuciones, cuotas, estado)
- `/files` → Listado de archivos y versiones
- `/logs` → Logs persistentes
- Acciones manuales: Sync, Sync-by-name, Full Sync, Verify

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
python3 -m spider_back.main run-once-sync-by-name
python3 -m spider_back.main run-once-verify
```

## Estructura de almacenamiento

- **GitHub**: Repositorios automáticos (`spider-back-0001`, `spider-back-0002`, …) con chunks en el branch configurado.
- **Telegram**: Mensajes en canales privados con `message_id` y `file_id`.

Todo el historial de versiones se guarda en `/state/index.json`.

## Seguridad

- La aplicación nunca descifra el contenido local (solo compara bytes cifrados).
- Clave de cifrado generada automáticamente y guardada en `/state/secrets.json`.
- Tokens y sesiones de Telegram se guardan en el volumen persistente `/state`.

## Recomendaciones

- Usa `gocryptfs` en `/datos` para cifrado local fuerte.
- Combina ambos backends para máxima redundancia.
- Monitorea las cuotas diarias para evitar rate limits.

---

**Spider-back** te da un sistema de backup "araña" distribuido, cifrado, verificable y de muy bajo coste usando infraestructuras públicas.
