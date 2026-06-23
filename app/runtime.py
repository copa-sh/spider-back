from __future__ import annotations

import logging
from pathlib import Path
import threading

from .config import load_config
from .service import AppService
from .state import StateManager


LOGGER = logging.getLogger("spider-back")
LOG_FORMAT = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")


def configure_file_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _attach_file_handler(LOGGER, log_path)


def _attach_file_handler(logger: logging.Logger, log_path: Path) -> None:
    resolved_path = log_path.resolve()
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            try:
                if Path(handler.baseFilename).resolve() == resolved_path:
                    return
            except OSError:
                continue

    handler = logging.FileHandler(resolved_path, encoding="utf-8")
    handler.setFormatter(LOG_FORMAT)
    logger.addHandler(handler)


class SchedulerThread(threading.Thread):
    def __init__(self, name: str, interval_seconds: int, target):
        super().__init__(name=name, daemon=True)
        self.interval_seconds = interval_seconds
        self.target = target
        self.stop_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.is_set():
            self.target()
            self.stop_event.wait(self.interval_seconds)

    def stop(self) -> None:
        self.stop_event.set()


def _check_pyrogram_available() -> bool:
    try:
        import pyrogram  # noqa: F401
        return True
    except ImportError:
        return False


def _log_telegram_startup(config) -> None:
    if not config.telegram_accounts:
        LOGGER.info(
            "Telegram: sin cuentas configuradas "
            "(define TG_ACCOUNT_<n>_API_ID, TG_ACCOUNT_<n>_API_HASH y TG_ACCOUNT_<n>_PHONE para activar)."
        )
        return

    pyrogram_ok = _check_pyrogram_available()
    LOGGER.info("Telegram: %s cuenta(s) detectada(s). pyrogram=%s", len(config.telegram_accounts), "ok" if pyrogram_ok else "NO INSTALADO")
    for acc in config.telegram_accounts:
        phone = acc.phone
        phone_display = phone[:4] + "****" + phone[-2:] if len(phone) > 6 else phone
        LOGGER.info(
            "Telegram cuenta: id=%s phone=%s api_id=%s",
            acc.account_id, phone_display, acc.api_id,
        )
    if not pyrogram_ok:
        LOGGER.warning(
            "Telegram: pyrogram no esta instalado — las cuentas Telegram no podran conectarse. "
            "Instala las dependencias: pip install pyrogram tgcrypto"
        )


def bootstrap_service() -> AppService:
    config = load_config()
    configure_file_logging(config.app_state_dir / "logs" / "spider-back.log")
    state_manager = StateManager(config.app_state_dir)
    secrets, generated = state_manager.bootstrap_secrets(config.app_web_pin, config.app_encryption_key)
    if generated["encryption_key"]:
        LOGGER.warning("APP_ENCRYPTION_KEY no definido. Clave generada y guardada en /state/secrets.json.")
    if generated["web_pin"]:
        LOGGER.warning("APP_WEB_PIN no definido. PIN generado y guardado en /state/secrets.json: %s", secrets.web_pin)
    if generated["flask_secret_key"]:
        LOGGER.info("Flask secret generado y guardado en /state/secrets.json.")
    _log_telegram_startup(config)
    service = AppService(config, secrets, state_manager)
    service.state_manager.load(service.default_config)
    return service


def start_scheduler_threads(service: AppService) -> tuple[SchedulerThread, SchedulerThread]:
    sync_thread = SchedulerThread("sync-scheduler", service.config.app_sync_interval_seconds, service.run_sync)
    verify_thread = SchedulerThread("verify-scheduler", service.config.app_verify_interval_seconds, service.run_verify)
    sync_thread.start()
    verify_thread.start()
    return sync_thread, verify_thread


def stop_scheduler_threads(*threads: SchedulerThread) -> None:
    for thread in threads:
        thread.stop()
