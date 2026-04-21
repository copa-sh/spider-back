from __future__ import annotations

import logging
import threading

from .config import load_config
from .service import AppService
from .state import StateManager


LOGGER = logging.getLogger("github-fs")


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


def bootstrap_service() -> AppService:
    config = load_config()
    state_manager = StateManager(config.app_state_dir)
    secrets, generated = state_manager.bootstrap_secrets(config.app_web_pin, config.app_encryption_key)
    if generated["encryption_key"]:
        LOGGER.warning("APP_ENCRYPTION_KEY no definido. Clave generada y guardada en /state/secrets.json.")
    if generated["web_pin"]:
        LOGGER.warning("APP_WEB_PIN no definido. PIN generado y guardado en /state/secrets.json: %s", secrets.web_pin)
    if generated["flask_secret_key"]:
        LOGGER.info("Flask secret generado y guardado en /state/secrets.json.")
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
