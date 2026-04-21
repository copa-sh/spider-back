from __future__ import annotations

import argparse
import logging
import threading

from .config import ConfigError, load_config
from .service import AppService
from .state import StateManager
from .utils import utc_now_iso
from .web import create_web_app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="github-fs daemon")
    sub = parser.add_subparsers(dest="command", required=False)
    sub.add_parser("daemon", help="Ejecuta el demonio completo")
    sub.add_parser("run-once-sync", help="Ejecuta una sincronizacion")
    sub.add_parser("run-once-verify", help="Ejecuta una verificacion")
    parser.set_defaults(command="daemon")
    return parser


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


def run_daemon(service: AppService) -> int:
    sync_thread = SchedulerThread("sync-scheduler", service.config.app_sync_interval_seconds, service.run_sync)
    verify_thread = SchedulerThread("verify-scheduler", service.config.app_verify_interval_seconds, service.run_verify)
    sync_thread.start()
    verify_thread.start()

    LOGGER.info("Demonio iniciado en %s", utc_now_iso())
    LOGGER.info("Web disponible en http://%s:%s", service.config.app_web_host, service.config.app_web_port)
    app = create_web_app(service)
    try:
        app.run(host=service.config.app_web_host, port=service.config.app_web_port, use_reloader=False)
        return 0
    finally:
        sync_thread.stop()
        verify_thread.stop()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        service = bootstrap_service()
        if args.command == "run-once-sync":
            result = service.run_sync()
            LOGGER.info("Sync result: %s", result.summary)
            return 0 if result.ok else 1
        if args.command == "run-once-verify":
            result = service.run_verify()
            LOGGER.info("Verify result: %s", result.summary)
            return 0 if result.ok else 1
        if args.command == "daemon":
            return run_daemon(service)
        parser.error("Comando no soportado")
        return 2
    except ConfigError as exc:
        LOGGER.error(str(exc))
        return 1
    except KeyboardInterrupt:
        LOGGER.info("Interrumpido por el usuario")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
