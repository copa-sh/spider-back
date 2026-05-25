from __future__ import annotations

import argparse
import logging
import time

from .config import ConfigError
from .runtime import bootstrap_service, start_scheduler_threads, stop_scheduler_threads
from .utils import utc_now_iso
from .web import create_web_app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("github-fs")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="github-fs daemon")
    sub = parser.add_subparsers(dest="command", required=False)
    sub.add_parser("daemon", help="Ejecuta el demonio completo")
    sub.add_parser("scheduler", help="Ejecuta solo los schedulers de sync y verify")
    sub.add_parser("web-dev", help="Ejecuta solo la web con el servidor de desarrollo de Flask")
    sub.add_parser("run-once-sync", help="Ejecuta una sincronizacion")
    sub.add_parser("run-once-sync-by-name", help="Ejecuta una sincronizacion ligera por nombre")
    sub.add_parser("run-once-verify", help="Ejecuta una verificacion")
    parser.set_defaults(command="daemon")
    return parser


def run_daemon(service) -> int:
    sync_thread, verify_thread = start_scheduler_threads(service)
    app = create_web_app(service)
    try:
        LOGGER.info("Demonio iniciado en %s", utc_now_iso())
        LOGGER.info("Web disponible en http://%s:%s", service.config.app_web_host, service.config.app_web_port)
        app.run(host=service.config.app_web_host, port=service.config.app_web_port, use_reloader=False)
        return 0
    finally:
        stop_scheduler_threads(sync_thread, verify_thread)


def run_scheduler(service) -> int:
    sync_thread, verify_thread = start_scheduler_threads(service)
    try:
        LOGGER.info("Scheduler iniciado en %s", utc_now_iso())
        while True:
            time.sleep(3600)
    finally:
        stop_scheduler_threads(sync_thread, verify_thread)


def run_web_dev(service) -> int:
    LOGGER.info("Web dev disponible en http://%s:%s", service.config.app_web_host, service.config.app_web_port)
    app = create_web_app(service)
    app.run(host=service.config.app_web_host, port=service.config.app_web_port, use_reloader=False)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        service = bootstrap_service()
        if args.command == "run-once-sync":
            result = service.run_sync()
            LOGGER.info("Sync result: %s", result.summary)
            return 0 if result.ok else 1
        if args.command == "run-once-sync-by-name":
            result = service.run_sync_by_name()
            LOGGER.info("Sync by name result: %s", result.summary)
            return 0 if result.ok else 1
        if args.command == "run-once-verify":
            result = service.run_verify()
            LOGGER.info("Verify result: %s", result.summary)
            return 0 if result.ok else 1
        if args.command == "scheduler":
            return run_scheduler(service)
        if args.command == "web-dev":
            return run_web_dev(service)
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
