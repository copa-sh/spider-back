from __future__ import annotations

import logging

from github_fs.runtime import bootstrap_service, start_scheduler_threads, stop_scheduler_threads


_scheduler_threads = None

accesslog = "-"
errorlog = "-"
loglevel = "info"
capture_output = True


def _configure_app_logging(gunicorn_logger):
    app_logger = logging.getLogger("github-fs")
    app_logger.handlers = gunicorn_logger.handlers
    app_logger.setLevel(gunicorn_logger.level)
    app_logger.propagate = False


def on_starting(server):
    _configure_app_logging(server.log.error_log)


def when_ready(server):
    global _scheduler_threads
    service = bootstrap_service()
    _scheduler_threads = start_scheduler_threads(service)
    server.log.info("Scheduler de github-fs iniciado en el proceso master de gunicorn.")


def on_exit(server):
    if _scheduler_threads is not None:
        stop_scheduler_threads(*_scheduler_threads)
        server.log.info("Scheduler de github-fs detenido.")


def post_fork(server, worker):
    _configure_app_logging(server.log.error_log)
