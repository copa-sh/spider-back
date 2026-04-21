from __future__ import annotations

from github_fs.runtime import bootstrap_service, start_scheduler_threads, stop_scheduler_threads


_scheduler_threads = None


def when_ready(server):
    global _scheduler_threads
    service = bootstrap_service()
    _scheduler_threads = start_scheduler_threads(service)
    server.log.info("Scheduler de github-fs iniciado en el proceso master de gunicorn.")


def on_exit(server):
    if _scheduler_threads is not None:
        stop_scheduler_threads(*_scheduler_threads)
        server.log.info("Scheduler de github-fs detenido.")
