from __future__ import annotations

from functools import wraps
from threading import Thread

from flask import Flask, abort, redirect, render_template_string, request, session, url_for

from .service import AppService
from .utils import add_seconds_iso


HOME_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>github-fs</h1>
    <p><strong>Ultima sincronizacion:</strong> {{ state.tasks.sync.last_finished_at or "nunca" }} ({{ state.tasks.sync.last_result }})</p>
    <p><strong>Ultima verificacion:</strong> {{ state.tasks.verify.last_finished_at or "nunca" }} ({{ state.tasks.verify.last_result }})</p>
    <p><strong>Proxima sync aprox.:</strong> {{ next_sync }}</p>
    <p><strong>Proxima verificacion aprox.:</strong> {{ next_verify }}</p>
    <p><strong>Sync en curso:</strong> {{ "si" if state.tasks.sync.running else "no" }}</p>
    <p><strong>Verificacion en curso:</strong> {{ "si" if state.tasks.verify.running else "no" }}</p>
    <p><strong>Archivos presentes:</strong> {{ stats.present }}</p>
    <p><strong>Archivos ausentes:</strong> {{ stats.absent }}</p>
    <p><strong>Archivos con error:</strong> {{ stats.with_error }}</p>
    <p><strong>Total de versiones:</strong> {{ stats.total_versions }}</p>
    <form method="post" action="{{ url_for('trigger_sync') }}">
      <button type="submit">Lanzar sync</button>
    </form>
    <form method="post" action="{{ url_for('trigger_verify') }}">
      <button type="submit">Lanzar verificacion</button>
    </form>
    <p><a href="{{ url_for('files') }}">Ver archivos</a></p>
    <p><a href="{{ url_for('logout') }}">Salir</a></p>
  </body>
</html>
"""


LOGIN_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>Acceso github-fs</h1>
    {% if error %}<p>{{ error }}</p>{% endif %}
    <form method="post">
      <label for="pin">PIN</label>
      <input id="pin" name="pin" type="password" autofocus>
      <button type="submit">Entrar</button>
    </form>
  </body>
</html>
"""


FILES_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>Archivos</h1>
    <p><a href="{{ url_for('home') }}">Volver</a></p>
    <ul>
      {% for file in files %}
      <li>
        <a href="{{ url_for('file_detail', file_id=file.file_id) }}">{{ file.path }}</a>
        | presente={{ file.present }}
        | versiones={{ file.versions|length }}
        | error={{ "si" if file.last_error else "no" }}
      </li>
      {% endfor %}
    </ul>
  </body>
</html>
"""


FILE_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>{{ file.path }}</h1>
    <p><a href="{{ url_for('files') }}">Volver</a></p>
    <p><strong>Presente:</strong> {{ file.present }}</p>
    <p><strong>SHA local:</strong> {{ file.source_sha256 }}</p>
    <p><strong>Ultima verificacion:</strong> {{ file.last_verification.checked_at if file.last_verification else "nunca" }}</p>
    <p><strong>Version activa:</strong> {{ file.active_version_id or "ninguna" }}</p>
    <p><strong>Error:</strong> {{ file.last_error or "ninguno" }}</p>
    <h2>Versiones</h2>
    <ul>
      {% for version in file.versions|reverse %}
      <li>
        {{ version.version_id }} | {{ version.created_at }} |
        <a href="{{ version.manifest_raw_url }}">manifest</a> |
        chunks={{ version.chunks|length }} |
        commit={{ version.commit_sha or "pendiente" }} |
        sha={{ version.plaintext_sha256 }}
      </li>
      {% endfor %}
    </ul>
  </body>
</html>
"""


def create_web_app(service: AppService) -> Flask:
    app = Flask(__name__)
    app.secret_key = service.secrets.flask_secret_key
    app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax")

    def require_login(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not session.get("authenticated"):
                return redirect(url_for("login"))
            return view(*args, **kwargs)

        return wrapped

    def run_background(method_name: str) -> None:
        task_name = "sync" if method_name == "run_sync" else "verify"
        service.mark_manual_trigger(task_name)
        target = getattr(service, method_name)
        thread = Thread(target=target, daemon=True)
        thread.start()

    @app.get("/login")
    def login():
        return render_template_string(LOGIN_TEMPLATE, error=None)

    @app.post("/login")
    def login_post():
        pin = request.form.get("pin", "")
        if pin != service.secrets.web_pin:
            return render_template_string(LOGIN_TEMPLATE, error="PIN invalido"), 401
        session["authenticated"] = True
        return redirect(url_for("home"))

    @app.get("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.get("/")
    @require_login
    def home():
        state = service.get_state()
        files = state["files"].values()
        stats = {
            "present": sum(1 for item in files if item.get("present")),
            "absent": sum(1 for item in files if not item.get("present")),
            "with_error": sum(1 for item in files if item.get("last_error")),
            "total_versions": sum(len(item.get("versions", [])) for item in files),
        }
        next_sync = _next_run_text(state["tasks"]["sync"]["last_finished_at"], service.config.app_sync_interval_seconds)
        next_verify = _next_run_text(state["tasks"]["verify"]["last_finished_at"], service.config.app_verify_interval_seconds)
        return render_template_string(
            HOME_TEMPLATE,
            state=state,
            stats=stats,
            next_sync=next_sync,
            next_verify=next_verify,
        )

    @app.get("/files")
    @require_login
    def files():
        state = service.get_state()
        items = sorted(state["files"].values(), key=lambda item: item["path"])
        return render_template_string(FILES_TEMPLATE, files=items)

    @app.get("/files/<file_id>")
    @require_login
    def file_detail(file_id: str):
        state = service.get_state()
        file_entry = state["files"].get(file_id)
        if not file_entry:
            abort(404)
        return render_template_string(FILE_TEMPLATE, file=file_entry)

    @app.post("/actions/sync")
    @require_login
    def trigger_sync():
        run_background("run_sync")
        return redirect(url_for("home"))

    @app.post("/actions/verify")
    @require_login
    def trigger_verify():
        run_background("run_verify")
        return redirect(url_for("home"))

    return app


def _next_run_text(last_finished_at: str | None, interval_seconds: int) -> str:
    if not last_finished_at:
        return "pendiente de primera ejecucion"
    return add_seconds_iso(last_finished_at, interval_seconds)
