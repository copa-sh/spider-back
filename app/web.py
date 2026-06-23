from __future__ import annotations

from functools import wraps
from pathlib import Path
from threading import Thread
from typing import Any

from flask import Flask, abort, redirect, render_template_string, request, session, url_for

from .runtime import bootstrap_service
from .service import AppService
from .telegram_api import _PyrogramClient
from .telegram_login import AUTHORIZED, LoginError, TelegramLoginManager
from .utils import add_seconds_iso


HOME_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>spider-back</h1>
    <p><strong>Ultima sync:</strong> {{ state.tasks.sync.last_finished_at or "nunca" }} ({{ state.tasks.sync.last_result }})</p>
    <p><strong>Ultima verificacion:</strong> {{ state.tasks.verify.last_finished_at or "nunca" }} ({{ state.tasks.verify.last_result }})</p>
    <p><strong>Proxima sync programada aprox.:</strong> {{ next_sync }}</p>
    <p><strong>Proxima verificacion aprox.:</strong> {{ next_verify }}</p>
    <p><strong>Sync en curso:</strong> {{ "si" if state.tasks.sync.running else "no" }}</p>
    <p><strong>Verificacion en curso:</strong> {{ "si" if state.tasks.verify.running else "no" }}</p>
    <p><strong>Archivos presentes:</strong> {{ stats.present }}</p>
    <p><strong>Archivos subidos:</strong> {{ stats.uploaded }}</p>
    <p><strong>Archivos verificados:</strong> {{ stats.verified }}</p>
    <p><strong>Archivos ausentes:</strong> {{ stats.absent }}</p>
    <p><strong>Archivos con error:</strong> {{ stats.with_error }}</p>
    <p><strong>Total de versiones:</strong> {{ stats.total_versions }}</p>
    <p><strong>Total de copias:</strong> {{ stats.total_copies }}</p>
    <h2>Cobertura de copias (cuentas distintas)</h2>
    <ul>
      {% for bucket in stats.copy_distribution %}
      <li><strong>{{ bucket.threshold }}+ copias:</strong> {{ bucket.percent }}% ({{ bucket.count }} archivos)</li>
      {% endfor %}
    </ul>
    <h2>Cuentas GitHub</h2>
    <ul>
      {% for account in state.github_account_summaries %}
      <li>
        {{ account.account_id }} ({{ account.owner }}) |
        disponible={{ "si" if account.available else "no" }} |
        hoy={{ account.uploaded_today_bytes }}/{{ account.daily_limit_bytes }} bytes |
        repos={{ account.repositories|length }}
      </li>
      {% endfor %}
    </ul>
    <h2>Cuentas Telegram</h2>
    {% if state.telegram_account_summaries %}
      {% if not state.pyrogram_available %}
      <p><strong>⚠ pyrogram no está instalado.</strong> Las cuentas Telegram no pueden conectarse. Instala: <code>pip install pyrogram tgcrypto</code></p>
      {% endif %}
      <ul>
        {% for account in state.telegram_account_summaries %}
        <li>
          {{ account.account_id }} | phone={{ account.phone }} | api_id={{ account.api_id }} |
          pyrogram={{ "ok" if account.pyrogram_available else "NO INSTALADO" }} |
          disponible={{ "si" if account.available else "no" }} |
          sesión={{ "presente" if account.has_session else "FALTA" }} |
          hoy={{ account.uploaded_today_bytes }} bytes
          {% if account.unavailable_reason %}| ⚠ {{ account.unavailable_reason }}{% endif %}
          | <a href="{{ url_for('telegram_login', account_id=account.account_id) }}">{{ "re-autenticar" if account.has_session else "iniciar login" }}</a>
        </li>
        {% endfor %}
      </ul>
    {% else %}
      <p>No hay cuentas Telegram configuradas. Define <code>TG_ACCOUNT_&lt;n&gt;_API_ID</code>, <code>TG_ACCOUNT_&lt;n&gt;_API_HASH</code> y <code>TG_ACCOUNT_&lt;n&gt;_PHONE</code>.</p>
    {% endif %}
    <h2>Alertas</h2>
    {% if state.github_account_alerts %}
    <table border="1" cellpadding="6" cellspacing="0">
      <thead>
        <tr>
          <th>github_user</th>
          <th>Disponible</th>
          <th>Problema</th>
          <th>Detectado</th>
        </tr>
      </thead>
      <tbody>
        {% for account in state.github_account_alerts %}
          {% for alert in account.alerts %}
          <tr>
            <td>{{ account.owner }} ({{ account.account_id }})</td>
            <td>{{ "si" if account.available else "no" }}</td>
            <td>{{ alert.message }}</td>
            <td>{{ alert.detected_at }}</td>
          </tr>
          {% endfor %}
          {% if not account.alerts and account.unavailable_reason %}
          <tr>
            <td>{{ account.owner }} ({{ account.account_id }})</td>
            <td>{{ "si" if account.available else "no" }}</td>
            <td>{{ account.unavailable_reason }}</td>
            <td>{{ account.unavailable_since or "-" }}</td>
          </tr>
          {% endif %}
        {% endfor %}
      </tbody>
    </table>
    {% else %}
    <p>Sin alertas.</p>
    {% endif %}
    <form method="post" action="{{ url_for('trigger_sync') }}">
      <button type="submit">Sincronización Rápida (Optimizada)</button>
    </form>
    <form method="post" action="{{ url_for('trigger_full_sync') }}">
      <button type="submit">Verificar y Sincronizar Todo</button>
    </form>
    <form method="post" action="{{ url_for('trigger_verify') }}">
      <button type="submit">Lanzar verificacion de integridad</button>
    </form>
    <p><a href="{{ url_for('files') }}">Ver archivos</a></p>
    <p><a href="{{ url_for('logs') }}">Ver logs</a></p>
    <p><a href="{{ url_for('logout') }}">Salir</a></p>
  </body>
</html>
"""


LOGIN_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>Acceso spider-back</h1>
    {% if error %}<p>{{ error }}</p>{% endif %}
    <form method="post">
      <label for="pin">PIN</label>
      <input id="pin" name="pin" type="password" autofocus>
      <button type="submit">Entrar</button>
    </form>
  </body>
</html>
"""


TELEGRAM_LOGIN_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>Login Telegram — {{ account_id }}</h1>
    <p><a href="{{ url_for('home') }}">Volver</a></p>
    <p><strong>Teléfono:</strong> {{ phone }}</p>
    {% if error %}<p style="color:#b00;"><strong>{{ error }}</strong></p>{% endif %}
    {% if notice %}<p style="color:#070;"><strong>{{ notice }}</strong></p>{% endif %}

    {% if state == 'authorized' %}
      <p>✅ Cuenta autenticada. La sesión <code>{{ account_id }}.session</code> está lista.</p>
    {% elif state == 'code_sent' %}
      <p>Telegram ha enviado un código a la app/SMS del número. Introdúcelo:</p>
      <form method="post" action="{{ url_for('telegram_login_code', account_id=account_id) }}">
        <input name="code" inputmode="numeric" autocomplete="one-time-code" autofocus placeholder="12345">
        <button type="submit">Validar código</button>
      </form>
      <form method="post" action="{{ url_for('telegram_login_cancel', account_id=account_id) }}">
        <button type="submit">Cancelar</button>
      </form>
    {% elif state == 'password_needed' %}
      <p>La cuenta tiene verificación en dos pasos (2FA). Introduce la contraseña:</p>
      <form method="post" action="{{ url_for('telegram_login_password', account_id=account_id) }}">
        <input name="password" type="password" autofocus>
        <button type="submit">Validar contraseña</button>
      </form>
      <form method="post" action="{{ url_for('telegram_login_cancel', account_id=account_id) }}">
        <button type="submit">Cancelar</button>
      </form>
    {% else %}
      <p>
        {% if has_session %}Existe una sesión para esta cuenta.{% else %}No hay sesión para esta cuenta.{% endif %}
        Al iniciar el login, la sesión anterior se aparta y se solicita un código nuevo a Telegram.
      </p>
      <form method="post" action="{{ url_for('telegram_login_start', account_id=account_id) }}">
        <button type="submit">Iniciar login / Re-autenticar</button>
      </form>
    {% endif %}
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
    <p><strong>SHA al subir:</strong> {{ file.source_sha256 }}</p>
    <p><strong>Ultima verificacion:</strong> {{ file.last_verification.checked_at if file.last_verification else "nunca" }}</p>
    <p><strong>Version activa:</strong> {{ file.active_version_id or "ninguna" }}</p>
    <p><strong>Error:</strong> {{ file.last_error or "ninguno" }}</p>
    <h2>Versiones</h2>
    <ul>
      {% for version in file.versions|reverse %}
      <li>
        {{ version.version_id }} | {{ version.created_at }} |
        <a href="{{ version.manifest_raw_url }}">manifest</a> |
        copias={{ version.copies|length }} |
        chunks={{ version.chunks|length }} |
        commit={{ version.commit_sha or "pendiente" }} |
        sha={{ version.plaintext_sha256 }} |
        cuenta={{ version.account_id }} |
        repo={{ version.repository_owner }}/{{ version.repository }}
        <ul>
          {% for copy in version.copies %}
          <li>{{ copy.copy_index }}: {{ copy.network }} {{ copy.account_id }} {{ copy.repository_owner }}/{{ copy.repository }} commit={{ copy.commit_sha or "pendiente" }}</li>
          {% endfor %}
        </ul>
      </li>
      {% endfor %}
    </ul>
  </body>
</html>
"""


LOGS_TEMPLATE = """
<!doctype html>
<html lang="es">
  <body>
    <h1>Logs</h1>
    <p><a href="{{ url_for('home') }}">Volver</a></p>
    <p><strong>Fuente:</strong> {{ log_path }}</p>
    <p><strong>Lineas mostradas:</strong> {{ lines }}</p>
    {% if error %}
      <p>{{ error }}</p>
    {% else %}
      <pre style="white-space: pre-wrap;">{{ content }}</pre>
    {% endif %}
  </body>
</html>
"""


def _build_login_manager(service: AppService) -> TelegramLoginManager:
    """Wire a TelegramLoginManager to the running service's accounts and clients."""

    state_dir = service.config.app_state_dir

    def client_factory(account_id: str):
        if _PyrogramClient is None:
            raise LoginError(
                "Pyrogram no está instalado. Instala 'pyrogram' y 'tgcrypto' en el servidor."
            )
        acc = service.telegram_account_by_id[account_id]
        # name + workdir must match the running client so the session lands at
        # <state_dir>/<account_id>.session (see AppService telegram_clients).
        return _PyrogramClient(
            name=account_id,
            api_id=acc.api_id,
            api_hash=acc.api_hash,
            workdir=str(state_dir),
        )

    def phone_for(account_id: str):
        acc = service.telegram_account_by_id.get(account_id)
        return acc.phone if acc else None

    def session_path(account_id: str) -> str:
        return str(state_dir / f"{account_id}.session")

    def on_authorized(account_id: str) -> None:
        # Drop the running server's cached client so it reloads the new session.
        client = service.telegram_clients.get(account_id)
        if client is not None:
            client.reset()

    return TelegramLoginManager(
        client_factory=client_factory,
        phone_for=phone_for,
        session_path=session_path,
        on_authorized=on_authorized,
    )


def create_web_app(service: AppService, login_manager: TelegramLoginManager | None = None) -> Flask:
    app = Flask(__name__)
    app.secret_key = service.secrets.flask_secret_key
    app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax")

    if login_manager is None:
        login_manager = _build_login_manager(service)

    def require_login(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not session.get("authenticated"):
                return redirect(url_for("login"))
            return view(*args, **kwargs)

        return wrapped

    def run_background(method_name: str) -> None:
        task_name_map = {
            "run_sync": "sync",
            "run_full_sync": "sync",
            "run_verify": "verify",
        }
        task_name = task_name_map[method_name]
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
        files = list(state["files"].values())

        def _active_version(item):
            active_id = item.get("active_version_id")
            for version in item.get("versions", []):
                if version.get("version_id") == active_id:
                    return version
            return None

        # Per-file distinct-account copy count of the active version (two copies
        # under the same account count as one — see distinct_account_copy_count).
        per_file_copies = [
            service.distinct_account_copy_count(_active_version(item)) for item in files
        ]
        total_files = len(files)
        copy_count_target = service.config.copy_count
        copy_distribution = []
        for threshold in range(1, copy_count_target + 1):
            with_at_least = sum(1 for count in per_file_copies if count >= threshold)
            percent = round(with_at_least / total_files * 100, 1) if total_files else 0.0
            copy_distribution.append(
                {"threshold": threshold, "count": with_at_least, "percent": percent}
            )

        stats = {
          "present": sum(1 for item in files if item.get("present")),
          "uploaded": sum(1 for item in files if item.get("active_version_id")),
          "verified": sum(
              1
              for item in files
              if (item.get("last_verification") or {}).get("ok") is True
              and (item.get("last_verification") or {}).get("version_id") == item.get("active_version_id")
          ),
          "absent": sum(1 for item in files if not item.get("present")),
          "with_error": sum(1 for item in files if item.get("last_error")),
          "total_versions": sum(len(item.get("versions", [])) for item in files),
          "total_copies": sum(
              len(version.get("copies", []))
              for item in files
              for version in item.get("versions", [])
          ),
          "copy_count_target": copy_count_target,
          "copy_distribution": copy_distribution,
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

    @app.get("/logs")
    @require_login
    def logs():
        requested_lines = request.args.get("lines", "200")
        line_count = _parse_line_count(requested_lines)
        log_path = service.config.app_state_dir / "logs" / "spider-back.log"
        content, error = _read_tail(log_path, line_count)
        return render_template_string(
            LOGS_TEMPLATE,
            content=content,
            error=error,
            lines=line_count,
            log_path=log_path,
        )

    @app.post("/actions/sync")
    @require_login
    def trigger_sync():
        run_background("run_sync")
        return redirect(url_for("home"))

    @app.post("/actions/full-sync")
    @require_login
    def trigger_full_sync():
        run_background("run_full_sync")
        return redirect(url_for("home"))

    @app.post("/actions/verify")
    @require_login
    def trigger_verify():
        run_background("run_verify")
        return redirect(url_for("home"))

    # ── Telegram interactive login ───────────────────────────────────────────
    def _render_telegram_login(account_id: str, *, error=None, notice=None, status=400):
        acc = service.telegram_account_by_id.get(account_id)
        if acc is None:
            abort(404)
        body = render_template_string(
            TELEGRAM_LOGIN_TEMPLATE,
            account_id=account_id,
            phone=acc.phone,
            state=login_manager.state(account_id),
            has_session=login_manager.has_session(account_id),
            error=error,
            notice=notice,
        )
        return (body, status) if error else body

    @app.get("/telegram/<account_id>/login")
    @require_login
    def telegram_login(account_id: str):
        return _render_telegram_login(account_id)

    @app.post("/telegram/<account_id>/login/start")
    @require_login
    def telegram_login_start(account_id: str):
        try:
            login_manager.start(account_id)
        except LoginError as exc:
            return _render_telegram_login(account_id, error=str(exc))
        return redirect(url_for("telegram_login", account_id=account_id))

    @app.post("/telegram/<account_id>/login/code")
    @require_login
    def telegram_login_code(account_id: str):
        code = request.form.get("code", "").strip()
        try:
            new_state = login_manager.submit_code(account_id, code)
        except LoginError as exc:
            return _render_telegram_login(account_id, error=str(exc))
        notice = "Sesión autenticada correctamente." if new_state == AUTHORIZED else None
        return _render_telegram_login(account_id, notice=notice)

    @app.post("/telegram/<account_id>/login/password")
    @require_login
    def telegram_login_password(account_id: str):
        password = request.form.get("password", "")
        try:
            login_manager.submit_password(account_id, password)
        except LoginError as exc:
            return _render_telegram_login(account_id, error=str(exc))
        return _render_telegram_login(account_id, notice="Sesión autenticada correctamente.")

    @app.post("/telegram/<account_id>/login/cancel")
    @require_login
    def telegram_login_cancel(account_id: str):
        login_manager.cancel(account_id)
        return redirect(url_for("telegram_login", account_id=account_id))

    return app


def _next_run_text(last_finished_at: str | None, interval_seconds: int) -> str:
    if not last_finished_at:
        return "pendiente de primera ejecucion"
    return add_seconds_iso(last_finished_at, interval_seconds)


def _parse_line_count(raw_value: str) -> int:
    try:
        parsed = int(raw_value)
    except ValueError:
        return 200
    return min(max(parsed, 1), 1000)


def _read_tail(log_path: Path, line_count: int) -> tuple[str, str | None]:
    if not log_path.exists():
        return "", f"No existe el archivo de logs en {log_path}."

    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return "", f"No se pudo leer el archivo de logs: {exc}"

    tail = lines[-line_count:]
    return "\n".join(tail), None


_default_app: Flask | None = None


def get_default_app() -> Flask:
    global _default_app
    if _default_app is None:
        _default_app = create_web_app(bootstrap_service())
    return _default_app


def app(environ: Any, start_response: Any):
    return get_default_app()(environ, start_response)
