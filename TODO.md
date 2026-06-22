- [x] La migración de estado debe ser automática: detecta si el estado actual requiere agregar algun campo y lo rellena (sanitiza el estado de la aplicación)
      → `StateManager.load()` ahora persiste de vuelta el estado migrado cuando `migrate_state` añade/normaliza campos (idempotente). Test: `test_load_persists_sanitized_state_to_disk`.
- [x] Implementar telegram_api.py (basado en telegram.py.md)
      → `app/telegram_api.py` (Pyrogram opcional, fábrica de cliente inyectable). Tests: `tests/test_telegram_api.py`.
- [x] Revisar detenidamente si la implementacion de _sync_impl y _verify_impl en service.py es correcta.
      → Encontrados y corregidos 3 bugs de sync + verify ahora comprueba TODAS las copias (network-agnostic). Ver PR.
