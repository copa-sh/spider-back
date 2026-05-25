import json

from github_fs.state import StateManager


def test_bootstrap_secrets_creates_file(tmp_path):
    manager = StateManager(tmp_path)
    secrets, generated = manager.bootstrap_secrets(None, None)
    assert generated["web_pin"] is True
    assert generated["encryption_key"] is True
    assert manager.secrets_path.exists()
    payload = json.loads(manager.secrets_path.read_text(encoding="utf-8"))
    assert payload["web_pin"] == secrets.web_pin


def test_load_initial_state(tmp_path):
    manager = StateManager(tmp_path)
    state = manager.load({"repository": "owner/repo"})
    assert state["files"] == {}
    assert state["tasks"]["sync"]["last_result"] == "never"
    assert state["tasks"]["sync_by_name"]["last_result"] == "never"


def test_load_replaces_effective_config(tmp_path):
    manager = StateManager(tmp_path)
    manager.save({"created_at": "x", "config": {"repository": "old/repo"}, "tasks": {}, "files": {}})
    state = manager.load({"repository": "new/repo"})
    assert state["config"]["repository"] == "new/repo"
