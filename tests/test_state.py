import json

from app.state import StateManager


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
    assert "sync_by_name" not in state["tasks"]
    assert state["tasks"]["verify"]["last_result"] == "never"


def test_load_migrates_legacy_sync_by_name_task(tmp_path):
    manager = StateManager(tmp_path)
    manager.save(
        {
            "created_at": "x",
            "config": {"repository": "old/repo"},
            "tasks": {
                "sync": {"last_result": "success"},
                "sync_by_name": {"last_result": "never"},
                "verify": {"last_result": "never"},
            },
            "files": {},
        }
    )

    state = manager.load({"repository": "new/repo"})
    assert "sync_by_name" not in state["tasks"]
    assert state["tasks"]["sync"]["last_result"] == "success"
    assert state["tasks"]["verify"]["last_result"] == "never"


def test_load_replaces_effective_config(tmp_path):
    manager = StateManager(tmp_path)
    manager.save({"created_at": "x", "config": {"repository": "old/repo"}, "tasks": {}, "files": {}})
    state = manager.load({"repository": "new/repo"})
    assert state["config"]["repository"] == "new/repo"


def test_load_persists_sanitized_state_to_disk(tmp_path):
    manager = StateManager(tmp_path)
    # A legacy index.json missing the fields newer code expects.
    manager.state_path.write_text(
        json.dumps(
            {
                "config": {"repository": "old/repo"},
                "tasks": {"sync": {"last_result": "success"}, "sync_by_name": {}},
                "files": {
                    "f1": {
                        "path": "a.txt",
                        "versions": [
                            {
                                "version_id": "v1",
                                "network": "github",
                                "account_id": "account_1",
                                "chunks": [],
                            }
                        ],
                    }
                },
                "github_accounts": {"account_1": {"owner": "owner-a"}},
            }
        ),
        encoding="utf-8",
    )

    manager.load({"repository": "new/repo"})

    # The migration result must have been written back to disk automatically.
    persisted = json.loads(manager.state_path.read_text(encoding="utf-8"))
    assert "sync_by_name" not in persisted["tasks"]
    assert persisted["tasks"]["verify"]["last_result"] == "never"
    assert persisted["github_accounts"]["account_1"]["network"] == "github"
    assert persisted["github_accounts"]["account_1"]["repositories"] == {}
    version = persisted["files"]["f1"]["versions"][0]
    assert version["copies"]  # legacy single-copy version backfilled into copies[]
    assert version["replication_complete"] is True

    # A second load must be a no-op for the structure (idempotent migration):
    before = manager.state_path.read_text(encoding="utf-8")
    manager.load({"repository": "new/repo"})
    assert manager.state_path.read_text(encoding="utf-8") == before
