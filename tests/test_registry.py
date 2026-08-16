from __future__ import annotations

from app.registry import (
    STATUS_COMPLETE,
    STATUS_ERROR,
    STORAGE_BLOB,
    STORAGE_RELEASE,
    Registry,
    distinct_account_copy_count,
    import_legacy_files,
    version_storage,
)


def make_registry(tmp_path) -> Registry:
    return Registry(tmp_path / "upload_index.sqlite3")


def make_version(version_id: str, *, accounts=("account_1",), storage: str = STORAGE_BLOB) -> dict:
    return {
        "version_id": version_id,
        "created_at": "2026-01-01T00:00:00+00:00",
        "plaintext_sha256": "sha-" + version_id,
        "copies": [
            {
                "copy_index": index,
                "network": "github",
                "account_id": account,
                "storage": storage,
                "chunks": [],
            }
            for index, account in enumerate(accounts, start=1)
        ],
    }


def test_unchanged_lookup_is_a_single_indexed_read(tmp_path):
    registry = make_registry(tmp_path)
    registry.upsert_file(
        file_id="abc123", rel_path="a.txt", size=10, mtime_ns=99, status=STATUS_COMPLETE
    )
    row = registry.get_file("abc123")
    assert row is not None
    assert (row.size, row.mtime_ns, row.status) == (10, 99, STATUS_COMPLETE)
    assert row.present is True


def test_upsert_preserves_version_id_when_not_supplied(tmp_path):
    registry = make_registry(tmp_path)
    registry.upsert_file(
        file_id="f1", rel_path="a.txt", size=1, mtime_ns=1, status=STATUS_COMPLETE, version_id="v1"
    )
    registry.upsert_file(
        file_id="f1", rel_path="a.txt", size=2, mtime_ns=2, status=STATUS_ERROR, last_error="boom"
    )
    row = registry.get_file("f1")
    assert row.version_id == "v1"
    assert row.status == STATUS_ERROR
    assert row.last_error == "boom"


def test_mark_absent_is_keyed_on_the_run_not_a_timestamp(tmp_path):
    registry = make_registry(tmp_path)
    registry.upsert_file(
        file_id="seen", rel_path="a.txt", size=1, mtime_ns=1, status=STATUS_COMPLETE, seen_run=7
    )
    registry.upsert_file(
        file_id="gone", rel_path="b.txt", size=1, mtime_ns=1, status=STATUS_COMPLETE, seen_run=6
    )

    # Both rows were written within the same second; only the run counter can
    # tell them apart.
    assert registry.mark_absent_except_run(7) == 1
    assert registry.get_file("seen").present is True
    assert registry.get_file("gone").present is False


def test_clear_verification_drops_stale_results(tmp_path):
    registry = make_registry(tmp_path)
    registry.upsert_file(
        file_id="f1", rel_path="a.txt", size=1, mtime_ns=1, status=STATUS_COMPLETE, version_id="v1"
    )
    registry.record_verification("f1", ok=True, version_id="v1", detail={"ok": True})
    assert registry.verification_detail("f1") == {"ok": True}

    registry.clear_verification("f1")
    assert registry.verification_detail("f1") is None
    assert registry.get_file("f1").last_verification_ok is None


def test_stats_are_computed_in_sql(tmp_path):
    registry = make_registry(tmp_path)
    for index in range(3):
        file_id = f"f{index}"
        registry.upsert_file(
            file_id=file_id,
            rel_path=f"{index}.txt",
            size=1,
            mtime_ns=1,
            status=STATUS_COMPLETE,
            version_id="v1",
        )
        registry.save_version(file_id, make_version("v1", accounts=("a1", "a2")), distinct_account_copies=2)
    registry.record_verification("f0", ok=True, version_id="v1", detail={})
    registry.record_verification("f1", ok=True, version_id="other", detail={})
    registry.mark_missing("f2", error="ausente")

    stats = registry.stats(copy_count_target=2)
    assert stats["total"] == 3
    assert stats["present"] == 2
    assert stats["absent"] == 1
    assert stats["uploaded"] == 3
    # Only f0's verification matches its active version.
    assert stats["verified"] == 1
    assert stats["with_error"] == 1
    assert stats["total_copies"] == 6
    assert stats["copy_distribution"][1] == {"threshold": 2, "count": 3, "percent": 100.0}


def test_release_rollover_bookkeeping(tmp_path):
    registry = make_registry(tmp_path)
    registry.upsert_release(
        tag="model-0001", account_id="account_1", owner="owner-a", repo="store", release_id=10
    )
    assert registry.open_release_for_account("account_1").tag == "model-0001"

    registry.seal_release("model-0001")
    assert registry.open_release_for_account("account_1") is None
    assert registry.next_release_index("account_1") == 2


def test_sync_release_asset_count_seals_at_the_limit(tmp_path, monkeypatch):
    monkeypatch.setattr("app.registry.RELEASE_ASSET_LIMIT", 2)
    registry = make_registry(tmp_path)
    registry.upsert_release(
        tag="model-0001", account_id="account_1", owner="owner-a", repo="store", release_id=1
    )
    for part in range(2):
        registry.upsert_asset(
            name=f"f1-v1-{part:04d}.bin",
            file_id="f1",
            version_id="v1",
            part=part,
            parts=2,
            release_tag="model-0001",
        )
    assert registry.sync_release_asset_count("model-0001") == 2
    assert registry.get_release("model-0001").sealed is True


def test_version_storage_dispatch():
    assert version_storage(make_version("v1")) == STORAGE_BLOB
    assert version_storage(make_version("v1", storage=STORAGE_RELEASE)) == STORAGE_RELEASE
    assert version_storage(None) == STORAGE_BLOB


def test_distinct_account_copy_count_ignores_duplicates_on_one_account():
    version = make_version("v1", accounts=("a1", "a1", "a2"))
    assert len(version["copies"]) == 3
    assert distinct_account_copy_count(version) == 2


def test_import_legacy_files_is_lossless(tmp_path):
    registry = make_registry(tmp_path)
    legacy = {
        "f1": {
            "file_id": "f1",
            "path": "carpeta/a.txt",
            "size": 1234,
            "mtime_ns": 555,
            "source_sha256": "abc",
            "present": True,
            "active_version_id": "v2",
            "last_error": None,
            "last_verification": {"ok": True, "version_id": "v2"},
            "versions": [make_version("v1"), make_version("v2", accounts=("a1", "a2"))],
        }
    }

    assert import_legacy_files(legacy, registry) == 1

    row = registry.get_file("f1")
    assert row.rel_path == "carpeta/a.txt"
    assert row.size == 1234
    assert row.mtime_ns == 555
    assert row.source_sha256 == "abc"
    assert row.version_id == "v2"
    assert row.status == STATUS_COMPLETE
    # Version history survives, so legacy copies stay verifiable via raw_url.
    assert {item["version_id"] for item in registry.list_versions("f1")} == {"v1", "v2"}
    assert registry.get_active_version("f1")["version_id"] == "v2"
    assert registry.verification_detail("f1")["ok"] is True
