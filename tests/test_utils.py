from pathlib import Path

from github_fs.utils import add_seconds_iso, rel_path_str, stable_file_id


def test_stable_file_id_is_stable():
    assert stable_file_id("a/b.txt") == stable_file_id("a/b.txt")


def test_rel_path_str(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    child = root / "a.txt"
    child.write_text("x", encoding="utf-8")
    assert rel_path_str(root, child) == "a.txt"


def test_add_seconds_iso():
    assert add_seconds_iso("2026-01-01T00:00:00+00:00", 60) == "2026-01-01T00:01:00+00:00"
