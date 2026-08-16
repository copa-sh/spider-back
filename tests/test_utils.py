from app.utils import add_seconds_iso, iter_files, rel_path_str, stable_file_id


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


def test_iter_files_yields_all_files(tmp_path):
    root = tmp_path / "root"
    (root / "b").mkdir(parents=True)
    (root / "a.txt").write_text("a", encoding="utf-8")
    (root / "b" / "c.txt").write_text("c", encoding="utf-8")

    paths = {path.relative_to(root).as_posix() for path in iter_files(root)}
    assert paths == {"a.txt", "b/c.txt"}


def test_iter_files_is_deterministically_ordered(tmp_path):
    """The walk used to shuffle, which destroyed locality and resumability: an
    interrupted sync restarted in a different order every time."""
    root = tmp_path / "root"
    (root / "sub").mkdir(parents=True)
    for name in ("z.txt", "a.txt", "m.txt"):
        (root / name).write_text(name, encoding="utf-8")
    for name in ("y.txt", "b.txt"):
        (root / "sub" / name).write_text(name, encoding="utf-8")

    first = [path.relative_to(root).as_posix() for path in iter_files(root)]
    second = [path.relative_to(root).as_posix() for path in iter_files(root)]

    assert first == second
    assert first == sorted(first)


def test_iter_files_streams_without_materializing_the_tree(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    for index in range(5):
        (root / f"f{index}.txt").write_text("x", encoding="utf-8")

    walker = iter_files(root)
    # A generator must yield before the whole tree has been walked.
    assert next(iter(walker)).name == "f0.txt"
