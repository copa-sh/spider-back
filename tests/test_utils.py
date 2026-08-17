import pytest

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


def test_iter_files_path_order_is_deterministic_and_sorted(tmp_path):
    root = tmp_path / "root"
    (root / "sub").mkdir(parents=True)
    for name in ("z.txt", "a.txt", "m.txt"):
        (root / name).write_text(name, encoding="utf-8")
    for name in ("y.txt", "b.txt"):
        (root / "sub" / name).write_text(name, encoding="utf-8")

    first = [path.relative_to(root).as_posix() for path in iter_files(root, order="path")]
    second = [path.relative_to(root).as_posix() for path in iter_files(root, order="path")]

    assert first == second
    assert first == sorted(first)


def test_iter_files_path_order_streams_without_materializing_the_tree(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    for index in range(5):
        (root / f"f{index}.txt").write_text("x", encoding="utf-8")

    walker = iter_files(root, order="path")
    # A generator must yield before the whole tree has been walked.
    assert next(iter(walker)).name == "f0.txt"


def test_iter_files_spread_order_is_deterministic(tmp_path):
    """The previous implementation reshuffled on every run, so an interrupted
    sync was unreproducible. Spread order must be stable."""
    root = tmp_path / "root"
    root.mkdir()
    for index in range(30):
        (root / f"f{index:02d}.txt").write_text("x", encoding="utf-8")

    first = [path.name for path in iter_files(root)]
    second = [path.name for path in iter_files(root)]

    assert first == second
    assert len(first) == 30
    # ...and it is genuinely not path order.
    assert first != sorted(first)


def test_iter_files_spread_order_is_the_default():
    import inspect

    assert inspect.signature(iter_files).parameters["order"].default == "spread"


def test_iter_files_spread_order_covers_the_whole_tree_early(tmp_path):
    """The point of spreading: a sync interrupted part way through must leave a
    bit of everything backed up, not just the first region of the tree. On a
    date-organised corpus, path order means only the oldest files."""
    root = tmp_path / "root"
    years = ["2019", "2020", "2021", "2022", "2023"]
    for year in years:
        (root / year).mkdir(parents=True)
        for index in range(20):
            (root / year / f"foto{index:02d}.jpg").write_text("x", encoding="utf-8")

    spread = [path.relative_to(root).as_posix() for path in iter_files(root)]
    assert len(spread) == 100

    # Stopping after a quarter of the walk still touches every year.
    quarter = spread[:25]
    assert {path.split("/")[0] for path in quarter} == set(years)

    # Path order, by contrast, would have covered only the earliest years.
    by_path = [path.relative_to(root).as_posix() for path in iter_files(root, order="path")]
    assert {path.split("/")[0] for path in by_path[:25]} == {"2019", "2020"}


def test_iter_files_spread_order_matches_stable_file_id_order(tmp_path):
    root = tmp_path / "root"
    (root / "sub").mkdir(parents=True)
    for name in ("a.txt", "b.txt", "c.txt"):
        (root / name).write_text("x", encoding="utf-8")
        (root / "sub" / name).write_text("x", encoding="utf-8")

    walked = [path.relative_to(root).as_posix() for path in iter_files(root)]
    assert walked == sorted(walked, key=stable_file_id)


def test_iter_files_rejects_an_unknown_order(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(ValueError, match="Orden de escaneo"):
        list(iter_files(root, order="aleatorio"))
