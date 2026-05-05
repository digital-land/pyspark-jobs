"""Integration tests for list_delta_table_paths."""

from cloudpathlib import AnyPath

from jobs.utils.s3_utils import list_delta_table_paths


def _make_delta_table(base, name):
    """Create a minimal Delta table structure (just the _delta_log directory)."""
    (base / name / "_delta_log").mkdir(parents=True)


def test_finds_delta_tables(tmp_path):
    """Returns paths for directories that contain a _delta_log."""
    _make_delta_table(tmp_path, "entity")
    _make_delta_table(tmp_path, "fact")

    result = list_delta_table_paths(AnyPath(tmp_path))

    assert len(result) == 2
    assert any("entity" in p for p in result)
    assert any("fact" in p for p in result)


def test_ignores_non_delta_directories(tmp_path):
    """Directories without _delta_log are not returned."""
    _make_delta_table(tmp_path, "entity")
    (tmp_path / "not_a_table").mkdir()

    result = list_delta_table_paths(AnyPath(tmp_path))

    assert len(result) == 1
    assert any("entity" in p for p in result)


def test_returns_empty_list_when_no_delta_tables(tmp_path):
    """Returns an empty list when no Delta tables exist."""
    (tmp_path / "some_dir").mkdir()

    result = list_delta_table_paths(AnyPath(tmp_path))

    assert result == []


def test_returns_sorted_paths(tmp_path):
    """Returned paths are sorted alphabetically."""
    _make_delta_table(tmp_path, "fact")
    _make_delta_table(tmp_path, "entity")
    _make_delta_table(tmp_path, "issue")

    result = list_delta_table_paths(AnyPath(tmp_path))

    assert result == sorted(result)
