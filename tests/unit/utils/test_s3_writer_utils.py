"""Unit tests for s3_writer_utils.resolve_geometry."""

from jobs.utils.s3_writer_utils import resolve_geometry


def test_resolve_geometry_uses_geometry_when_present():
    """Uses geometry WKT when geometry is available."""
    result = resolve_geometry("POINT(1.0 2.0)", None)
    assert result == {"type": "Point", "coordinates": [1.0, 2.0]}


def test_resolve_geometry_falls_back_to_point_when_geometry_absent():
    """Falls back to point WKT when geometry is None."""
    result = resolve_geometry(None, "POINT(3.0 4.0)")
    assert result == {"type": "Point", "coordinates": [3.0, 4.0]}


def test_resolve_geometry_falls_back_to_point_when_geometry_empty_string():
    """Falls back to point WKT when geometry is an empty string."""
    result = resolve_geometry("", "POINT(3.0 4.0)")
    assert result == {"type": "Point", "coordinates": [3.0, 4.0]}


def test_resolve_geometry_prefers_geometry_over_point_when_both_present():
    """Uses geometry over point when both are provided."""
    result = resolve_geometry("POINT(1.0 2.0)", "POINT(9.0 9.0)")
    assert result == {"type": "Point", "coordinates": [1.0, 2.0]}


def test_resolve_geometry_returns_none_when_both_absent():
    """Returns None when both geometry and point are absent."""
    assert resolve_geometry(None, None) is None


def test_resolve_geometry_returns_none_when_both_empty():
    """Returns None when both geometry and point are empty strings."""
    assert resolve_geometry("", "") is None
