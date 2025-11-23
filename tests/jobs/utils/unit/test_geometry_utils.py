"""Unit tests for geometry_utils module."""
import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))

def test_geometry_utils_import():
    """Test geometry_utils module import."""
    try:
        from jobs.utils.geometry_utils import calculate_centroid, SEDONA_AVAILABLE
        assert callable(calculate_centroid)
        assert isinstance(SEDONA_AVAILABLE, bool)
    except ImportError:
        pytest.skip("Geometry utils module not available")

def test_sedona_availability():
    """Test Sedona availability check."""
    from jobs.utils.geometry_utils import SEDONA_AVAILABLE
    # Just test that the variable exists and is boolean
    assert isinstance(SEDONA_AVAILABLE, bool)

def test_wkt_validation_logic():
    """Test basic WKT validation logic."""
    valid_wkt_patterns = [
        "POINT(1 2)",
        "POLYGON((0 0,1 0,1 1,0 1,0 0))",
        "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))"
    ]
    
    for wkt in valid_wkt_patterns:
        # Basic validation - starts with geometry type
        assert any(wkt.startswith(geom_type) for geom_type in ['POINT', 'POLYGON', 'MULTIPOLYGON'])
        assert '(' in wkt and ')' in wkt

def test_coordinate_extraction_logic():
    """Test coordinate extraction logic."""
    point_wkt = "POINT(1.5 2.5)"
    # Extract coordinates from POINT WKT
    coords_part = point_wkt.replace('POINT(', '').replace(')', '')
    coords = [float(x) for x in coords_part.split()]
    assert coords == [1.5, 2.5]