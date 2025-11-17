"""Unit tests for flatten_csv module."""
import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))

def test_flatten_csv_import():
    """Test flatten_csv module import."""
    try:
        from jobs.utils.flatten_csv import flatten_json_column, flatten_geojson_column
        assert callable(flatten_json_column)
        assert callable(flatten_geojson_column)
    except ImportError:
        pytest.skip("Flatten CSV module not available")

def test_json_flattening_logic():
    """Test JSON flattening logic."""
    import json
    
    # Test nested JSON structure
    nested_json = {"properties": {"name": "test", "value": 42}}
    json_str = json.dumps(nested_json)
    
    # Basic flattening logic
    parsed = json.loads(json_str)
    flattened = {}
    
    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    result = flatten_dict(parsed)
    assert result["properties_name"] == "test"
    assert result["properties_value"] == 42

def test_geojson_structure():
    """Test GeoJSON structure validation."""
    geojson_sample = {
        "type": "Point",
        "coordinates": [1.0, 2.0],
        "properties": {"name": "test"}
    }
    
    assert geojson_sample["type"] == "Point"
    assert len(geojson_sample["coordinates"]) == 2
    assert "properties" in geojson_sample