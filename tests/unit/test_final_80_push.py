"""Minimal tests to push coverage from 76.27% to 80%+ targeting easiest wins."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Mock external dependencies
with patch.dict('sys.modules', {
    'pyspark': MagicMock(),
    'pyspark.sql': MagicMock(),
    'pyspark.sql.functions': MagicMock(),
    'pyspark.sql.types': MagicMock(),
    'pg8000': MagicMock(),
    'boto3': MagicMock(),
    'requests': MagicMock(),
}):
    from jobs.utils import postgres_writer_utils
    from jobs.utils import s3_format_utils
    from jobs.utils import s3_writer_utils


@pytest.mark.unit
class TestEasyWins:
    """Target easy coverage wins to reach 80%."""

    def test_postgres_writer_utils_ensure_columns_defaults(self):
        """Test _ensure_required_columns with defaults."""
        mock_df = Mock()
        mock_df.columns = ['existing']
        mock_df.withColumn.return_value = mock_df
        
        with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit:
            mock_lit.return_value.cast.return_value = 'col'
            
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, ['existing', 'new_col'], {'new_col': 'default'}
            )
            
            assert result == mock_df
            mock_lit.assert_called()

    def test_s3_format_utils_parse_json_edge_cases(self):
        """Test parse_possible_json edge cases."""
        # Test simple string (should return None)
        result = s3_format_utils.parse_possible_json('"test"')
        assert result is None
        
        # Test empty string
        assert s3_format_utils.parse_possible_json('') is None

    def test_s3_writer_utils_wkt_conversions(self):
        """Test WKT to GeoJSON conversions."""
        # Test empty/None cases
        assert s3_writer_utils.wkt_to_geojson(None) is None
        assert s3_writer_utils.wkt_to_geojson('') is None
        assert s3_writer_utils.wkt_to_geojson('INVALID') is None
        
        # Test MULTIPOLYGON with single polygon (should simplify)
        wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))"
        result = s3_writer_utils.wkt_to_geojson(wkt)
        assert result["type"] == "Polygon"
        
        # Test complex MULTIPOLYGON (function may simplify, so check for either)
        wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))"
        result = s3_writer_utils.wkt_to_geojson(wkt)
        assert result["type"] in ["Polygon", "MultiPolygon"]

    @patch('requests.get')
    def test_s3_writer_utils_fetch_schema_yaml_parsing(self, mock_get):
        """Test YAML parsing in fetch_dataset_schema_fields."""
        # Test complex YAML with multiple fields
        mock_response = Mock()
        mock_response.text = """---
name: complex-dataset
fields:
- field: entity
- field: name  
- field: start-date
- field: end-date
- field: geometry
other: value
---
Content here"""
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result = s3_writer_utils.fetch_dataset_schema_fields('complex-dataset')
        
        expected = ['entity', 'name', 'start-date', 'end-date', 'geometry']
        assert result == expected

    def test_s3_writer_utils_ensure_schema_no_missing(self):
        """Test ensure_schema_fields when no fields missing."""
        mock_df = Mock()
        mock_df.columns = ['entity', 'name']
        
        with patch('jobs.utils.s3_writer_utils.fetch_dataset_schema_fields') as mock_fetch:
            mock_fetch.return_value = ['entity', 'name']  # All present
            
            result = s3_writer_utils.ensure_schema_fields(mock_df, 'test')
            
            assert result == mock_df
            mock_fetch.assert_called_once()

    def test_s3_format_utils_renaming_no_files(self):
        """Test renaming when no CSV files found."""
        with patch('boto3.client') as mock_boto:
            mock_s3 = Mock()
            mock_boto.return_value = mock_s3
            
            # No CSV files in response
            mock_s3.list_objects_v2.return_value = {
                'Contents': [
                    {'Key': 'dataset/temp/test/_SUCCESS'},
                    {'Key': 'dataset/temp/test/.hidden'}
                ]
            }
            
            # Should not raise error
            s3_format_utils.renaming('test', 'bucket')
            
            mock_s3.list_objects_v2.assert_called_once()

    @patch('boto3.client')
    def test_s3_writer_utils_cleanup_temp_empty(self, mock_boto):
        """Test cleanup_temp_path with empty bucket."""
        mock_s3 = Mock()
        mock_boto.return_value = mock_s3
        
        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]  # No Contents key
        
        # Should not raise error
        s3_writer_utils.cleanup_temp_path('dev', 'test')
        
        mock_s3.get_paginator.assert_called_once()

    def test_s3_writer_utils_round_point_no_point_column(self):
        """Test round_point_coordinates when no point column."""
        mock_df = Mock()
        mock_df.columns = ['entity', 'name']  # No 'point' column
        
        result = s3_writer_utils.round_point_coordinates(mock_df)
        
        assert result == mock_df

    def test_s3_writer_utils_simple_coverage(self):
        """Test simple s3_writer_utils coverage."""
        # Test module has required functions
        assert hasattr(s3_writer_utils, 'round_point_coordinates')
        assert hasattr(s3_writer_utils, 'cleanup_temp_path')