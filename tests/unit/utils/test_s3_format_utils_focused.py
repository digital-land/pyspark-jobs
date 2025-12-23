"""
Unit tests for s3_format_utils module.
Tests S3 formatting functionality with proper mocking.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from jobs.utils import s3_format_utils


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame."""
    df = Mock()
    df.schema = Mock()
    df.select.return_value = df
    df.dropna.return_value = df
    df.limit.return_value = df
    df.collect.return_value = []
    df.withColumn.return_value = df
    df.drop.return_value = df
    df.columns = ['col1', 'col2']
    df.dtypes = [('col1', 'string'), ('col2', 'int')]
    return df


@pytest.mark.unit
class TestParsePossibleJson:
    """Test parse_possible_json helper function."""

    def test_parse_possible_json_none(self):
        """Test parsing None value."""
        result = s3_format_utils.parse_possible_json(None)
        assert result is None

    def test_parse_possible_json_valid_json(self):
        """Test parsing valid JSON string."""
        json_str = '{"key": "value"}'
        result = s3_format_utils.parse_possible_json(json_str)
        assert result == {"key": "value"}

    def test_parse_possible_json_quoted_json(self):
        """Test parsing JSON with outer quotes."""
        json_str = '"{""key"": ""value""}"'
        result = s3_format_utils.parse_possible_json(json_str)
        assert result == {"key": "value"}

    def test_parse_possible_json_double_quotes(self):
        """Test parsing JSON with double quotes."""
        json_str = '{"key": "value"}'
        result = s3_format_utils.parse_possible_json(json_str)
        assert result == {"key": "value"}

    def test_parse_possible_json_invalid(self):
        """Test parsing invalid JSON."""
        result = s3_format_utils.parse_possible_json("not json")
        assert result is None

    def test_parse_possible_json_empty_string(self):
        """Test parsing empty string."""
        result = s3_format_utils.parse_possible_json("")
        assert result is None


@pytest.mark.unit
class TestS3CsvFormat:
    """Test s3_csv_format function."""

    @patch('jobs.utils.s3_format_utils.logger')
    def test_s3_csv_format_no_string_columns(self, mock_logger, mock_dataframe):
        """Test s3_csv_format with no string columns."""
        # Mock schema with no StringType columns
        mock_field = Mock()
        mock_field.name = 'int_col'
        mock_field.dataType = Mock()
        mock_field.dataType.__class__.__name__ = 'IntegerType'
        mock_dataframe.schema = [mock_field]
        
        result = s3_format_utils.s3_csv_format(mock_dataframe)
        
        # Should return original dataframe
        assert result == mock_dataframe
        mock_logger.info.assert_called()

    @patch('jobs.utils.s3_format_utils.logger')
    @patch('jobs.utils.s3_format_utils.StringType')
    def test_s3_csv_format_with_string_columns_no_json(self, mock_string_type, mock_logger, mock_dataframe):
        """Test s3_csv_format with string columns but no JSON data."""
        # Mock schema with StringType columns
        mock_field = Mock()
        mock_field.name = 'string_col'
        mock_field.dataType = mock_string_type()
        mock_dataframe.schema = [mock_field]
        
        # Mock collect to return non-JSON data
        mock_dataframe.collect.return_value = [Mock()]
        mock_dataframe.collect.return_value[0] = ['not json']
        
        with patch('jobs.utils.s3_format_utils.parse_possible_json', return_value=None):
            result = s3_format_utils.s3_csv_format(mock_dataframe)
        
        assert result == mock_dataframe
        mock_logger.info.assert_called()

    @patch('jobs.utils.s3_format_utils.logger')
    @patch('jobs.utils.s3_format_utils.StringType')
    @patch('jobs.utils.s3_format_utils.from_json')
    @patch('jobs.utils.s3_format_utils.MapType')
    def test_s3_csv_format_with_json_columns(self, mock_map_type, mock_from_json, 
                                           mock_string_type, mock_logger, mock_dataframe):
        """Test s3_csv_format with JSON columns."""
        # Mock schema with StringType columns
        mock_field = Mock()
        mock_field.name = 'json_col'
        mock_field.dataType = mock_string_type()
        mock_dataframe.schema = [mock_field]
        
        # Mock collect to return JSON data
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='{"key": "value"}')
        mock_dataframe.collect.return_value = [mock_row]
        
        # Mock RDD operations for key extraction
        mock_rdd = Mock()
        mock_rdd.flatMap.return_value.distinct.return_value.collect.return_value = ['key']
        mock_dataframe.select.return_value.rdd = mock_rdd
        
        with patch('jobs.utils.s3_format_utils.parse_possible_json', return_value={"key": "value"}):
            result = s3_format_utils.s3_csv_format(mock_dataframe)
        
        # Should have called withColumn to add new columns
        mock_dataframe.withColumn.assert_called()
        mock_logger.info.assert_called()

    @patch('jobs.utils.s3_format_utils.logger')
    def test_s3_csv_format_empty_dataframe(self, mock_logger, mock_dataframe):
        """Test s3_csv_format with empty dataframe."""
        mock_dataframe.schema = []
        mock_dataframe.collect.return_value = []
        
        result = s3_format_utils.s3_csv_format(mock_dataframe)
        
        assert result == mock_dataframe


@pytest.mark.unit
class TestFlattenS3Json:
    """Test flatten_s3_json function."""

    def test_flatten_s3_json_no_nested_columns(self, mock_dataframe):
        """Test flattening with no nested columns."""
        mock_dataframe.dtypes = [('col1', 'string'), ('col2', 'int')]
        
        result = s3_format_utils.flatten_s3_json(mock_dataframe)
        
        # Should return dataframe with selected columns
        mock_dataframe.select.assert_called()

    def test_flatten_s3_json_with_struct_columns(self, mock_dataframe):
        """Test flattening with struct columns."""
        mock_dataframe.dtypes = [('col1', 'string'), ('nested', 'struct<field1:string,field2:int>')]
        mock_dataframe.columns = ['col1', 'nested_field1', 'nested_field2']
        
        # Mock the select operation for nested columns
        mock_nested_df = Mock()
        mock_nested_df.columns = ['field1', 'field2']
        mock_dataframe.select.return_value = mock_nested_df
        
        result = s3_format_utils.flatten_s3_json(mock_dataframe)
        
        # Should have called select and drop operations
        mock_dataframe.select.assert_called()
        mock_dataframe.drop.assert_called()

    def test_flatten_s3_json_multiple_nested_levels(self, mock_dataframe):
        """Test flattening with multiple nested levels."""
        # Start with nested struct
        mock_dataframe.dtypes = [
            ('col1', 'string'), 
            ('nested1', 'struct<field1:string>'),
            ('nested2', 'struct<field2:int>')
        ]
        
        # Mock iterative flattening
        def mock_dtypes_side_effect():
            # First call: has nested columns
            # Second call: no more nested columns
            if not hasattr(mock_dtypes_side_effect, 'call_count'):
                mock_dtypes_side_effect.call_count = 0
            mock_dtypes_side_effect.call_count += 1
            
            if mock_dtypes_side_effect.call_count == 1:
                return [('col1', 'string'), ('nested1', 'struct<field1:string>')]
            else:
                return [('col1', 'string'), ('nested1_field1', 'string')]
        
        mock_dataframe.dtypes = property(lambda self: mock_dtypes_side_effect())
        
        result = s3_format_utils.flatten_s3_json(mock_dataframe)
        
        # Should have processed nested columns
        assert result is not None


@pytest.mark.unit
class TestRenaming:
    """Test renaming function."""

    @patch('jobs.utils.s3_format_utils.boto3')
    def test_renaming_basic(self, mock_boto3):
        """Test basic renaming functionality."""
        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3
        
        # Mock list_objects_v2 response
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'csv/test-dataset.csv/part-00000.csv'},
                {'Key': 'csv/test-dataset.csv/part-00001.csv'}
            ]
        }
        
        s3_format_utils.renaming('test-dataset', 'test-bucket')
        
        # Verify S3 operations
        mock_s3.list_objects_v2.assert_called_once()
        mock_s3.copy_object.assert_called_once()
        mock_s3.delete_object.assert_called_once()

    @patch('jobs.utils.s3_format_utils.boto3')
    def test_renaming_no_csv_files(self, mock_boto3):
        """Test renaming with no CSV files."""
        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3
        
        # Mock empty response
        mock_s3.list_objects_v2.return_value = {'Contents': []}
        
        s3_format_utils.renaming('test-dataset', 'test-bucket')
        
        # Should not call copy or delete
        mock_s3.copy_object.assert_not_called()
        mock_s3.delete_object.assert_not_called()

    @patch('jobs.utils.s3_format_utils.boto3')
    def test_renaming_no_part_files(self, mock_boto3):
        """Test renaming with no part files."""
        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3
        
        # Mock response with non-part files
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'csv/test-dataset.csv/other-file.csv'}
            ]
        }
        
        s3_format_utils.renaming('test-dataset', 'test-bucket')
        
        # Should not call copy or delete
        mock_s3.copy_object.assert_not_called()
        mock_s3.delete_object.assert_not_called()


@pytest.mark.unit
class TestFlattenS3Geojson:
    """Test flatten_s3_geojson function."""

    @patch('jobs.utils.s3_format_utils.regexp_extract')
    @patch('jobs.utils.s3_format_utils.struct')
    @patch('jobs.utils.s3_format_utils.lit')
    @patch('jobs.utils.s3_format_utils.array')
    @patch('jobs.utils.s3_format_utils.create_map')
    @patch('jobs.utils.s3_format_utils.collect_list')
    @patch('jobs.utils.s3_format_utils.to_json')
    @patch('builtins.open', create=True)
    def test_flatten_s3_geojson_basic(self, mock_open, mock_to_json, mock_collect_list, 
                                    mock_create_map, mock_array, mock_lit, mock_struct, 
                                    mock_regexp_extract, mock_dataframe):
        """Test basic GeoJSON flattening."""
        # Mock the various Spark functions
        mock_dataframe.columns = ['point', 'name', 'value']
        
        # Mock the final result
        mock_result = Mock()
        mock_result.first.return_value = ['{"type": "FeatureCollection", "features": []}']
        mock_dataframe.select.return_value = mock_result
        
        # Mock file operations
        mock_file = Mock()
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Execute with mocked output_path
        with patch('jobs.utils.s3_format_utils.output_path', '/tmp/test.geojson'):
            s3_format_utils.flatten_s3_geojson(mock_dataframe)
        
        # Verify DataFrame transformations were called
        mock_dataframe.withColumn.assert_called()
        mock_dataframe.select.assert_called()
        
        # Verify file was written
        mock_file.write.assert_called_once()

    def test_flatten_s3_geojson_coordinate_extraction(self, mock_dataframe):
        """Test coordinate extraction from point column."""
        with patch('jobs.utils.s3_format_utils.regexp_extract') as mock_regexp_extract, \
             patch('jobs.utils.s3_format_utils.struct'), \
             patch('jobs.utils.s3_format_utils.lit'), \
             patch('jobs.utils.s3_format_utils.array'), \
             patch('jobs.utils.s3_format_utils.create_map'), \
             patch('jobs.utils.s3_format_utils.collect_list'), \
             patch('jobs.utils.s3_format_utils.to_json'), \
             patch('builtins.open', create=True), \
             patch('jobs.utils.s3_format_utils.output_path', '/tmp/test.geojson'):
            
            mock_dataframe.columns = ['point', 'name']
            mock_result = Mock()
            mock_result.first.return_value = ['{}']
            mock_dataframe.select.return_value = mock_result
            
            s3_format_utils.flatten_s3_geojson(mock_dataframe)
            
            # Verify regexp_extract was called for longitude and latitude
            assert mock_regexp_extract.call_count >= 2