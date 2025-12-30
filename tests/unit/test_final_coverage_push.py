"""Final targeted tests to push coverage from 76.27% to 80%+ by covering remaining high-impact lines."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, date
import json
import hashlib

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Mock all external dependencies
with patch.dict('sys.modules', {
    'pyspark': MagicMock(),
    'pyspark.sql': MagicMock(),
    'pyspark.sql.functions': MagicMock(),
    'pyspark.sql.types': MagicMock(),
    'pyspark.sql.window': MagicMock(),
    'pg8000': MagicMock(),
    'boto3': MagicMock(),
    'requests': MagicMock(),
}):
    from jobs.utils import postgres_writer_utils
    from jobs.utils import s3_format_utils
    from jobs.utils import s3_writer_utils


def create_mock_df(columns=None, count_return=100):
    """Create a comprehensive mock DataFrame."""
    mock_df = Mock()
    mock_df.columns = columns or ['entity', 'name']
    mock_df.count.return_value = count_return
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.repartition.return_value = mock_df
    mock_df.coalesce.return_value = mock_df
    mock_df.write = Mock()
    mock_df.write.jdbc = Mock()
    mock_df.write.mode.return_value = mock_df.write
    mock_df.write.option.return_value = mock_df.write
    mock_df.write.partitionBy.return_value = mock_df.write
    mock_df.write.parquet = Mock()
    mock_df.write.csv = Mock()
    mock_df.dropDuplicates.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.groupBy.return_value = mock_df
    mock_df.join.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.toLocalIterator.return_value = iter([
        Mock(asDict=lambda: {'entity': 1, 'name': 'test', 'geometry': 'POINT(1 2)', 'entry_date': '2023-01-01'})
    ])
    mock_df.schema = []
    mock_df.dtypes = [('entity', 'bigint'), ('name', 'string')]
    return mock_df


@pytest.mark.unit
class TestPostgresWriterUtilsLines104to256:
    """Target lines 104-256 in postgres_writer_utils.py (missing 71 lines)."""

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('pg8000.connect')
    @patch('time.sleep')
    def test_jdbc_write_retry_mechanism_lines_150_180(self, mock_sleep, mock_connect, mock_show_df, mock_get_secret):
        """Test JDBC write retry mechanism covering lines 150-180."""
        mock_df = create_mock_df(['entity', 'name'], 1000)
        mock_get_secret.return_value = {
            'host': 'localhost', 'port': 5432, 'database': 'test', 
            'user': 'user', 'password': 'pass'
        }
        
        # Mock connection for staging table creation
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.rowcount = 100
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn
        
        # Mock JDBC write to fail first two times, succeed third time
        mock_df.write.jdbc.side_effect = [
            Exception("Connection timeout"),
            Exception("Network error"), 
            None  # Success
        ]
        
        with patch('jobs.utils.postgres_writer_utils._ensure_required_columns') as mock_ensure, \
             patch('hashlib.md5') as mock_md5, \
             patch('jobs.utils.postgres_writer_utils.datetime') as mock_datetime:
            
            mock_ensure.return_value = mock_df
            mock_md5.return_value.hexdigest.return_value = 'abcd1234'
            mock_datetime.now.return_value.strftime.return_value = '20231201_120000'
            
            # This should trigger the retry logic in lines 150-180
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_df, "entity", "test-dataset", "dev"
            )
            
            # Verify retry logic was executed
            assert mock_df.write.jdbc.call_count == 3
            assert mock_sleep.call_count == 2  # Sleep between retries

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('pg8000.connect')
    def test_atomic_transaction_retry_lines_190_230(self, mock_connect, mock_get_secret):
        """Test atomic transaction retry covering lines 190-230."""
        mock_df = create_mock_df(['entity', 'name'], 500)
        mock_get_secret.return_value = {
            'host': 'localhost', 'port': 5432, 'database': 'test', 
            'user': 'user', 'password': 'pass'
        }
        
        # Mock connection behavior for transaction retry
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.rowcount = 100
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn
        
        # Make transaction fail first time, succeed second time
        call_count = 0
        def mock_execute_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if 'INSERT INTO entity' in str(args[0]) and call_count == 1:
                raise Exception("Transaction deadlock")
            return None
        
        mock_cur.execute.side_effect = mock_execute_side_effect
        
        with patch('jobs.utils.postgres_writer_utils._ensure_required_columns') as mock_ensure, \
             patch('jobs.utils.postgres_writer_utils.show_df'), \
             patch('hashlib.md5') as mock_md5, \
             patch('time.sleep'):
            
            mock_ensure.return_value = mock_df
            mock_md5.return_value.hexdigest.return_value = 'abcd1234'
            
            # This should trigger transaction retry logic in lines 190-230
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_df, "entity", "test-dataset", "dev"
            )
            
            # Verify transaction operations
            mock_cur.execute.assert_called()
            mock_conn.commit.assert_called()

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('pg8000.connect')
    def test_cleanup_staging_table_lines_240_256(self, mock_connect, mock_get_secret):
        """Test cleanup staging table covering lines 240-256."""
        mock_df = create_mock_df(['entity'], 100)
        mock_get_secret.return_value = {
            'host': 'localhost', 'port': 5432, 'database': 'test', 
            'user': 'user', 'password': 'pass'
        }
        
        # Mock connection for cleanup
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn
        
        with patch('jobs.utils.postgres_writer_utils._ensure_required_columns') as mock_ensure, \
             patch('jobs.utils.postgres_writer_utils.show_df'), \
             patch('hashlib.md5') as mock_md5:
            
            mock_ensure.return_value = mock_df
            mock_md5.return_value.hexdigest.return_value = 'abcd1234'
            
            # This should trigger cleanup logic in lines 240-256
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_df, "entity", "test-dataset", "dev"
            )
            
            # Verify cleanup operations
            drop_table_calls = [call for call in mock_cur.execute.call_args_list 
                              if 'DROP TABLE' in str(call)]
            assert len(drop_table_calls) >= 1

    def test_ensure_required_columns_comprehensive_types_lines_104_127(self):
        """Test comprehensive column type handling covering lines 104-127."""
        mock_df = create_mock_df(['existing_col'])
        
        # Test all column types mentioned in the function
        required_cols = [
            'entity', 'organisation_entity',  # bigint
            'json', 'geojson', 'geometry', 'point', 'quality', 'name', 'prefix', 'reference', 'typology', 'dataset',  # string
            'entry_date', 'start_date', 'end_date',  # date
            'custom_field1', 'custom_field2'  # default string
        ]
        
        defaults = {
            'entity': 12345,
            'dataset': 'test-dataset',
            'name': 'Test Name'
        }
        
        with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit, \
             patch('jobs.utils.postgres_writer_utils.col') as mock_col, \
             patch('jobs.utils.postgres_writer_utils.to_json') as mock_to_json:
            
            mock_lit.return_value.cast.return_value = 'mocked_column'
            mock_col.return_value.cast.return_value = 'mocked_column'
            mock_to_json.return_value = 'json_string'
            
            # This should cover lines 104-127 with all column type branches
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols, defaults, logger=Mock()
            )
            
            # Verify all column types were processed
            assert mock_df.withColumn.call_count >= len(required_cols) - 1
            mock_lit.assert_called()
            mock_to_json.assert_called()
            assert result == mock_df


@pytest.mark.unit
class TestS3FormatUtilsLines34to152:
    """Target lines 34-152 in s3_format_utils.py (missing 41 lines)."""

    def test_s3_csv_format_comprehensive_json_processing_lines_34_74(self):
        """Test comprehensive JSON processing covering lines 34-74."""
        mock_df = create_mock_df(['json_col1', 'json_col2', 'normal_col'])
        
        # Mock schema with multiple StringType fields
        mock_fields = []
        for col_name in ['json_col1', 'json_col2', 'normal_col']:
            mock_field = Mock()
            mock_field.name = col_name
            mock_field.dataType = Mock()
            mock_field.dataType.__class__.__name__ = 'StringType'
            mock_fields.append(mock_field)
        mock_df.schema = mock_fields
        
        # Mock sample data for JSON detection
        def mock_select_behavior(col_name):
            mock_result = Mock()
            if 'json_col' in col_name:
                mock_row = Mock()
                mock_row.__getitem__.return_value = '{"key": "value", "nested": {"field": "data"}}'
                mock_result.dropna.return_value.limit.return_value.collect.return_value = [mock_row]
            else:
                mock_result.dropna.return_value.limit.return_value.collect.return_value = []
            return mock_result
        
        mock_df.select.side_effect = mock_select_behavior
        
        # Mock JSON key extraction
        mock_df.select.return_value.rdd.flatMap.return_value.distinct.return_value.collect.return_value = [
            'key', 'nested', 'field1', 'field2'
        ]
        
        with patch('jobs.utils.s3_format_utils.parse_possible_json') as mock_parse, \
             patch('jobs.utils.s3_format_utils.from_json') as mock_from_json, \
             patch('jobs.utils.s3_format_utils.regexp_replace') as mock_regexp, \
             patch('jobs.utils.s3_format_utils.when') as mock_when, \
             patch('jobs.utils.s3_format_utils.expr') as mock_expr, \
             patch('jobs.utils.s3_format_utils.col') as mock_col:
            
            mock_parse.return_value = {"key": "value", "nested": {"field": "data"}}
            mock_from_json.return_value = 'parsed_json'
            mock_regexp.return_value = 'cleaned_string'
            mock_when.return_value.otherwise.return_value = 'when_expr'
            mock_expr.return_value = 'expr_result'
            mock_col.return_value = 'col_ref'
            
            # This should cover lines 34-74 with comprehensive JSON processing
            result = s3_format_utils.s3_csv_format(mock_df)
            
            # Verify JSON processing steps
            mock_parse.assert_called()
            mock_from_json.assert_called()
            mock_regexp.assert_called()
            assert result == mock_df

    def test_flatten_s3_json_nested_structs_lines_85_89(self):
        """Test nested struct flattening covering lines 85-89."""
        mock_df = create_mock_df()
        
        # Mock complex nested structure
        mock_df.dtypes = [
            ('flat_col1', 'string'),
            ('struct_col1', 'struct<field1:string,field2:struct<nested1:int,nested2:string>>'),
            ('struct_col2', 'struct<field3:array<string>,field4:map<string,int>>'),
            ('array_col', 'array<struct<item:string>>'),
            ('flat_col2', 'int')
        ]
        
        # Mock struct expansion behavior
        def mock_select_behavior(*args, **kwargs):
            if len(args) > 0 and '.*' in str(args[0]):
                mock_result = Mock()
                mock_result.columns = ['field1', 'field2', 'field3', 'field4']
                return mock_result
            return mock_df
        
        mock_df.select.side_effect = mock_select_behavior
        
        with patch('jobs.utils.s3_format_utils.col') as mock_col:
            mock_col.return_value.alias.return_value = 'aliased_col'
            
            # This should cover lines 85-89 with nested struct processing
            result = s3_format_utils.flatten_s3_json(mock_df)
            
            # Verify struct flattening operations
            mock_df.select.assert_called()
            mock_df.drop.assert_called()
            assert result == mock_df

    @patch('boto3.client')
    def test_renaming_comprehensive_s3_operations_lines_114_152(self, mock_boto_client):
        """Test comprehensive S3 renaming operations covering lines 114-152."""
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Mock list_objects_v2 with multiple files
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'csv/test-dataset.csv/part-00000-uuid1.csv'},
                {'Key': 'csv/test-dataset.csv/part-00001-uuid2.csv'},
                {'Key': 'csv/test-dataset.csv/part-00002-uuid3.csv'},
                {'Key': 'csv/test-dataset.csv/_SUCCESS'},
                {'Key': 'csv/test-dataset.csv/.part-00000.csv.crc'}
            ]
        }
        
        # This should cover lines 114-152 with comprehensive S3 operations
        s3_format_utils.renaming('test-dataset', 'test-bucket')
        
        # Verify all S3 operations
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket='test-bucket', 
            Prefix='csv/test-dataset.csv/'
        )
        
        # Should copy and delete CSV files (not _SUCCESS or .crc files)
        csv_files = [obj for obj in mock_s3.list_objects_v2.return_value['Contents'] 
                    if obj['Key'].endswith('.csv') and 'part-' in obj['Key']]
        
        assert mock_s3.copy_object.call_count >= len(csv_files)
        assert mock_s3.delete_object.call_count >= len(csv_files)


@pytest.mark.unit
class TestS3WriterUtilsLines490to711:
    """Target lines 490-711 in s3_writer_utils.py (high-impact missing lines)."""

    @patch('jobs.utils.s3_writer_utils.read_csv_from_s3')
    @patch('jobs.utils.s3_writer_utils.cleanup_dataset_data')
    @patch('jobs.utils.s3_writer_utils.normalise_dataframe_schema')
    @patch('jobs.utils.s3_writer_utils.calculate_centroid')
    @patch('jobs.utils.s3_writer_utils.flatten_json_column')
    @patch('jobs.utils.s3_writer_utils.ensure_schema_fields')
    @patch('jobs.utils.s3_writer_utils.cleanup_temp_path')
    @patch('jobs.utils.s3_writer_utils.s3_rename_and_move')
    @patch('boto3.client')
    def test_write_to_s3_format_geojson_multipart_upload_lines_600_711(
        self, mock_boto_client, mock_rename, mock_cleanup_temp, mock_ensure_schema,
        mock_flatten_json, mock_calculate_centroid, mock_normalise_schema,
        mock_cleanup_dataset, mock_read_csv
    ):
        """Test GeoJSON multipart upload covering lines 600-711."""
        
        # Create large dataset to trigger multipart upload
        mock_df = create_mock_df(['entity', 'name', 'geometry'], 50000)
        mock_spark = Mock()
        
        # Setup all mocks
        mock_bake_df = create_mock_df(['entity', 'json'])
        mock_read_csv.return_value = mock_bake_df
        mock_cleanup_dataset.return_value = {'objects_deleted': 5, 'errors': []}
        mock_normalise_schema.return_value = mock_df
        mock_calculate_centroid.return_value = mock_df
        mock_flatten_json.return_value = mock_df
        mock_ensure_schema.return_value = mock_df
        
        # Mock S3 client for multipart upload
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        mock_s3.exceptions.ClientError = Exception
        mock_s3.head_object.side_effect = Exception("Not found")
        
        # Mock multipart upload
        mock_mpu = {'UploadId': 'test-upload-id'}
        mock_s3.create_multipart_upload.return_value = mock_mpu
        mock_s3.upload_part.return_value = {'ETag': 'test-etag'}
        
        # Mock large dataset iteration for GeoJSON processing
        large_dataset = []
        for i in range(1000):  # Simulate 1000 records
            mock_row = Mock()
            mock_row.asDict.return_value = {
                'entity': i,
                'name': f'test_{i}',
                'geometry': f'POINT({i} {i+1})',
                'entry_date': '2023-01-01',
                'dataset': 'test-dataset'
            }
            large_dataset.append(mock_row)
        
        mock_df.toLocalIterator.return_value = iter(large_dataset)
        
        with patch('jobs.utils.s3_writer_utils.count_df') as mock_count_df, \
             patch('jobs.utils.s3_writer_utils.show_df') as mock_show_df, \
             patch('jobs.utils.s3_writer_utils.lit') as mock_lit, \
             patch('jobs.utils.s3_writer_utils.wkt_to_geojson') as mock_wkt_to_geojson, \
             patch('json.dumps') as mock_json_dumps:
            
            mock_count_df.return_value = 50000
            mock_lit.return_value = 'dataset_value'
            mock_wkt_to_geojson.return_value = {"type": "Point", "coordinates": [1, 2]}
            mock_json_dumps.return_value = '{"test": "feature"}'
            
            # This should cover lines 600-711 with GeoJSON multipart upload
            result = s3_writer_utils.write_to_s3_format(
                mock_df, "s3://bucket/path", "test-dataset", "entity", mock_spark, "dev"
            )
            
            # Verify multipart upload operations
            mock_s3.create_multipart_upload.assert_called_once()
            mock_s3.upload_part.assert_called()
            mock_s3.complete_multipart_upload.assert_called_once()
            
            # Verify GeoJSON processing
            mock_wkt_to_geojson.assert_called()
            mock_json_dumps.assert_called()
            
            assert result == mock_df

    @patch('requests.get')
    def test_fetch_dataset_schema_fields_yaml_parsing_lines_490_520(self, mock_get):
        """Test YAML parsing in schema field fetching covering lines 490-520."""
        
        # Mock complex YAML frontmatter response
        mock_response = Mock()
        mock_response.text = """---
name: test-dataset
description: Test dataset for validation
fields:
- field: entity
  description: Unique identifier
- field: name
  description: Entity name
- field: start-date
  description: Start date
- field: end-date
  description: End date
- field: geometry
  description: Spatial geometry
collection: test-collection
dataset: test-dataset
---

# Test Dataset

This is a test dataset with multiple fields.

## Usage

Use this dataset for testing purposes.
"""
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # This should cover lines 490-520 with YAML parsing
        result = s3_writer_utils.fetch_dataset_schema_fields('test-dataset')
        
        expected_fields = ['entity', 'name', 'start-date', 'end-date', 'geometry']
        assert result == expected_fields
        
        # Verify GitHub API call
        mock_get.assert_called_once_with(
            'https://raw.githubusercontent.com/digital-land/specification/main/content/dataset/test-dataset.md',
            timeout=10
        )

    def test_wkt_to_geojson_complex_multipolygon_lines_345_355(self):
        """Test complex MULTIPOLYGON conversion covering lines 345-355."""
        
        # Complex MULTIPOLYGON with multiple polygons
        wkt = """MULTIPOLYGON (
            ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1)),
            ((10 10, 14 10, 14 14, 10 14, 10 10))
        )"""
        
        # This should cover lines 345-355 with complex MULTIPOLYGON processing
        result = s3_writer_utils.wkt_to_geojson(wkt)
        
        # Should return MultiPolygon (not simplified to Polygon)
        assert result["type"] == "MultiPolygon"
        assert len(result["coordinates"]) == 2  # Two separate polygons
        
        # First polygon should have exterior and interior rings
        assert len(result["coordinates"][0]) == 2  # Exterior + interior ring
        
        # Second polygon should have only exterior ring
        assert len(result["coordinates"][1]) == 1  # Only exterior ring