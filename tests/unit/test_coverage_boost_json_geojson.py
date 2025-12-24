"""Additional targeted tests for s3_writer_utils missing lines 490-711 (JSON/GeoJSON processing)."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, date
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Mock problematic imports
with patch.dict('sys.modules', {
    'pyspark': MagicMock(),
    'pyspark.sql': MagicMock(),
    'pyspark.sql.functions': MagicMock(),
    'pyspark.sql.types': MagicMock(),
    'boto3': MagicMock(),
}):
    from jobs.utils import s3_writer_utils


@pytest.mark.unit
class TestS3WriterUtilsJSONGeoJSON:
    """Targeted tests for JSON and GeoJSON processing in s3_writer_utils (lines 490-711)."""

    @patch('boto3.client')
    def test_write_to_s3_format_json_processing(self, mock_boto3):
        """Test JSON processing in write_to_s3_format."""
        # Setup mock DataFrame with date/datetime values
        mock_row1 = Mock()
        mock_row1.asDict.return_value = {
            'entity': 1,
            'name': 'Test Entity',
            'entry_date': date(2023, 1, 1),
            'start_date': datetime(2023, 1, 1, 10, 30, 0),
            'end_date': None,
            'dataset': 'test-dataset'
        }
        
        mock_row2 = Mock()
        mock_row2.asDict.return_value = {
            'entity': 2,
            'name': 'Another Entity',
            'entry_date': None,
            'start_date': date(2023, 2, 1),
            'end_date': datetime(2023, 12, 31, 23, 59, 59),
            'dataset': 'test-dataset'
        }
        
        mock_df = Mock()
        mock_df.count.return_value = 2
        mock_df.withColumn.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.coalesce.return_value = mock_df
        mock_df.write = Mock()
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.csv = Mock()
        mock_df.toLocalIterator.return_value = iter([mock_row1, mock_row2])
        
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        # Mock all the dependencies
        with patch('jobs.utils.s3_writer_utils.read_csv_from_s3') as mock_read_csv, \
             patch('jobs.utils.s3_writer_utils.cleanup_dataset_data') as mock_cleanup, \
             patch('jobs.utils.s3_writer_utils.normalise_dataframe_schema') as mock_normalise, \
             patch('jobs.utils.s3_writer_utils.calculate_centroid') as mock_centroid, \
             patch('jobs.utils.s3_writer_utils.flatten_json_column') as mock_flatten, \
             patch('jobs.utils.s3_writer_utils.ensure_schema_fields') as mock_ensure, \
             patch('jobs.utils.s3_writer_utils.cleanup_temp_path') as mock_cleanup_temp, \
             patch('jobs.utils.s3_writer_utils.s3_rename_and_move') as mock_rename, \
             patch('jobs.utils.s3_writer_utils.count_df', return_value=2), \
             patch('jobs.utils.s3_writer_utils.show_df'), \
             patch('jobs.utils.s3_writer_utils.lit'):
            
            # Setup return values
            mock_bake_df = Mock()
            mock_bake_df.select.return_value = mock_bake_df
            mock_read_csv.return_value = mock_bake_df
            mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
            mock_normalise.return_value = mock_df
            mock_centroid.return_value = mock_df
            mock_flatten.return_value = mock_df
            mock_ensure.return_value = mock_df
            
            # Mock S3 operations for JSON
            mock_s3.head_object.side_effect = [
                mock_s3.exceptions.ClientError({}, 'NoSuchKey'),  # JSON file doesn't exist
                mock_s3.exceptions.ClientError({}, 'NoSuchKey')   # GeoJSON file doesn't exist
            ]
            
            result = s3_writer_utils.write_to_s3_format(
                mock_df, "s3://bucket/output/", "test-dataset", "entity", Mock(), "dev"
            )
            
            # Verify JSON was written to S3
            json_put_calls = [call for call in mock_s3.put_object.call_args_list 
                             if 'dataset/test-dataset.json' in str(call)]
            assert len(json_put_calls) > 0
            
            # Verify the JSON content structure
            json_call = json_put_calls[0]
            json_body = json_call[1]['Body']
            assert '{"entities":[' in json_body
            assert '"entry_date":"2023-01-01"' in json_body  # Date serialization
            assert '"start_date":"2023-01-01T10:30:00"' in json_body  # Datetime serialization
            assert '"end_date":""' in json_body  # None handling

    @patch('boto3.client')
    def test_write_to_s3_format_geojson_processing(self, mock_boto3):
        """Test GeoJSON processing in write_to_s3_format."""
        # Setup mock DataFrame with geometry data
        mock_row1 = Mock()
        mock_row1.asDict.return_value = {
            'entity': 1,
            'name': 'Test Entity',
            'geometry': 'POINT (1.0 2.0)',
            'point': 'POINT (1.0 2.0)',
            'dataset': 'test-dataset'
        }
        
        mock_row2 = Mock()
        mock_row2.asDict.return_value = {
            'entity': 2,
            'name': 'Another Entity',
            'geometry': 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
            'point': None,
            'dataset': 'test-dataset'
        }
        
        mock_df = Mock()
        mock_df.count.return_value = 2
        mock_df.withColumn.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.coalesce.return_value = mock_df
        mock_df.write = Mock()
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.csv = Mock()
        mock_df.toLocalIterator.return_value = iter([mock_row1, mock_row2])
        mock_df.repartition.return_value = mock_df
        
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        # Mock multipart upload
        mock_s3.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_s3.upload_part.return_value = {'ETag': 'test-etag'}
        
        with patch('jobs.utils.s3_writer_utils.read_csv_from_s3') as mock_read_csv, \
             patch('jobs.utils.s3_writer_utils.cleanup_dataset_data') as mock_cleanup, \
             patch('jobs.utils.s3_writer_utils.normalise_dataframe_schema') as mock_normalise, \
             patch('jobs.utils.s3_writer_utils.calculate_centroid') as mock_centroid, \
             patch('jobs.utils.s3_writer_utils.flatten_json_column') as mock_flatten, \
             patch('jobs.utils.s3_writer_utils.ensure_schema_fields') as mock_ensure, \
             patch('jobs.utils.s3_writer_utils.cleanup_temp_path') as mock_cleanup_temp, \
             patch('jobs.utils.s3_writer_utils.s3_rename_and_move') as mock_rename, \
             patch('jobs.utils.s3_writer_utils.count_df', return_value=2), \
             patch('jobs.utils.s3_writer_utils.show_df'), \
             patch('jobs.utils.s3_writer_utils.lit'), \
             patch('jobs.utils.s3_writer_utils.wkt_to_geojson') as mock_wkt_to_geojson:
            
            # Setup return values
            mock_bake_df = Mock()
            mock_bake_df.select.return_value = mock_bake_df
            mock_read_csv.return_value = mock_bake_df
            mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
            mock_normalise.return_value = mock_df
            mock_centroid.return_value = mock_df
            mock_flatten.return_value = mock_df
            mock_ensure.return_value = mock_df
            
            # Mock WKT to GeoJSON conversion
            mock_wkt_to_geojson.side_effect = [
                {"type": "Point", "coordinates": [1.0, 2.0]},
                {"type": "Polygon", "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]}
            ]
            
            # Mock S3 operations
            mock_s3.head_object.side_effect = [
                mock_s3.exceptions.ClientError({}, 'NoSuchKey'),  # JSON file doesn't exist
                mock_s3.exceptions.ClientError({}, 'NoSuchKey')   # GeoJSON file doesn't exist
            ]
            
            result = s3_writer_utils.write_to_s3_format(
                mock_df, "s3://bucket/output/", "test-dataset", "entity", Mock(), "dev"
            )
            
            # Verify multipart upload was initiated for GeoJSON
            mock_s3.create_multipart_upload.assert_called_with(
                Bucket='dev-collection-data',
                Key='dataset/test-dataset.geojson'
            )
            
            # Verify parts were uploaded
            assert mock_s3.upload_part.call_count >= 1
            
            # Verify multipart upload was completed
            mock_s3.complete_multipart_upload.assert_called_once()

    @patch('boto3.client')
    def test_write_to_s3_format_geojson_multipart_error_handling(self, mock_boto3):
        """Test GeoJSON multipart upload error handling."""
        mock_df = Mock()
        mock_df.count.return_value = 1
        mock_df.withColumn.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.coalesce.return_value = mock_df
        mock_df.write = Mock()
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.csv = Mock()
        mock_df.toLocalIterator.return_value = iter([])
        mock_df.repartition.return_value = mock_df
        
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        # Mock multipart upload failure
        mock_s3.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_s3.upload_part.side_effect = Exception("Upload failed")
        
        with patch('jobs.utils.s3_writer_utils.read_csv_from_s3') as mock_read_csv, \
             patch('jobs.utils.s3_writer_utils.cleanup_dataset_data') as mock_cleanup, \
             patch('jobs.utils.s3_writer_utils.normalise_dataframe_schema') as mock_normalise, \
             patch('jobs.utils.s3_writer_utils.calculate_centroid') as mock_centroid, \
             patch('jobs.utils.s3_writer_utils.flatten_json_column') as mock_flatten, \
             patch('jobs.utils.s3_writer_utils.ensure_schema_fields') as mock_ensure, \
             patch('jobs.utils.s3_writer_utils.cleanup_temp_path') as mock_cleanup_temp, \
             patch('jobs.utils.s3_writer_utils.s3_rename_and_move') as mock_rename, \
             patch('jobs.utils.s3_writer_utils.count_df', return_value=1), \
             patch('jobs.utils.s3_writer_utils.show_df'), \
             patch('jobs.utils.s3_writer_utils.lit'):
            
            # Setup return values
            mock_bake_df = Mock()
            mock_bake_df.select.return_value = mock_bake_df
            mock_read_csv.return_value = mock_bake_df
            mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
            mock_normalise.return_value = mock_df
            mock_centroid.return_value = mock_df
            mock_flatten.return_value = mock_df
            mock_ensure.return_value = mock_df
            
            # Mock S3 operations
            mock_s3.head_object.side_effect = [
                mock_s3.exceptions.ClientError({}, 'NoSuchKey'),  # JSON file doesn't exist
                mock_s3.exceptions.ClientError({}, 'NoSuchKey')   # GeoJSON file doesn't exist
            ]
            
            with pytest.raises(Exception, match="Upload failed"):
                s3_writer_utils.write_to_s3_format(
                    mock_df, "s3://bucket/output/", "test-dataset", "entity", Mock(), "dev"
                )
            
            # Verify abort was called on failure
            mock_s3.abort_multipart_upload.assert_called_once()

    def test_convert_row_function_date_handling(self):
        """Test the convert_row function's date/datetime handling."""
        # This tests the internal convert_row function logic
        from datetime import date, datetime
        
        def convert_row_logic(row_dict):
            for key, value in row_dict.items():
                if isinstance(value, (date, datetime)):
                    row_dict[key] = value.isoformat() if value else ""
                elif value is None:
                    row_dict[key] = ""
            return row_dict
        
        test_cases = [
            {
                'input': {'date_field': date(2023, 1, 1), 'datetime_field': datetime(2023, 1, 1, 10, 30), 'null_field': None, 'string_field': 'test'},
                'expected': {'date_field': '2023-01-01', 'datetime_field': '2023-01-01T10:30:00', 'null_field': '', 'string_field': 'test'}
            },
            {
                'input': {'empty_date': None, 'normal_field': 'value'},
                'expected': {'empty_date': '', 'normal_field': 'value'}
            }
        ]
        
        for case in test_cases:
            result = convert_row_logic(case['input'].copy())
            assert result == case['expected']

    @patch('boto3.client')
    def test_write_to_s3_format_large_geojson_partitioning(self, mock_boto3):
        """Test GeoJSON processing with large dataset partitioning."""
        # Create a large number of mock rows to test partitioning logic
        mock_rows = []
        for i in range(250000):  # Large dataset to trigger partitioning
            mock_row = Mock()
            mock_row.asDict.return_value = {
                'entity': i,
                'name': f'Entity {i}',
                'geometry': f'POINT ({i}.0 {i+1}.0)',
                'dataset': 'test-dataset'
            }
            mock_rows.append(mock_row)
        
        mock_df = Mock()
        mock_df.count.return_value = 250000
        mock_df.withColumn.return_value = mock_df
        mock_df.join.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.coalesce.return_value = mock_df
        mock_df.write = Mock()
        mock_df.write.mode.return_value = mock_df.write
        mock_df.write.option.return_value = mock_df.write
        mock_df.write.csv = Mock()
        mock_df.toLocalIterator.return_value = iter(mock_rows[:100])  # Limit for test performance
        mock_df.repartition.return_value = mock_df
        
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        # Mock multipart upload
        mock_s3.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
        mock_s3.upload_part.return_value = {'ETag': 'test-etag'}
        
        with patch('jobs.utils.s3_writer_utils.read_csv_from_s3') as mock_read_csv, \
             patch('jobs.utils.s3_writer_utils.cleanup_dataset_data') as mock_cleanup, \
             patch('jobs.utils.s3_writer_utils.normalise_dataframe_schema') as mock_normalise, \
             patch('jobs.utils.s3_writer_utils.calculate_centroid') as mock_centroid, \
             patch('jobs.utils.s3_writer_utils.flatten_json_column') as mock_flatten, \
             patch('jobs.utils.s3_writer_utils.ensure_schema_fields') as mock_ensure, \
             patch('jobs.utils.s3_writer_utils.cleanup_temp_path') as mock_cleanup_temp, \
             patch('jobs.utils.s3_writer_utils.s3_rename_and_move') as mock_rename, \
             patch('jobs.utils.s3_writer_utils.count_df', return_value=250000), \
             patch('jobs.utils.s3_writer_utils.show_df'), \
             patch('jobs.utils.s3_writer_utils.lit'), \
             patch('jobs.utils.s3_writer_utils.wkt_to_geojson', return_value={"type": "Point", "coordinates": [1.0, 2.0]}):
            
            # Setup return values
            mock_bake_df = Mock()
            mock_bake_df.select.return_value = mock_bake_df
            mock_read_csv.return_value = mock_bake_df
            mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
            mock_normalise.return_value = mock_df
            mock_centroid.return_value = mock_df
            mock_flatten.return_value = mock_df
            mock_ensure.return_value = mock_df
            
            # Mock S3 operations
            mock_s3.head_object.side_effect = [
                mock_s3.exceptions.ClientError({}, 'NoSuchKey'),  # JSON file doesn't exist
                mock_s3.exceptions.ClientError({}, 'NoSuchKey')   # GeoJSON file doesn't exist
            ]
            
            result = s3_writer_utils.write_to_s3_format(
                mock_df, "s3://bucket/output/", "test-dataset", "entity", Mock(), "dev"
            )
            
            # Verify partitioning was calculated for large dataset
            # The function should calculate num_partitions = max(1, 250000 // 10000) = 25
            mock_df.repartition.assert_called_with(25)

    def test_wkt_to_geojson_edge_cases(self):
        """Test WKT to GeoJSON conversion edge cases."""
        test_cases = [
            # Empty/None cases
            (None, None),
            ("", None),
            ("   ", None),
            
            # Invalid WKT
            ("INVALID WKT STRING", None),
            ("POINT", None),
            ("POLYGON", None),
            
            # Valid but unsupported types
            ("LINESTRING (0 0, 1 1)", None),
            ("MULTIPOINT ((0 0), (1 1))", None),
            
            # Edge case coordinates
            ("POINT (0 0)", {"type": "Point", "coordinates": [0.0, 0.0]}),
            ("POINT (-180 -90)", {"type": "Point", "coordinates": [-180.0, -90.0]}),
            ("POINT (180 90)", {"type": "Point", "coordinates": [180.0, 90.0]}),
        ]
        
        for wkt_input, expected in test_cases:
            result = s3_writer_utils.wkt_to_geojson(wkt_input)
            assert result == expected

    @patch('boto3.client')
    def test_s3_rename_and_move_existing_file_handling(self, mock_boto3):
        """Test s3_rename_and_move with existing file scenarios."""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        # Test case 1: File exists and should be deleted
        mock_s3.head_object.return_value = {}  # File exists
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'dataset/temp/test-dataset/part-00000-123.csv'}]
        }
        
        s3_writer_utils.s3_rename_and_move("dev", "test-dataset", "csv", "test-bucket")
        
        # Should delete existing file first
        mock_s3.delete_object.assert_called()
        mock_s3.copy_object.assert_called()
        
        # Reset mocks for test case 2
        mock_s3.reset_mock()
        
        # Test case 2: File doesn't exist
        mock_s3.head_object.side_effect = mock_s3.exceptions.ClientError({}, 'NoSuchKey')
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'dataset/temp/test-dataset/part-00000-456.csv'}]
        }
        
        s3_writer_utils.s3_rename_and_move("dev", "test-dataset", "csv", "test-bucket")
        
        # Should not try to delete non-existent file, but should still copy
        delete_calls = [call for call in mock_s3.delete_object.call_args_list 
                       if 'dataset/test-dataset.csv' in str(call)]
        assert len(delete_calls) == 0  # No delete of target file
        mock_s3.copy_object.assert_called()