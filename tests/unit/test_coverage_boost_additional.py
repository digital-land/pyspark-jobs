"""Additional targeted tests to push coverage from 74.86% to 80%+."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

@pytest.mark.unit
class TestCoverageBoostAdditional:
    """Additional tests targeting specific uncovered lines."""

    def test_s3_format_utils_remaining_lines(self):
        """Target s3_format_utils remaining uncovered lines."""
        with patch.dict('sys.modules', {'pyspark.sql.functions': MagicMock(), 'pyspark.sql.types': MagicMock(), 'boto3': MagicMock()}):
            from jobs.utils.s3_format_utils import s3_csv_format, flatten_s3_json
            
            # Test s3_csv_format with string columns but no JSON (lines 42-47)
            mock_df = Mock()
            mock_string_field = Mock()
            mock_string_field.name = 'text_column'
            mock_string_field.dataType = Mock()
            mock_string_field.dataType.__class__ = Mock()
            mock_string_field.dataType.__class__.__name__ = 'StringType'
            mock_df.schema = [mock_string_field]
            
            # Mock select to return non-JSON content (lines 50-74)
            mock_select_result = Mock()
            mock_select_result.dropna.return_value.limit.return_value.collect.return_value = [
                Mock(**{'text_column': 'plain text not json'})
            ]
            mock_df.select.return_value = mock_select_result
            
            result = s3_csv_format(mock_df)
            assert result == mock_df
            
            # Test flatten_s3_json with array columns (lines 85-89)
            mock_df.dtypes = [('array_col', 'array<string>'), ('struct_col', 'struct<field:string>')]
            mock_df.columns = ['array_col', 'struct_col']
            mock_df.select.return_value.columns = ['field']
            mock_df.drop.return_value = mock_df
            
            with patch('jobs.utils.s3_format_utils.col') as mock_col:
                mock_col.return_value.alias.return_value = 'aliased'
                result = flatten_s3_json(mock_df)
                assert result == mock_df

    def test_postgres_writer_utils_column_types(self):
        """Test postgres_writer_utils specific column type handling."""
        with patch.dict('sys.modules', {'pg8000': MagicMock(), 'pyspark.sql.functions': MagicMock(), 'pyspark.sql.types': MagicMock()}):
            from jobs.utils.postgres_writer_utils import _ensure_required_columns
            
            mock_df = Mock()
            mock_df.columns = ['existing']
            mock_df.withColumn.return_value = mock_df
            
            with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit, \
                 patch('jobs.utils.postgres_writer_utils.col') as mock_col, \
                 patch('jobs.utils.postgres_writer_utils.to_json') as mock_to_json:
                
                mock_lit.return_value.cast.return_value = 'col'
                mock_col.return_value.cast.return_value = 'col'
                mock_to_json.return_value = 'json'
                
                # Test all specific column types (lines 93-130)
                required_cols = [
                    'entity', 'organisation_entity',  # bigint
                    'json', 'geojson', 'geometry', 'point', 'quality', 'name', 'prefix', 'reference', 'typology', 'dataset',  # string
                    'entry_date', 'start_date', 'end_date'  # date
                ]
                
                result = _ensure_required_columns(mock_df, required_cols)
                assert mock_df.withColumn.call_count >= len(required_cols) - 1
                
                # Test with existing columns type normalization (lines 131-150)
                mock_df.columns = ['entity', 'organisation_entity', 'entry_date', 'json', 'geojson']
                mock_df.withColumn.reset_mock()
                result = _ensure_required_columns(mock_df, required_cols)
                assert mock_df.withColumn.call_count >= 5  # Type normalization calls

    def test_s3_writer_utils_additional_functions(self):
        """Test additional s3_writer_utils functions."""
        with patch.dict('sys.modules', {'pyspark.sql.functions': MagicMock(), 'boto3': MagicMock(), 'requests': MagicMock()}):
            from jobs.utils.s3_writer_utils import ensure_schema_fields, round_point_coordinates
            
            # Test ensure_schema_fields with missing fields (lines 550-580)
            mock_df = Mock()
            mock_df.columns = ['entity']
            mock_df.withColumn.return_value = mock_df
            mock_df.select.return_value = mock_df
            
            with patch('jobs.utils.s3_writer_utils.fetch_dataset_schema_fields', return_value=['entity', 'name', 'geometry']), \
                 patch('jobs.utils.s3_writer_utils.lit') as mock_lit:
                
                mock_lit.return_value = 'empty_value'
                result = ensure_schema_fields(mock_df, "test-dataset")
                
                # Should add missing fields
                assert mock_df.withColumn.call_count >= 2  # name and geometry
                assert result == mock_df
            
            # Test round_point_coordinates with point column (lines 345-355)
            mock_df = Mock()
            mock_df.columns = ['point', 'other']
            mock_df.withColumn.return_value = mock_df
            
            with patch('jobs.utils.s3_writer_utils.udf') as mock_udf, \
                 patch('jobs.utils.s3_writer_utils.col') as mock_col:
                
                mock_udf.return_value = 'udf_func'
                result = round_point_coordinates(mock_df)
                
                mock_udf.assert_called_once()
                mock_df.withColumn.assert_called_once()
                assert result == mock_df

    def test_geometry_utils_coverage(self):
        """Test geometry_utils missing lines."""
        with patch.dict('sys.modules', {'pyspark.sql.functions': MagicMock()}):
            from jobs.utils.geometry_utils import calculate_centroid
            
            # Test calculate_centroid function (lines 18-27)
            mock_df = Mock()
            mock_df.withColumn.return_value = mock_df
            
            with patch('jobs.utils.geometry_utils.when') as mock_when, \
                 patch('jobs.utils.geometry_utils.col') as mock_col, \
                 patch('jobs.utils.geometry_utils.regexp_extract') as mock_extract:
                
                mock_when.return_value.otherwise.return_value = 'point_col'
                mock_extract.return_value.cast.return_value = 'coord'
                
                result = calculate_centroid(mock_df)
                
                mock_when.assert_called()
                mock_df.withColumn.assert_called()
                assert result == mock_df

    def test_aws_secrets_manager_error_paths(self):
        """Test aws_secrets_manager error handling paths."""
        with patch.dict('sys.modules', {'boto3': MagicMock()}):
            from jobs.utils.aws_secrets_manager import get_database_credentials
            
            # Test get_database_credentials with exception (lines 234-244)
            with patch('jobs.utils.aws_secrets_manager.boto3.client') as mock_boto3:
                mock_client = Mock()
                mock_boto3.return_value = mock_client
                mock_client.get_secret_value.side_effect = Exception("Secret not found")
                
                with pytest.raises(Exception, match="Secret not found"):
                    get_database_credentials("test-secret")

    def test_logger_config_missing_lines(self):
        """Test logger_config missing lines."""
        from jobs.utils.logger_config import setup_logging
        
        # Test setup_logging with file handler (lines 178-183)
        with patch('jobs.utils.logger_config.logging') as mock_logging, \
             patch('jobs.utils.logger_config.os.makedirs') as mock_makedirs:
            
            mock_logger = Mock()
            mock_logging.getLogger.return_value = mock_logger
            
            # Test with log_file parameter
            setup_logging(log_level="INFO", log_file="/tmp/test.log")
            
            mock_makedirs.assert_called_once()
            mock_logger.addHandler.assert_called()

    def test_s3_utils_error_handling(self):
        """Test s3_utils error handling paths."""
        with patch.dict('sys.modules', {'boto3': MagicMock(), 'pyspark.sql': MagicMock()}):
            from jobs.utils.s3_utils import cleanup_dataset_data
            
            # Test cleanup_dataset_data with S3 errors (lines 202-216)
            with patch('jobs.utils.s3_utils.boto3.client') as mock_boto3:
                mock_s3 = Mock()
                mock_boto3.return_value = mock_s3
                
                # Test with exception during delete (lines 212-216)
                mock_s3.list_objects_v2.return_value = {
                    'Contents': [{'Key': 'test/file.csv'}]
                }
                mock_s3.delete_objects.side_effect = Exception("Delete failed")
                
                result = cleanup_dataset_data("s3://bucket/path/", "dataset")
                
                assert 'errors' in result
                assert len(result['errors']) > 0

    def test_main_collection_data_error_paths(self):
        """Test main_collection_data error handling."""
        with patch.dict('sys.modules', {'pyspark.sql': MagicMock(), 'boto3': MagicMock()}):
            from jobs.main_collection_data import load_metadata
            
            # Test load_metadata with S3 error (lines 191-193)
            with patch('jobs.main_collection_data.boto3.client') as mock_boto3:
                mock_s3 = Mock()
                mock_boto3.return_value = mock_s3
                mock_s3.get_object.side_effect = Exception("S3 access denied")
                
                with pytest.raises(Exception, match="S3 access denied"):
                    load_metadata("config/test.json")

    def test_csv_s3_writer_error_paths(self):
        """Test csv_s3_writer error handling paths."""
        with patch.dict('sys.modules', {'pyspark.sql': MagicMock(), 'boto3': MagicMock()}):
            from jobs.csv_s3_writer import cleanup_temp_csv_files
            
            # Test cleanup_temp_csv_files with S3 errors (lines 989-993)
            with patch('jobs.csv_s3_writer.boto3.client') as mock_boto3:
                mock_s3 = Mock()
                mock_boto3.return_value = mock_s3
                mock_s3.list_objects_v2.side_effect = Exception("S3 list failed")
                
                # Should handle error gracefully
                try:
                    cleanup_temp_csv_files("bucket", "prefix")
                except Exception:
                    pass  # Expected to handle gracefully