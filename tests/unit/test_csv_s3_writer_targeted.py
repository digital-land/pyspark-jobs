"""
Targeted tests for csv_s3_writer.py to improve coverage from 51.46% to 60%+.
Focuses on uncovered lines: 115-186, 238-259, 268-305, 310-346, 386-405, etc.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import json

# Import functions from csv_s3_writer
from jobs.csv_s3_writer import (
    cleanup_temp_csv_files,
    write_dataframe_to_csv_s3,
    read_csv_from_s3,
    import_csv_to_aurora,
    get_aurora_connection_params,
    prepare_dataframe_for_csv,
    create_spark_session_for_csv
)


class TestCleanupTempCsvFiles:
    """Test cleanup_temp_csv_files function."""
    
    @patch('boto3.client')
    def test_cleanup_single_file(self, mock_boto3):
        """Test cleanup of single CSV file."""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        cleanup_temp_csv_files("s3://bucket/file.csv")
        mock_s3.delete_object.assert_called_once()
    
    @patch('boto3.client')
    def test_cleanup_directory(self, mock_boto3):
        """Test cleanup of CSV directory."""
        mock_s3 = Mock()
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'temp/file1.csv'}, {'Key': 'temp/file2.csv'}]
        }
        mock_boto3.return_value = mock_s3
        
        cleanup_temp_csv_files("s3://bucket/temp/")
        mock_s3.delete_objects.assert_called_once()


class TestWriteDataframeToCsvS3:
    """Test write_dataframe_to_csv_s3 function."""
    
    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.cleanup_dataset_data')
    def test_write_csv_basic(self, mock_cleanup, mock_prepare):
        """Test basic CSV writing functionality."""
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_prepare.return_value = mock_df
        mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
        
        # Mock the coalesce and write operations
        mock_coalesced = Mock()
        mock_df.coalesce.return_value = mock_coalesced
        mock_writer = Mock()
        mock_coalesced.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        
        with patch('jobs.csv_s3_writer._move_csv_to_final_location') as mock_move:
            mock_move.return_value = "s3://bucket/final.csv"
            
            result = write_dataframe_to_csv_s3(
                mock_df, "s3://bucket/output/", "entity", "test-dataset"
            )
            
            assert result == "s3://bucket/final.csv"


class TestReadCsvFromS3:
    """Test read_csv_from_s3 function."""
    
    def test_read_csv_basic(self):
        """Test basic CSV reading functionality."""
        mock_spark = Mock()
        mock_reader = Mock()
        mock_df = Mock()
        mock_df.count.return_value = 500
        
        mock_spark.read = mock_reader
        mock_reader.format.return_value = mock_reader
        mock_reader.option.return_value = mock_reader
        mock_reader.load.return_value = mock_df
        
        result = read_csv_from_s3(mock_spark, "s3://bucket/file.csv")
        assert result == mock_df


class TestImportCsvToAurora:
    """Test import_csv_to_aurora function."""
    
    @patch('jobs.csv_s3_writer._import_via_aurora_s3')
    @patch('jobs.csv_s3_writer.cleanup_temp_csv_files')
    def test_import_s3_method(self, mock_cleanup, mock_import):
        """Test Aurora S3 import method."""
        mock_import.return_value = {'rows_imported': 1000}
        
        result = import_csv_to_aurora(
            "s3://bucket/file.csv", "entity", "test-dataset", "development"
        )
        
        assert result['import_successful'] is True
        assert result['import_method_used'] == 'aurora_s3'
        mock_cleanup.assert_called_once()


class TestGetAuroraConnectionParams:
    """Test get_aurora_connection_params function."""
    
    @patch('jobs.csv_s3_writer.get_secret_emr_compatible')
    def test_get_connection_params_success(self, mock_get_secret):
        """Test successful connection parameter retrieval."""
        mock_get_secret.return_value = json.dumps({
            'host': 'test-host',
            'port': '5432',
            'db_name': 'testdb',
            'username': 'testuser',
            'password': 'testpass'
        })
        
        result = get_aurora_connection_params('development')
        
        assert result['host'] == 'test-host'
        assert result['database'] == 'testdb'


class TestPrepareDataframeForCsv:
    """Test prepare_dataframe_for_csv function."""
    
    def test_prepare_dataframe_basic(self):
        """Test basic DataFrame preparation."""
        mock_df = Mock()
        mock_field = Mock()
        mock_field.name = 'test_col'
        mock_field.dataType = 'string'
        mock_df.schema.fields = [mock_field]
        
        result = prepare_dataframe_for_csv(mock_df)
        assert result is not None


class TestCreateSparkSessionForCsv:
    """Test create_spark_session_for_csv function."""
    
    @patch('jobs.csv_s3_writer.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        """Test Spark session creation."""
        mock_builder = Mock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = Mock()
        
        result = create_spark_session_for_csv()
        assert result is not None


class TestPrepareDataframeForCsvExtended:
    """Extended tests for prepare_dataframe_for_csv function - targets lines 129-130, 138, 143-154, 163-164, 176-177."""
    
    def test_prepare_dataframe_json_columns(self):
        """Test DataFrame preparation with JSON columns."""
        mock_df = Mock()
        mock_field = Mock()
        mock_field.name = 'geojson'
        mock_field.dataType = 'struct<type:string>'
        mock_df.schema.fields = [mock_field]
        mock_df.withColumn.return_value = mock_df
        
        result = prepare_dataframe_for_csv(mock_df)
        assert result is not None
        mock_df.withColumn.assert_called()
    
    def test_prepare_dataframe_date_columns(self):
        """Test DataFrame preparation with date columns."""
        mock_df = Mock()
        mock_field = Mock()
        mock_field.name = 'created_date'
        mock_field.dataType = 'date'
        mock_df.schema.fields = [mock_field]
        mock_df.withColumn.return_value = mock_df
        
        result = prepare_dataframe_for_csv(mock_df)
        assert result is not None
        mock_df.withColumn.assert_called()
    
    def test_prepare_dataframe_geometry_columns(self):
        """Test DataFrame preparation with geometry columns."""
        mock_df = Mock()
        mock_field = Mock()
        mock_field.name = 'geometry'
        mock_field.dataType = 'string'
        mock_df.schema.fields = [mock_field]
        mock_df.withColumn.return_value = mock_df
        
        result = prepare_dataframe_for_csv(mock_df)
        assert result is not None
        mock_df.withColumn.assert_called()
    
    def test_prepare_dataframe_boolean_columns(self):
        """Test DataFrame preparation with boolean columns."""
        mock_df = Mock()
        mock_field = Mock()
        mock_field.name = 'active_flag'
        mock_field.dataType = 'boolean'
        mock_df.schema.fields = [mock_field]
        mock_df.withColumn.return_value = mock_df
        
        result = prepare_dataframe_for_csv(mock_df)
        assert result is not None
        mock_df.withColumn.assert_called()


class TestWriteDataframeToCsvS3Extended:
    """Extended tests for write_dataframe_to_csv_s3 - targets lines 247, 255, 289."""
    
    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.cleanup_dataset_data')
    @patch('jobs.csv_s3_writer.validate_s3_path')
    def test_write_csv_invalid_s3_path(self, mock_validate, mock_cleanup, mock_prepare):
        """Test CSV writing with invalid S3 path."""
        mock_validate.return_value = False
        mock_df = Mock()
        
        with pytest.raises(Exception):  # Should raise CSVWriterError
            write_dataframe_to_csv_s3(
                mock_df, "invalid://path", "entity", "test-dataset"
            )
    
    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.cleanup_dataset_data')
    def test_write_csv_no_cleanup(self, mock_cleanup, mock_prepare):
        """Test CSV writing without cleanup."""
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_prepare.return_value = mock_df
        
        # Mock the coalesce and write operations
        mock_coalesced = Mock()
        mock_df.coalesce.return_value = mock_coalesced
        mock_writer = Mock()
        mock_coalesced.write = mock_writer
        mock_writer.format.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        
        with patch('jobs.csv_s3_writer._move_csv_to_final_location') as mock_move:
            mock_move.return_value = "s3://bucket/final.csv"
            
            result = write_dataframe_to_csv_s3(
                mock_df, "s3://bucket/output/", "entity", "test-dataset", 
                cleanup_existing=False
            )
            
            mock_cleanup.assert_not_called()
            assert result == "s3://bucket/final.csv"


class TestImportCsvToAuroraExtended:
    """Extended tests for import_csv_to_aurora - targets error handling paths."""
    
    @patch('jobs.csv_s3_writer._import_via_jdbc')
    @patch('jobs.csv_s3_writer.cleanup_temp_csv_files')
    def test_import_jdbc_method(self, mock_cleanup, mock_import):
        """Test Aurora JDBC import method."""
        mock_import.return_value = {'rows_imported': 500}
        
        result = import_csv_to_aurora(
            "s3://bucket/file.csv", "entity", "test-dataset", "development",
            use_s3_import=False
        )
        
        assert result['import_successful'] is True
        assert result['import_method_used'] == 'jdbc'
        mock_cleanup.assert_called_once()
    
    @patch('jobs.csv_s3_writer._import_via_aurora_s3')
    @patch('jobs.csv_s3_writer.cleanup_temp_csv_files')
    def test_import_failure_cleanup(self, mock_cleanup, mock_import):
        """Test cleanup occurs even when import fails."""
        mock_import.side_effect = Exception("Import failed")
        
        with pytest.raises(Exception):
            import_csv_to_aurora(
                "s3://bucket/file.csv", "entity", "test-dataset", "development"
            )
        
        # Cleanup should still be called even on failure
        mock_cleanup.assert_called_once()