"""
Focused unit tests for csv_s3_writer module.
Tests core functionality without complex PySpark integration issues.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from jobs import csv_s3_writer


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame."""
    df = Mock()
    df.columns = ['col1', 'col2', 'date_col']
    df.dtypes = [('col1', 'string'), ('col2', 'int'), ('date_col', 'date')]
    df.withColumn.return_value = df
    df.select.return_value = df
    df.write = Mock()
    df.write.option.return_value = df.write
    df.write.csv = Mock()
    return df


@pytest.mark.unit
class TestCsvS3WriterConstants:
    """Test CSV S3 writer constants and configuration."""

    def test_csv_config_exists(self):
        """Test that CSV_CONFIG constant exists."""
        assert hasattr(csv_s3_writer, 'CSV_CONFIG')
        assert isinstance(csv_s3_writer.CSV_CONFIG, dict)

    def test_csv_config_has_required_keys(self):
        """Test that CSV_CONFIG has required configuration keys."""
        config = csv_s3_writer.CSV_CONFIG
        
        # Check for expected keys
        expected_keys = ['header', 'delimiter', 'quote', 'escape', 'nullValue']
        for key in expected_keys:
            assert key in config, f"Missing required config key: {key}"

    def test_csv_config_values(self):
        """Test CSV_CONFIG values are appropriate."""
        config = csv_s3_writer.CSV_CONFIG
        
        # Test specific values
        assert config['header'] == 'true'
        assert config['delimiter'] == ','
        assert config['quote'] == '"'
        assert config['escape'] == '"'
        assert config['nullValue'] == ''


@pytest.mark.unit
class TestPrepareDataframeForCsv:
    """Test prepare_dataframe_for_csv function."""

    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.date_format')
    def test_prepare_dataframe_basic(self, mock_date_format, mock_when, mock_col, mock_dataframe):
        """Test basic dataframe preparation."""
        # Mock Spark functions to avoid PySpark integration issues
        mock_col.return_value = Mock()
        mock_when.return_value = Mock()
        mock_when.return_value.otherwise.return_value = Mock()
        mock_date_format.return_value = Mock()
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Should return a dataframe
        assert result is not None
        # Should have called withColumn for transformations
        mock_dataframe.withColumn.assert_called()

    def test_prepare_dataframe_no_columns(self):
        """Test preparation with empty dataframe."""
        empty_df = Mock()
        empty_df.columns = []
        empty_df.dtypes = []
        
        result = csv_s3_writer.prepare_dataframe_for_csv(empty_df)
        
        # Should return the original dataframe
        assert result == empty_df

    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.date_format')
    def test_prepare_dataframe_date_columns(self, mock_date_format, mock_when, mock_col, mock_dataframe):
        """Test preparation with date columns."""
        mock_dataframe.dtypes = [('date_col', 'date'), ('timestamp_col', 'timestamp')]
        mock_dataframe.columns = ['date_col', 'timestamp_col']
        
        # Mock Spark functions
        mock_col.return_value = Mock()
        mock_when_obj = Mock()
        mock_when_obj.otherwise.return_value = Mock()
        mock_when.return_value = mock_when_obj
        mock_date_format.return_value = Mock()
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Should process date columns
        assert mock_dataframe.withColumn.call_count >= 2  # At least one call per date column

    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.regexp_replace')
    def test_prepare_dataframe_string_columns(self, mock_regexp_replace, mock_when, mock_col, mock_dataframe):
        """Test preparation with string columns."""
        mock_dataframe.dtypes = [('string_col', 'string')]
        mock_dataframe.columns = ['string_col']
        
        # Mock Spark functions
        mock_col.return_value = Mock()
        mock_when.return_value = Mock()
        mock_when.return_value.otherwise.return_value = Mock()
        mock_regexp_replace.return_value = Mock()
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Should process string columns
        mock_dataframe.withColumn.assert_called()


@pytest.mark.unit
class TestWriteDataframeToCsvS3:
    """Test write_dataframe_to_csv_s3 function."""

    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_dataframe_to_csv_s3_basic(self, mock_logger, mock_prepare, mock_dataframe):
        """Test basic CSV writing to S3."""
        mock_prepare.return_value = mock_dataframe
        
        csv_s3_writer.write_dataframe_to_csv_s3(
            mock_dataframe, 
            "s3://test-bucket/path/", 
            "test-dataset"
        )
        
        # Should prepare dataframe
        mock_prepare.assert_called_once_with(mock_dataframe)
        
        # Should configure write options
        mock_dataframe.write.option.assert_called()
        
        # Should write CSV
        mock_dataframe.write.csv.assert_called_once()
        
        # Should log the operation
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_dataframe_to_csv_s3_with_coalesce(self, mock_logger, mock_prepare, mock_dataframe):
        """Test CSV writing with coalesce option."""
        mock_prepare.return_value = mock_dataframe
        mock_dataframe.coalesce.return_value = mock_dataframe
        
        csv_s3_writer.write_dataframe_to_csv_s3(
            mock_dataframe, 
            "s3://test-bucket/path/", 
            "test-dataset",
            coalesce=True
        )
        
        # Should coalesce the dataframe
        mock_dataframe.coalesce.assert_called_once_with(1)

    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_dataframe_to_csv_s3_error_handling(self, mock_logger, mock_prepare, mock_dataframe):
        """Test error handling in CSV writing."""
        mock_prepare.return_value = mock_dataframe
        mock_dataframe.write.csv.side_effect = Exception("S3 write failed")
        
        with pytest.raises(Exception, match="S3 write failed"):
            csv_s3_writer.write_dataframe_to_csv_s3(
                mock_dataframe, 
                "s3://test-bucket/path/", 
                "test-dataset"
            )
        
        # Should still log the attempt
        mock_logger.info.assert_called()


@pytest.mark.unit
class TestCleanupTempCsvFiles:
    """Test cleanup_temp_csv_files function."""

    @patch('jobs.csv_s3_writer.boto3')
    @patch('jobs.csv_s3_writer.logger')
    def test_cleanup_temp_csv_files_basic(self, mock_logger, mock_boto3):
        """Test basic cleanup functionality."""
        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3
        
        # Mock list_objects_v2 response
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'temp/file1.csv'},
                {'Key': 'temp/file2.csv'}
            ]
        }
        
        csv_s3_writer.cleanup_temp_csv_files("test-bucket", "temp/")
        
        # Should list objects
        mock_s3.list_objects_v2.assert_called_once()
        
        # Should delete objects
        mock_s3.delete_objects.assert_called_once()
        
        # Should log the operation
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.boto3')
    @patch('jobs.csv_s3_writer.logger')
    def test_cleanup_temp_csv_files_no_objects(self, mock_logger, mock_boto3):
        """Test cleanup with no objects to delete."""
        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3
        
        # Mock empty response
        mock_s3.list_objects_v2.return_value = {}
        
        csv_s3_writer.cleanup_temp_csv_files("test-bucket", "temp/")
        
        # Should not call delete_objects
        mock_s3.delete_objects.assert_not_called()
        
        # Should log no objects found
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.boto3')
    @patch('jobs.csv_s3_writer.logger')
    def test_cleanup_temp_csv_files_error_handling(self, mock_logger, mock_boto3):
        """Test error handling in cleanup."""
        mock_s3 = Mock()
        mock_boto3.client.return_value = mock_s3
        mock_s3.list_objects_v2.side_effect = Exception("S3 error")
        
        # Should not raise exception, just log error
        csv_s3_writer.cleanup_temp_csv_files("test-bucket", "temp/")
        
        # Should log the error
        mock_logger.error.assert_called()


@pytest.mark.unit
class TestImportCsvToAurora:
    """Test import_csv_to_aurora function."""

    @patch('jobs.csv_s3_writer.get_aws_secret')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_csv_to_aurora_basic(self, mock_logger, mock_get_secret):
        """Test basic Aurora import functionality."""
        mock_get_secret.return_value = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        
        # Mock pg8000 connection
        with patch('jobs.csv_s3_writer.pg8000') as mock_pg8000:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor
            mock_pg8000.connect.return_value = mock_conn
            
            csv_s3_writer.import_csv_to_aurora(
                "s3://bucket/file.csv",
                "test_table",
                "development"
            )
            
            # Should get AWS secrets
            mock_get_secret.assert_called_once_with("development")
            
            # Should connect to database
            mock_pg8000.connect.assert_called_once()
            
            # Should execute SQL
            mock_cursor.execute.assert_called()
            
            # Should commit transaction
            mock_conn.commit.assert_called()

    @patch('jobs.csv_s3_writer.get_aws_secret')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_csv_to_aurora_connection_error(self, mock_logger, mock_get_secret):
        """Test Aurora import with connection error."""
        mock_get_secret.return_value = {}
        
        with patch('jobs.csv_s3_writer.pg8000') as mock_pg8000:
            mock_pg8000.connect.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                csv_s3_writer.import_csv_to_aurora(
                    "s3://bucket/file.csv",
                    "test_table",
                    "development"
                )
            
            # Should log the error
            mock_logger.error.assert_called()


@pytest.mark.unit
class TestUtilityFunctions:
    """Test utility functions in csv_s3_writer."""

    def test_module_imports(self):
        """Test that required modules are imported."""
        # Check that key functions exist
        assert hasattr(csv_s3_writer, 'write_dataframe_to_csv_s3')
        assert hasattr(csv_s3_writer, 'prepare_dataframe_for_csv')
        assert hasattr(csv_s3_writer, 'cleanup_temp_csv_files')
        assert hasattr(csv_s3_writer, 'import_csv_to_aurora')

    def test_logger_exists(self):
        """Test that logger is configured."""
        assert hasattr(csv_s3_writer, 'logger')
        assert csv_s3_writer.logger is not None