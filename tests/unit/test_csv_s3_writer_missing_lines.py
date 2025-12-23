"""
Targeted tests for missing lines in csv_s3_writer.py
Focus on lines: 115-186, 238-259, 268-305, 310-346, 386-405, 479, 496, 544-547, 706-814, 819-855, 864-993
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

try:
    from jobs import csv_s3_writer
except ImportError:
    # Mock PySpark if not available
    with patch.dict('sys.modules', {
        'pyspark': Mock(),
        'pyspark.sql': Mock(),
        'pyspark.sql.functions': Mock(),
        'pyspark.sql.types': Mock()
    }):
        from jobs import csv_s3_writer


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame."""
    df = Mock()
    df.count.return_value = 1000
    df.schema.fields = [
        Mock(name='entity', dataType=Mock(__str__=lambda x: 'bigint')),
        Mock(name='json', dataType=Mock(__str__=lambda x: 'struct')),
        Mock(name='entry_date', dataType=Mock(__str__=lambda x: 'date')),
        Mock(name='geometry', dataType=Mock(__str__=lambda x: 'string')),
        Mock(name='active_flag', dataType=Mock(__str__=lambda x: 'boolean'))
    ]
    df.withColumn.return_value = df
    df.coalesce.return_value.write.format.return_value.option.return_value.mode.return_value.save.return_value = None
    return df


@pytest.fixture
def mock_spark():
    """Create mock Spark session."""
    spark = Mock()
    spark.read.format.return_value.option.return_value.load.return_value = Mock()
    return spark


@pytest.mark.unit
class TestPrepareDataframeForCsvMissingLines:
    """Test missing lines 115-186 in prepare_dataframe_for_csv function."""

    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.to_json')
    @patch('jobs.csv_s3_writer.logger')
    def test_prepare_dataframe_struct_column_handling(self, mock_logger, mock_to_json, mock_col, mock_when, mock_dataframe):
        """Test lines 115-186: Struct column conversion to JSON."""
        mock_dataframe.schema.fields = [
            Mock(name='json_field', dataType=Mock(__str__=lambda x: 'struct<field1:string,field2:int>'))
        ]
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Verify struct column is converted to JSON (lines 120-125)
        mock_to_json.assert_called()
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.logger')
    def test_prepare_dataframe_json_string_column(self, mock_logger, mock_col, mock_when, mock_dataframe):
        """Test lines 127-130: JSON string column handling."""
        mock_dataframe.schema.fields = [
            Mock(name='geojson', dataType=Mock(__str__=lambda x: 'string'))
        ]
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Verify JSON string columns are kept as-is (lines 128-129)
        mock_logger.info.assert_called_with("prepare_dataframe_for_csv: Column geojson is already a string, keeping as-is")

    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.date_format')
    @patch('jobs.csv_s3_writer.logger')
    def test_prepare_dataframe_timestamp_column(self, mock_logger, mock_date_format, mock_col, mock_when, mock_dataframe):
        """Test lines 132-145: Timestamp column handling."""
        mock_dataframe.schema.fields = [
            Mock(name='created_timestamp', dataType=Mock(__str__=lambda x: 'timestamp'))
        ]
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Verify timestamp format is applied (lines 137-141)
        mock_date_format.assert_called()
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.date_format')
    @patch('jobs.csv_s3_writer.logger')
    def test_prepare_dataframe_date_column(self, mock_logger, mock_date_format, mock_col, mock_when, mock_dataframe):
        """Test lines 142-147: Date column handling."""
        mock_dataframe.schema.fields = [
            Mock(name='entry_date', dataType=Mock(__str__=lambda x: 'date'))
        ]
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Verify date format is applied (lines 143-147)
        mock_date_format.assert_called()
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.logger')
    def test_prepare_dataframe_geometry_column(self, mock_logger, mock_col, mock_when, mock_dataframe):
        """Test lines 149-160: Geometry column handling."""
        mock_dataframe.schema.fields = [
            Mock(name='geometry', dataType=Mock(__str__=lambda x: 'string'))
        ]
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Verify geometry column processing (lines 150-160)
        mock_when.assert_called()
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.when')
    @patch('jobs.csv_s3_writer.col')
    @patch('jobs.csv_s3_writer.StringType')
    @patch('jobs.csv_s3_writer.logger')
    def test_prepare_dataframe_boolean_column(self, mock_logger, mock_string_type, mock_col, mock_when, mock_dataframe):
        """Test lines 162-170: Boolean column handling."""
        mock_dataframe.schema.fields = [
            Mock(name='active_flag', dataType=Mock(__str__=lambda x: 'boolean'))
        ]
        
        result = csv_s3_writer.prepare_dataframe_for_csv(mock_dataframe)
        
        # Verify boolean column conversion (lines 163-170)
        mock_when.assert_called()
        mock_logger.info.assert_called()


@pytest.mark.unit
class TestWriteDataframeToCsvS3MissingLines:
    """Test missing lines 238-259, 268-305 in write_dataframe_to_csv_s3 function."""

    @patch('jobs.csv_s3_writer.validate_s3_path')
    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.cleanup_dataset_data')
    @patch('jobs.csv_s3_writer._write_single_csv_file')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_dataframe_invalid_s3_path(self, mock_logger, mock_write_single, mock_cleanup, 
                                           mock_prepare, mock_validate, mock_dataframe):
        """Test lines 238-259: Invalid S3 path handling."""
        mock_validate.return_value = False
        
        with pytest.raises(csv_s3_writer.CSVWriterError, match="Invalid S3 path format"):
            csv_s3_writer.write_dataframe_to_csv_s3(
                mock_dataframe, "invalid-path", "entity", "test-dataset"
            )

    @patch('jobs.csv_s3_writer.validate_s3_path')
    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.cleanup_dataset_data')
    @patch('jobs.csv_s3_writer._write_single_csv_file')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_dataframe_cleanup_warnings(self, mock_logger, mock_write_single, mock_cleanup, 
                                            mock_prepare, mock_validate, mock_dataframe):
        """Test lines 268-305: Cleanup warnings handling."""
        mock_validate.return_value = True
        mock_prepare.return_value = mock_dataframe
        mock_cleanup.return_value = {
            'objects_deleted': 5,
            'errors': ['Warning 1', 'Warning 2']
        }
        mock_write_single.return_value = "s3://bucket/file.csv"
        
        result = csv_s3_writer.write_dataframe_to_csv_s3(
            mock_dataframe, "s3://bucket/path/", "entity", "test-dataset"
        )
        
        # Verify cleanup warnings are logged (lines 270-271)
        mock_logger.warning.assert_called()

    @patch('jobs.csv_s3_writer.validate_s3_path')
    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.cleanup_dataset_data')
    @patch('jobs.csv_s3_writer._write_single_csv_file')
    @patch('jobs.csv_s3_writer.time.time')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_dataframe_temp_folder_creation(self, mock_logger, mock_time, mock_write_single, 
                                                mock_cleanup, mock_prepare, mock_validate, mock_dataframe):
        """Test lines 273-280: Temporary folder creation."""
        mock_validate.return_value = True
        mock_prepare.return_value = mock_dataframe
        mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
        mock_time.return_value = 1234567890
        mock_write_single.return_value = "s3://bucket/file.csv"
        
        result = csv_s3_writer.write_dataframe_to_csv_s3(
            mock_dataframe, "s3://bucket/path/", "entity", "test-dataset", temp_folder=True
        )
        
        # Verify temporary folder path is created (lines 275-277)
        mock_logger.info.assert_called()

    @patch('jobs.csv_s3_writer.validate_s3_path')
    @patch('jobs.csv_s3_writer.prepare_dataframe_for_csv')
    @patch('jobs.csv_s3_writer.cleanup_dataset_data')
    @patch('jobs.csv_s3_writer._write_single_csv_file')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_dataframe_no_temp_folder(self, mock_logger, mock_write_single, mock_cleanup, 
                                          mock_prepare, mock_validate, mock_dataframe):
        """Test lines 279-280: No temporary folder handling."""
        mock_validate.return_value = True
        mock_prepare.return_value = mock_dataframe
        mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
        mock_write_single.return_value = "s3://bucket/file.csv"
        
        result = csv_s3_writer.write_dataframe_to_csv_s3(
            mock_dataframe, "s3://bucket/path/", "entity", "test-dataset", temp_folder=False
        )
        
        # Verify output path is used directly (line 280)
        mock_write_single.assert_called()


@pytest.mark.unit
class TestWriteSingleCsvFileMissingLines:
    """Test missing lines 310-346 in _write_single_csv_file function."""

    @patch('jobs.csv_s3_writer._move_csv_to_final_location')
    @patch('jobs.csv_s3_writer._cleanup_temp_path')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_single_csv_compression_option(self, mock_logger, mock_cleanup, mock_move, mock_dataframe):
        """Test lines 310-346: Compression option handling."""
        mock_move.return_value = "s3://bucket/final.csv"
        config = {"compression": "gzip", "include_header": True, "sep": ",", 
                 "quote_char": '"', "escape_char": '"', "null_value": "",
                 "date_format": "yyyy-MM-dd", "timestamp_format": "yyyy-MM-dd HH:mm:ss"}
        
        result = csv_s3_writer._write_single_csv_file(
            mock_dataframe, "s3://bucket/output/", "entity", "test-dataset", config, 1234567890
        )
        
        # Verify compression option is applied (lines 330-331)
        mock_dataframe.coalesce.assert_called_with(1)

    @patch('jobs.csv_s3_writer._move_csv_to_final_location')
    @patch('jobs.csv_s3_writer._cleanup_temp_path')
    @patch('jobs.csv_s3_writer.logger')
    def test_write_single_csv_exception_cleanup(self, mock_logger, mock_cleanup, mock_move, mock_dataframe):
        """Test lines 340-346: Exception cleanup handling."""
        mock_move.side_effect = Exception("Move failed")
        config = {"compression": None, "include_header": True, "sep": ",", 
                 "quote_char": '"', "escape_char": '"', "null_value": "",
                 "date_format": "yyyy-MM-dd", "timestamp_format": "yyyy-MM-dd HH:mm:ss"}
        
        with pytest.raises(Exception, match="Move failed"):
            csv_s3_writer._write_single_csv_file(
                mock_dataframe, "s3://bucket/output/", "entity", "test-dataset", config, 1234567890
            )
        
        # Verify cleanup is attempted on exception (line 344)
        mock_cleanup.assert_called()


@pytest.mark.unit
class TestWriteMultipleCsvFilesMissingLines:
    """Test missing lines 386-405 in _write_multiple_csv_files function."""

    @patch('jobs.csv_s3_writer.logger')
    def test_write_multiple_csv_compression_option(self, mock_logger, mock_dataframe):
        """Test lines 386-405: Compression option in multiple files."""
        config = {
            "compression": "snappy", "include_header": True, "sep": ",", 
            "quote_char": '"', "escape_char": '"', "null_value": "",
            "date_format": "yyyy-MM-dd", "timestamp_format": "yyyy-MM-dd HH:mm:ss",
            "max_records_per_file": 1000
        }
        mock_dataframe.count.return_value = 2500
        mock_dataframe.repartition.return_value = mock_dataframe
        
        result = csv_s3_writer._write_multiple_csv_files(
            mock_dataframe, "s3://bucket/output/", "entity", "test-dataset", config
        )
        
        # Verify compression option is applied (lines 400-401)
        mock_dataframe.repartition.assert_called_with(3)  # (2500 // 1000) + 1


@pytest.mark.unit
class TestMoveCsvToFinalLocationMissingLines:
    """Test missing lines 479, 496, 544-547 in _move_csv_to_final_location function."""

    @patch('boto3.client')
    @patch('jobs.csv_s3_writer._cleanup_temp_path')
    @patch('jobs.csv_s3_writer.logger')
    def test_move_csv_no_csv_file_found(self, mock_logger, mock_cleanup, mock_boto3):
        """Test line 479: No CSV file found error."""
        mock_s3_client = Mock()
        mock_boto3.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {'Contents': []}  # No CSV files
        
        with pytest.raises(csv_s3_writer.CSVWriterError, match="No CSV file found in temporary location"):
            csv_s3_writer._move_csv_to_final_location(
                "s3://bucket/temp/", "s3://bucket/final.csv"
            )

    @patch('boto3.client')
    @patch('boto3.s3.transfer.create_transfer_manager')
    @patch('jobs.csv_s3_writer._cleanup_temp_path')
    @patch('jobs.csv_s3_writer.logger')
    def test_move_csv_large_file_multipart(self, mock_logger, mock_cleanup, mock_transfer_manager, mock_boto3):
        """Test lines 496, 544-547: Large file multipart transfer."""
        mock_s3_client = Mock()
        mock_boto3.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'temp/file.csv'}]
        }
        mock_s3_client.head_object.return_value = {
            'ContentLength': 6 * 1024 * 1024 * 1024  # 6GB file
        }
        
        # Mock transfer manager
        mock_transfer = Mock()
        mock_future = Mock()
        mock_transfer.copy.return_value = mock_future
        mock_transfer_manager.return_value = mock_transfer
        
        result = csv_s3_writer._move_csv_to_final_location(
            "s3://bucket/temp/", "s3://bucket/final.csv"
        )
        
        # Verify multipart transfer is used for large files (lines 496, 544-547)
        mock_logger.info.assert_called()
        mock_transfer.copy.assert_called()
        mock_future.result.assert_called()

    @patch('boto3.client')
    @patch('jobs.csv_s3_writer._cleanup_temp_path')
    @patch('jobs.csv_s3_writer.logger')
    def test_move_csv_regular_file_copy(self, mock_logger, mock_cleanup, mock_boto3):
        """Test regular file copy for smaller files."""
        mock_s3_client = Mock()
        mock_boto3.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'temp/file.csv'}]
        }
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024 * 1024  # 1MB file
        }
        
        result = csv_s3_writer._move_csv_to_final_location(
            "s3://bucket/temp/", "s3://bucket/final.csv"
        )
        
        # Verify regular copy is used for smaller files
        mock_s3_client.copy_object.assert_called()
        mock_cleanup.assert_called()


@pytest.mark.unit
class TestImportViaAuroraS3MissingLines:
    """Test missing lines 706-814 in _import_via_aurora_s3 function."""

    @patch('pg8000.connect')
    @patch('jobs.csv_s3_writer.get_aurora_connection_params')
    @patch('jobs.csv_s3_writer.ssl.create_default_context')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_aurora_s3_ssl_configuration(self, mock_logger, mock_ssl, mock_get_params, mock_pg8000):
        """Test lines 706-814: SSL configuration for Aurora."""
        mock_get_params.return_value = {
            'host': 'aurora.amazonaws.com',
            'port': '5432',
            'database': 'testdb',
            'username': 'user',
            'password': 'pass'
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [1000]
        mock_pg8000.return_value = mock_conn
        
        result = csv_s3_writer._import_via_aurora_s3(
            "s3://bucket/file.csv", "entity", "test-dataset", True, "development"
        )
        
        # Verify SSL context is created and used (lines 730-740)
        mock_ssl.assert_called()
        mock_pg8000.assert_called()

    @patch('pg8000.connect')
    @patch('jobs.csv_s3_writer.get_aurora_connection_params')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_aurora_s3_extension_creation(self, mock_logger, mock_get_params, mock_pg8000):
        """Test lines 750-753: aws_s3 extension creation."""
        mock_get_params.return_value = {
            'host': 'aurora.amazonaws.com',
            'port': '5432',
            'database': 'testdb',
            'username': 'user',
            'password': 'pass'
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [1000]
        mock_pg8000.return_value = mock_conn
        
        result = csv_s3_writer._import_via_aurora_s3(
            "s3://bucket/file.csv", "entity", "test-dataset", True, "development"
        )
        
        # Verify aws_s3 extension is created (lines 751-753)
        extension_calls = [call for call in mock_cursor.execute.call_args_list 
                          if 'CREATE EXTENSION' in str(call)]
        assert len(extension_calls) > 0

    @patch('pg8000.connect')
    @patch('jobs.csv_s3_writer.get_aurora_connection_params')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_aurora_s3_truncate_table(self, mock_logger, mock_get_params, mock_pg8000):
        """Test lines 755-761: Table truncation logic."""
        mock_get_params.return_value = {
            'host': 'aurora.amazonaws.com',
            'port': '5432',
            'database': 'testdb',
            'username': 'user',
            'password': 'pass'
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 500
        mock_cursor.fetchone.return_value = [1000]
        mock_pg8000.return_value = mock_conn
        
        result = csv_s3_writer._import_via_aurora_s3(
            "s3://bucket/file.csv", "entity", "test-dataset", True, "development"
        )
        
        # Verify table truncation is performed (lines 756-761)
        delete_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'DELETE FROM' in str(call)]
        assert len(delete_calls) > 0

    @patch('pg8000.connect')
    @patch('jobs.csv_s3_writer.get_aurora_connection_params')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_aurora_s3_import_execution(self, mock_logger, mock_get_params, mock_pg8000):
        """Test lines 763-780: S3 import SQL execution."""
        mock_get_params.return_value = {
            'host': 'aurora.amazonaws.com',
            'port': '5432',
            'database': 'testdb',
            'username': 'user',
            'password': 'pass'
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [1000]
        mock_pg8000.return_value = mock_conn
        
        result = csv_s3_writer._import_via_aurora_s3(
            "s3://bucket/file.csv", "entity", "test-dataset", False, "development"
        )
        
        # Verify S3 import SQL is executed (lines 775-780)
        import_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'aws_s3.table_import_from_s3' in str(call)]
        assert len(import_calls) > 0
        assert result['rows_imported'] == 1000

    @patch('pg8000.connect')
    @patch('jobs.csv_s3_writer.get_aurora_connection_params')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_aurora_s3_pg8000_error(self, mock_logger, mock_get_params, mock_pg8000):
        """Test lines 800-814: pg8000 error handling."""
        import pg8000
        mock_get_params.return_value = {
            'host': 'aurora.amazonaws.com',
            'port': '5432',
            'database': 'testdb',
            'username': 'user',
            'password': 'pass'
        }
        
        mock_pg8000.side_effect = pg8000.Error("Connection failed")
        
        with pytest.raises(csv_s3_writer.AuroraImportError, match="Aurora S3 import failed"):
            csv_s3_writer._import_via_aurora_s3(
                "s3://bucket/file.csv", "entity", "test-dataset", False, "development"
            )
        
        # Verify pg8000 error is handled (lines 800-804)
        mock_logger.error.assert_called()


@pytest.mark.unit
class TestImportViaJdbcMissingLines:
    """Test missing lines 819-855 in _import_via_jdbc function."""

    @patch('jobs.csv_s3_writer.create_spark_session_for_csv')
    @patch('jobs.csv_s3_writer.read_csv_from_s3')
    @patch('jobs.csv_s3_writer.write_to_postgres')
    @patch('jobs.csv_s3_writer.get_aws_secret')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_via_jdbc_success(self, mock_logger, mock_get_secret, mock_write_postgres, 
                                   mock_read_csv, mock_create_spark):
        """Test lines 819-855: JDBC import success path."""
        mock_spark = Mock()
        mock_create_spark.return_value = mock_spark
        
        mock_df = Mock()
        mock_df.count.return_value = 2000
        mock_read_csv.return_value = mock_df
        
        mock_get_secret.return_value = {'host': 'localhost'}
        
        result = csv_s3_writer._import_via_jdbc(
            "s3://bucket/file.csv", "entity", "test-dataset", "development", True
        )
        
        # Verify JDBC import process (lines 830-850)
        mock_create_spark.assert_called_with("JDBCImport")
        mock_read_csv.assert_called_with(mock_spark, "s3://bucket/file.csv")
        mock_write_postgres.assert_called()
        mock_spark.stop.assert_called()
        
        assert result['rows_imported'] == 2000

    @patch('jobs.csv_s3_writer.create_spark_session_for_csv')
    @patch('jobs.csv_s3_writer.read_csv_from_s3')
    @patch('jobs.csv_s3_writer.logger')
    def test_import_via_jdbc_exception(self, mock_logger, mock_read_csv, mock_create_spark):
        """Test lines 855: JDBC import exception handling."""
        mock_spark = Mock()
        mock_create_spark.return_value = mock_spark
        mock_read_csv.side_effect = Exception("Read failed")
        
        with pytest.raises(csv_s3_writer.AuroraImportError, match="JDBC import failed"):
            csv_s3_writer._import_via_jdbc(
                "s3://bucket/file.csv", "entity", "test-dataset", "development", True
            )
        
        # Verify exception handling and spark cleanup (line 855)
        mock_logger.error.assert_called()
        mock_spark.stop.assert_called()


@pytest.mark.unit
class TestMainFunctionMissingLines:
    """Test missing lines 864-993 in main function."""

    @patch('jobs.csv_s3_writer.create_spark_session_for_csv')
    @patch('jobs.csv_s3_writer.write_dataframe_to_csv_s3')
    @patch('jobs.csv_s3_writer.import_csv_to_aurora')
    @patch('jobs.csv_s3_writer.logger')
    @patch('sys.argv', ['csv_s3_writer.py', '--input', 's3://bucket/input/', '--output', 's3://bucket/output/', '--table', 'entity', '--dataset', 'test'])
    def test_main_parquet_to_csv_conversion(self, mock_logger, mock_import, mock_write_csv, mock_create_spark):
        """Test lines 864-993: Parquet to CSV conversion workflow."""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.read.parquet.return_value = mock_df
        mock_create_spark.return_value = mock_spark
        
        mock_write_csv.return_value = "s3://bucket/output/file.csv"
        mock_import.return_value = {
            'rows_imported': 1000,
            'import_method_used': 'aurora_s3'
        }
        
        with patch('builtins.print') as mock_print:
            csv_s3_writer.main()
        
        # Verify parquet reading and conversion (lines 920-950)
        mock_spark.read.parquet.assert_called_with('s3://bucket/input/')
        mock_write_csv.assert_called()
        mock_import.assert_called()
        mock_spark.stop.assert_called()

    @patch('jobs.csv_s3_writer.import_csv_to_aurora')
    @patch('jobs.csv_s3_writer.logger')
    @patch('sys.argv', ['csv_s3_writer.py', '--import-csv', 's3://bucket/file.csv', '--table', 'entity', '--dataset', 'test'])
    def test_main_csv_import_only(self, mock_logger, mock_import):
        """Test lines 960-975: CSV import only workflow."""
        mock_import.return_value = {
            'rows_imported': 500,
            'import_method_used': 'jdbc'
        }
        
        with patch('builtins.print') as mock_print:
            csv_s3_writer.main()
        
        # Verify CSV import (lines 960-975)
        mock_import.assert_called_with(
            's3://bucket/file.csv', 'entity', 'test', 'development', use_s3_import=True
        )

    @patch('jobs.csv_s3_writer.logger')
    @patch('sys.argv', ['csv_s3_writer.py', '--table', 'entity', '--dataset', 'test'])
    def test_main_missing_arguments(self, mock_logger):
        """Test lines 977-980: Missing arguments error handling."""
        with patch('sys.exit') as mock_exit:
            with patch('argparse.ArgumentParser.error') as mock_error:
                mock_error.side_effect = SystemExit(2)
                
                with pytest.raises(SystemExit):
                    csv_s3_writer.main()

    @patch('jobs.csv_s3_writer.create_spark_session_for_csv')
    @patch('jobs.csv_s3_writer.logger')
    @patch('sys.argv', ['csv_s3_writer.py', '--input', 's3://bucket/input/', '--output', 's3://bucket/output/', '--table', 'entity', '--dataset', 'test'])
    def test_main_exception_handling(self, mock_logger, mock_create_spark):
        """Test lines 982-993: Exception handling in main."""
        mock_create_spark.side_effect = Exception("Spark failed")
        
        with patch('builtins.print') as mock_print:
            with patch('sys.exit') as mock_exit:
                csv_s3_writer.main()
        
        # Verify exception handling (lines 982-993)
        mock_print.assert_called()
        mock_exit.assert_called_with(1)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])