"""
Targeted tests for missing lines in main_collection_data.py
Focus on lines: 32-34, 87, 99, 114, 123-124, 164-166, 170-198, 231, 233, 235, 302-303, 315-347, 431-432, 440-443
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

try:
    from jobs import main_collection_data
except ImportError:
    # Mock PySpark if not available
    with patch.dict('sys.modules', {
        'pyspark': Mock(),
        'pyspark.sql': Mock(),
        'pyspark.sql.functions': Mock(),
        'pyspark.sql.types': Mock()
    }):
        from jobs import main_collection_data


@pytest.fixture
def mock_args():
    """Create mock arguments."""
    args = Mock()
    args.load_type = 'full'
    args.data_set = 'test-dataset'
    args.path = 's3://test-bucket/data/'
    args.env = 'development'
    args.use_jdbc = False
    return args


@pytest.fixture
def mock_spark():
    """Create mock Spark session."""
    spark = Mock()
    spark.read.option.return_value.csv.return_value = Mock()
    spark.stop.return_value = None
    return spark


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame."""
    df = Mock()
    df.columns = ['entity', 'field', 'value']
    df.rdd.isEmpty.return_value = False
    df.withColumnRenamed.return_value = df
    df.printSchema.return_value = None
    df.cache.return_value = df
    return df


@pytest.mark.unit
class TestInitializeLoggingMissingLines:
    """Test missing lines 32-34 in initialize_logging function."""

    @patch('jobs.main_collection_data.setup_logging')
    def test_initialize_logging_attribute_error(self, mock_setup_logging, mock_args):
        """Test lines 32-34: AttributeError handling."""
        # Remove an attribute to trigger AttributeError
        delattr(mock_args, 'env')
        
        with pytest.raises(AttributeError):
            main_collection_data.initialize_logging(mock_args)

    @patch('jobs.main_collection_data.setup_logging')
    def test_initialize_logging_general_exception(self, mock_setup_logging, mock_args):
        """Test lines 32-34: General exception handling."""
        mock_setup_logging.side_effect = Exception("Setup failed")
        
        with pytest.raises(Exception, match="Setup failed"):
            main_collection_data.initialize_logging(mock_args)


@pytest.mark.unit
class TestCreateSparkSessionMissingLines:
    """Test missing line 87 in create_spark_session function."""

    @patch('jobs.main_collection_data.SparkSession')
    @patch('jobs.main_collection_data.logger')
    def test_create_spark_session_exception(self, mock_logger, mock_spark_session):
        """Test line 87: Exception handling in Spark session creation."""
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.getOrCreate.side_effect = Exception("Spark failed")
        
        result = main_collection_data.create_spark_session()
        
        # Verify exception is handled and None is returned (line 87)
        assert result is None
        mock_logger.error.assert_called()


@pytest.mark.unit
class TestLoadMetadataMissingLines:
    """Test missing lines 99, 114, 123-124, 164-166, 170-198 in load_metadata function."""

    @patch('jobs.main_collection_data.pkgutil.get_data')
    @patch('jobs.main_collection_data.logger')
    def test_load_metadata_pkgutil_none_result(self, mock_logger, mock_get_data):
        """Test line 99: pkgutil.get_data returns None."""
        mock_get_data.return_value = None
        
        with pytest.raises(FileNotFoundError, match="pkgutil.get_data could not find"):
            main_collection_data.load_metadata("config/test.json")

    @patch('jobs.main_collection_data.pkgutil.get_data')
    @patch('jobs.main_collection_data.os.path.isabs')
    @patch('jobs.main_collection_data.logger')
    def test_load_metadata_absolute_path_handling(self, mock_logger, mock_isabs, mock_get_data):
        """Test line 114: Absolute path handling."""
        mock_get_data.side_effect = FileNotFoundError("Package not found")
        mock_isabs.return_value = True
        
        with patch('builtins.open', side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError):
                main_collection_data.load_metadata("/absolute/path/config.json")
        
        # Verify absolute path is used directly (line 114)
        mock_isabs.assert_called_with("/absolute/path/config.json")

    @patch('jobs.main_collection_data.pkgutil.get_data')
    @patch('jobs.main_collection_data.os.path.isabs')
    @patch('jobs.main_collection_data.os.path.dirname')
    @patch('jobs.main_collection_data.os.path.abspath')
    @patch('jobs.main_collection_data.os.path.normpath')
    @patch('jobs.main_collection_data.os.path.join')
    @patch('jobs.main_collection_data.logger')
    def test_load_metadata_relative_path_construction(self, mock_logger, mock_join, mock_normpath, 
                                                    mock_abspath, mock_dirname, mock_isabs, mock_get_data):
        """Test lines 123-124: Relative path construction."""
        mock_get_data.side_effect = FileNotFoundError("Package not found")
        mock_isabs.return_value = False
        mock_dirname.return_value = "/script/dir"
        mock_abspath.return_value = "/script/dir/file.py"
        mock_join.return_value = "/script/dir/config.json"
        mock_normpath.return_value = "/script/dir/config.json"
        
        with patch('builtins.open', side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError):
                main_collection_data.load_metadata("config.json")
        
        # Verify relative path construction (lines 116-120)
        mock_dirname.assert_called()
        mock_join.assert_called()
        mock_normpath.assert_called()

    @patch('jobs.main_collection_data.pkgutil.get_data')
    @patch('jobs.main_collection_data.os.path.isabs')
    @patch('jobs.main_collection_data.os.path.normpath')
    @patch('jobs.main_collection_data.logger')
    def test_load_metadata_path_traversal_detection(self, mock_logger, mock_normpath, mock_isabs, mock_get_data):
        """Test lines 164-166: Path traversal detection."""
        mock_get_data.side_effect = FileNotFoundError("Package not found")
        mock_isabs.return_value = False
        mock_normpath.return_value = "/different/path/config.json"  # Path outside script dir
        
        with patch('jobs.main_collection_data.os.path.dirname', return_value="/script/dir"):
            with patch('jobs.main_collection_data.os.path.abspath', return_value="/script/dir/file.py"):
                with patch('jobs.main_collection_data.os.path.join', return_value="/different/path/config.json"):
                    with pytest.raises(ValueError, match="Invalid file path: path traversal detected"):
                        main_collection_data.load_metadata("../../../config.json")

    @patch('jobs.main_collection_data.pkgutil.get_data')
    @patch('jobs.main_collection_data.logger')
    def test_load_metadata_file_system_success(self, mock_logger, mock_get_data):
        """Test lines 170-198: Successful file system loading."""
        mock_get_data.side_effect = FileNotFoundError("Package not found")
        test_data = {"test": "data"}
        
        with patch('jobs.main_collection_data.os.path.isabs', return_value=True):
            with patch('builtins.open', mock_open_read_data=json.dumps(test_data)):
                result = main_collection_data.load_metadata("/absolute/path/config.json")
        
        # Verify successful loading from file system (lines 170-198)
        assert result == test_data
        mock_logger.info.assert_called()

    @patch('jobs.main_collection_data.pkgutil.get_data')
    @patch('jobs.main_collection_data.logger')
    def test_load_metadata_json_decode_error(self, mock_logger, mock_get_data):
        """Test lines 170-198: JSON decode error handling."""
        mock_get_data.side_effect = FileNotFoundError("Package not found")
        
        with patch('jobs.main_collection_data.os.path.isabs', return_value=True):
            with patch('builtins.open', mock_open_read_data="invalid json"):
                with pytest.raises(json.JSONDecodeError):
                    main_collection_data.load_metadata("/absolute/path/config.json")
        
        # Verify JSON decode error is handled (lines 170-198)
        mock_logger.error.assert_called()

    @patch('jobs.main_collection_data.pkgutil.get_data')
    @patch('jobs.main_collection_data.logger')
    def test_load_metadata_io_error(self, mock_logger, mock_get_data):
        """Test lines 170-198: IO error handling."""
        mock_get_data.side_effect = FileNotFoundError("Package not found")
        
        with patch('jobs.main_collection_data.os.path.isabs', return_value=True):
            with patch('builtins.open', side_effect=IOError("IO error")):
                with pytest.raises(IOError):
                    main_collection_data.load_metadata("/absolute/path/config.json")
        
        # Verify IO error is handled (lines 170-198)
        mock_logger.error.assert_called()


@pytest.mark.unit
class TestTransformDataMissingLines:
    """Test missing lines 231, 233, 235 in transform_data function."""

    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.transform_data_fact_res')
    @patch('jobs.main_collection_data.logger')
    def test_transform_data_fact_resource_table(self, mock_logger, mock_transform_fact_res, 
                                              mock_show_df, mock_load_metadata, mock_dataframe, mock_spark):
        """Test line 231: fact_resource table transformation."""
        mock_load_metadata.return_value = {"schema_fact_res_fact_entity": ["entity", "field", "value"]}
        mock_transform_fact_res.return_value = mock_dataframe
        
        result = main_collection_data.transform_data(mock_dataframe, "fact_resource", "test-dataset", mock_spark)
        
        # Verify fact_resource transformation is called (line 231)
        mock_transform_fact_res.assert_called_once_with(mock_dataframe)
        mock_logger.info.assert_called()

    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.transform_data_fact')
    @patch('jobs.main_collection_data.logger')
    def test_transform_data_fact_table(self, mock_logger, mock_transform_fact, 
                                     mock_show_df, mock_load_metadata, mock_dataframe, mock_spark):
        """Test line 233: fact table transformation."""
        mock_load_metadata.return_value = {"schema_fact_res_fact_entity": ["entity", "field", "value"]}
        mock_transform_fact.return_value = mock_dataframe
        
        result = main_collection_data.transform_data(mock_dataframe, "fact", "test-dataset", mock_spark)
        
        # Verify fact transformation is called (line 233)
        mock_transform_fact.assert_called_once_with(mock_dataframe)
        mock_logger.info.assert_called()

    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.transform_data_issue')
    @patch('jobs.main_collection_data.logger')
    def test_transform_data_issue_table(self, mock_logger, mock_transform_issue, 
                                      mock_show_df, mock_load_metadata, mock_dataframe, mock_spark):
        """Test line 235: issue table transformation."""
        mock_load_metadata.return_value = {"schema_issue": ["issue", "severity", "message"]}
        mock_transform_issue.return_value = mock_dataframe
        
        result = main_collection_data.transform_data(mock_dataframe, "issue", "test-dataset", mock_spark)
        
        # Verify issue transformation is called (line 235)
        mock_transform_issue.assert_called_once_with(mock_dataframe)
        mock_logger.info.assert_called()


@pytest.mark.unit
class TestMainFunctionMissingLines:
    """Test missing lines 302-303, 315-347, 431-432, 440-443 in main function."""

    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.logger')
    def test_main_spark_session_creation_failure(self, mock_logger, mock_create_spark, 
                                                mock_validate, mock_init_logging, mock_args):
        """Test lines 302-303: Spark session creation failure."""
        mock_create_spark.return_value = None
        
        with pytest.raises(Exception, match="Failed to create Spark session"):
            main_collection_data.main(mock_args)
        
        # Verify exception is raised when Spark session is None (lines 302-303)
        mock_logger.info.assert_called()

    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.write_to_s3_format')
    @patch('jobs.main_collection_data.transform_data')
    @patch('jobs.main_collection_data.write_to_s3')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.count_df')
    @patch('jobs.main_collection_data.logger')
    def test_main_entity_table_processing(self, mock_logger, mock_count_df, mock_show_df, 
                                        mock_write_s3, mock_transform, mock_write_s3_format,
                                        mock_create_spark, mock_validate, mock_init_logging, 
                                        mock_args, mock_spark, mock_dataframe):
        """Test lines 315-347: Entity table processing logic."""
        mock_create_spark.return_value = mock_spark
        mock_spark.read.option.return_value.csv.return_value = mock_dataframe
        mock_write_s3_format.return_value = mock_dataframe
        mock_transform.return_value = mock_dataframe
        mock_count_df.return_value = 1000
        
        # Set df_entity to None initially to test the entity processing path
        main_collection_data.df_entity = None
        
        main_collection_data.main(mock_args)
        
        # Verify entity table specific processing (lines 315-347)
        mock_write_s3_format.assert_called()
        mock_logger.info.assert_called()

    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.write_dataframe_to_postgres_jdbc')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.logger')
    def test_main_df_entity_none_handling(self, mock_logger, mock_show_df, mock_write_postgres,
                                        mock_create_spark, mock_validate, mock_init_logging, 
                                        mock_args, mock_spark):
        """Test lines 431-432: df_entity None handling."""
        mock_create_spark.return_value = mock_spark
        mock_spark.read.option.return_value.csv.return_value = Mock(rdd=Mock(isEmpty=Mock(return_value=True)))
        
        # Set df_entity to None to test the None handling path
        main_collection_data.df_entity = None
        
        main_collection_data.main(mock_args)
        
        # Verify df_entity None handling (lines 431-432)
        mock_logger.info.assert_called_with("Main: df_entity is None, skipping Postgres write")
        mock_write_postgres.assert_not_called()

    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.logger')
    def test_main_finally_block_spark_stop_error(self, mock_logger, mock_create_spark, 
                                                mock_validate, mock_init_logging, mock_args, mock_spark):
        """Test lines 440-443: Spark stop error handling in finally block."""
        mock_create_spark.return_value = mock_spark
        mock_spark.stop.side_effect = Exception("Stop failed")
        mock_spark.read.option.return_value.csv.return_value = Mock(rdd=Mock(isEmpty=Mock(return_value=True)))
        
        main_collection_data.main(mock_args)
        
        # Verify Spark stop error is handled gracefully (lines 440-443)
        mock_logger.warning.assert_called()

    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.datetime')
    @patch('jobs.main_collection_data.logger')
    def test_main_finally_block_duration_calculation_error(self, mock_logger, mock_datetime, 
                                                          mock_create_spark, mock_validate, 
                                                          mock_init_logging, mock_args, mock_spark):
        """Test lines 440-443: Duration calculation error handling."""
        mock_create_spark.return_value = mock_spark
        mock_spark.read.option.return_value.csv.return_value = Mock(rdd=Mock(isEmpty=Mock(return_value=True)))
        
        # Mock datetime to cause error in duration calculation
        mock_datetime.now.side_effect = [Mock(), Exception("Time error")]
        
        main_collection_data.main(mock_args)
        
        # Verify duration calculation error is handled (lines 440-443)
        mock_logger.warning.assert_called()


@pytest.mark.unit
class TestValidateS3PathMissingLines:
    """Test validate_s3_path function edge cases."""

    def test_validate_s3_path_none_input(self):
        """Test None input validation."""
        with pytest.raises(ValueError, match="S3 path must be a non-empty string"):
            main_collection_data.validate_s3_path(None)

    def test_validate_s3_path_non_string_input(self):
        """Test non-string input validation."""
        with pytest.raises(ValueError, match="S3 path must be a non-empty string"):
            main_collection_data.validate_s3_path(123)

    def test_validate_s3_path_empty_string(self):
        """Test empty string validation."""
        with pytest.raises(ValueError, match="S3 path must be a non-empty string"):
            main_collection_data.validate_s3_path("")

    def test_validate_s3_path_invalid_prefix(self):
        """Test invalid prefix validation."""
        with pytest.raises(ValueError, match="Invalid S3 path format.*Must start with s3://"):
            main_collection_data.validate_s3_path("http://bucket/path")

    def test_validate_s3_path_too_short(self):
        """Test path too short validation."""
        with pytest.raises(ValueError, match="Invalid S3 path.*Path too short"):
            main_collection_data.validate_s3_path("s3://")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])