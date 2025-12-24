"""
Comprehensive tests for main_collection_data.py module.
Targets uncovered lines: 32-34, 87, 99, 114, 123-124, 164-166, 170-198, 231, 233, 235, 302-303, 315-347, 431-432, 440-443
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import argparse
from datetime import datetime
import json
import os


class TestInitializeLogging:
    """Test initialize_logging function - targets lines 32-34."""
    
    def test_initialize_logging_missing_env_attribute(self):
        """Test initialize_logging with missing env attribute."""
        from jobs.main_collection_data import initialize_logging
        
        args = Mock()
        del args.env  # Remove env attribute
        
        with pytest.raises(AttributeError):
            initialize_logging(args)
    
    @patch('jobs.main_collection_data.setup_logging')
    def test_initialize_logging_setup_exception(self, mock_setup):
        """Test initialize_logging when setup_logging raises exception."""
        from jobs.main_collection_data import initialize_logging
        
        mock_setup.side_effect = Exception("Setup failed")
        args = Mock()
        args.env = "dev"
        
        with pytest.raises(Exception):
            initialize_logging(args)


class TestCreateSparkSession:
    """Test create_spark_session function - targets line 87."""
    
    @patch('jobs.main_collection_data.SparkSession')
    @patch('jobs.main_collection_data.get_logger')
    def test_create_spark_session_failure(self, mock_logger, mock_spark_session):
        """Test create_spark_session when SparkSession creation fails."""
        from jobs.main_collection_data import create_spark_session
        
        mock_logger.return_value = Mock()
        mock_spark_session.builder.appName.return_value.config.return_value.config.return_value.config.return_value.config.return_value.getOrCreate.side_effect = Exception("Spark failed")
        
        result = create_spark_session()
        assert result is None


class TestLoadMetadata:
    """Test load_metadata function - targets lines 99, 114, 123-124."""
    
    @patch('boto3.client')
    @patch('jobs.main_collection_data.get_logger')
    def test_load_metadata_s3_path(self, mock_logger, mock_boto3):
        """Test load_metadata with S3 path."""
        from jobs.main_collection_data import load_metadata
        
        mock_logger.return_value = Mock()
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        mock_response = {'Body': Mock()}
        mock_response['Body'].read = Mock(return_value=b'{"test": "data"}')
        mock_s3.get_object.return_value = mock_response
        
        with patch('json.load', return_value={"test": "data"}):
            result = load_metadata("s3://bucket/key.json")
            assert result == {"test": "data"}
    
    @patch('pkgutil.get_data')
    @patch('jobs.main_collection_data.get_logger')
    def test_load_metadata_pkgutil_success(self, mock_logger, mock_get_data):
        """Test load_metadata using pkgutil successfully."""
        from jobs.main_collection_data import load_metadata
        
        mock_logger.return_value = Mock()
        mock_get_data.return_value = b'{"test": "data"}'
        
        result = load_metadata("config/test.json")
        assert result == {"test": "data"}
    
    @patch('builtins.open', create=True)
    @patch('os.path.isabs')
    @patch('os.path.dirname')
    @patch('os.path.abspath')
    @patch('os.path.normpath')
    @patch('jobs.main_collection_data.get_logger')
    def test_load_metadata_filesystem_absolute_path(self, mock_logger, mock_normpath, 
                                                   mock_abspath, mock_dirname, mock_isabs, 
                                                   mock_open):
        """Test load_metadata with absolute filesystem path."""
        from jobs.main_collection_data import load_metadata
        
        mock_logger.return_value = Mock()
        mock_isabs.return_value = True
        
        # Create a proper context manager mock
        mock_file_content = Mock()
        mock_file_content.read.return_value = '{"test": "data"}'
        mock_open.return_value.__enter__.return_value = mock_file_content
        mock_open.return_value.__exit__.return_value = None
        
        with patch('json.load', return_value={"test": "data"}):
            result = load_metadata("/absolute/path/config.json")
            assert result == {"test": "data"}
    
    @patch('pkgutil.get_data')
    @patch('builtins.open')
    @patch('os.path.isabs')
    @patch('os.path.dirname')
    @patch('os.path.abspath')
    @patch('os.path.normpath')
    @patch('jobs.main_collection_data.get_logger')
    def test_load_metadata_path_traversal_protection(self, mock_logger, mock_normpath, 
                                                    mock_abspath, mock_dirname, mock_isabs, 
                                                    mock_open, mock_get_data):
        """Test load_metadata path traversal protection."""
        from jobs.main_collection_data import load_metadata
        
        mock_logger.return_value = Mock()
        mock_get_data.side_effect = FileNotFoundError("Not found")
        mock_isabs.return_value = False
        mock_dirname.return_value = "/safe/dir"
        mock_abspath.return_value = "/safe/dir/script.py"
        mock_normpath.return_value = "/unsafe/dir/config.json"  # Path outside base
        
        with pytest.raises(ValueError, match="path traversal detected"):
            load_metadata("../../../etc/passwd")


class TestTransformData:
    """Test transform_data function - targets lines 164-166, 170-198."""
    
    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.get_logger')
    def test_transform_data_none_dataframe(self, mock_logger, mock_show, mock_load):
        """Test transform_data with None DataFrame."""
        from jobs.main_collection_data import transform_data
        
        mock_logger.return_value = Mock()
        
        with pytest.raises(ValueError, match="DataFrame is None"):
            transform_data(None, "fact", "test-dataset", Mock())
    
    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.get_logger')
    def test_transform_data_empty_dataframe(self, mock_logger, mock_show, mock_load):
        """Test transform_data with empty DataFrame."""
        from jobs.main_collection_data import transform_data
        
        mock_logger.return_value = Mock()
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = True
        
        with pytest.raises(ValueError, match="DataFrame is empty"):
            transform_data(mock_df, "fact", "test-dataset", Mock())
    
    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.transform_data_fact_res')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.get_logger')
    def test_transform_data_fact_resource_table(self, mock_logger, mock_show, mock_transform, mock_load):
        """Test transform_data for fact_resource table."""
        from jobs.main_collection_data import transform_data
        
        mock_logger.return_value = Mock()
        mock_load.return_value = {"schema_fact_res_fact_entity": ["field1", "field2"]}
        mock_transform.return_value = Mock()
        
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.columns = ["field1", "field2"]
        mock_df.withColumnRenamed.return_value = mock_df
        mock_df.printSchema = Mock()
        
        # Mock env as global variable
        with patch('jobs.main_collection_data.env', 'dev'):
            result = transform_data(mock_df, "fact_resource", "test-dataset", Mock())
            mock_transform.assert_called_once_with(mock_df)
    
    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.transform_data_fact')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.get_logger')
    def test_transform_data_fact_table(self, mock_logger, mock_show, mock_transform, mock_load):
        """Test transform_data for fact table."""
        from jobs.main_collection_data import transform_data
        
        mock_logger.return_value = Mock()
        mock_load.return_value = {"schema_fact_res_fact_entity": ["field1", "field2"]}
        mock_transform.return_value = Mock()
        
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.columns = ["field1", "field2"]
        mock_df.withColumnRenamed.return_value = mock_df
        mock_df.printSchema = Mock()
        
        with patch('jobs.main_collection_data.env', 'dev'):
            result = transform_data(mock_df, "fact", "test-dataset", Mock())
            mock_transform.assert_called_once_with(mock_df)
    
    @patch('jobs.main_collection_data.load_metadata')
    @patch('jobs.main_collection_data.transform_data_issue')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.get_logger')
    def test_transform_data_issue_table(self, mock_logger, mock_show, mock_transform, mock_load):
        """Test transform_data for issue table."""
        from jobs.main_collection_data import transform_data
        
        mock_logger.return_value = Mock()
        mock_load.return_value = {"schema_issue": ["field1", "field2"]}
        mock_transform.return_value = Mock()
        
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.columns = ["field1", "field2"]
        mock_df.withColumnRenamed.return_value = mock_df
        mock_df.printSchema = Mock()
        
        with patch('jobs.main_collection_data.env', 'dev'):
            result = transform_data(mock_df, "issue", "test-dataset", Mock())
            mock_transform.assert_called_once_with(mock_df)


class TestValidateS3Path:
    """Test validate_s3_path function - targets lines 231, 233, 235."""
    
    def test_validate_s3_path_none(self):
        """Test validate_s3_path with None."""
        from jobs.main_collection_data import validate_s3_path
        
        with pytest.raises(ValueError, match="S3 path must be a non-empty string"):
            validate_s3_path(None)
    
    def test_validate_s3_path_not_string(self):
        """Test validate_s3_path with non-string."""
        from jobs.main_collection_data import validate_s3_path
        
        with pytest.raises(ValueError, match="S3 path must be a non-empty string"):
            validate_s3_path(123)
    
    def test_validate_s3_path_invalid_format(self):
        """Test validate_s3_path with invalid format."""
        from jobs.main_collection_data import validate_s3_path
        
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            validate_s3_path("http://bucket/key")
    
    def test_validate_s3_path_too_short(self):
        """Test validate_s3_path with too short path."""
        from jobs.main_collection_data import validate_s3_path
        
        with pytest.raises(ValueError, match="Path too short"):
            validate_s3_path("s3://")


class TestMainFunction:
    """Test main function - targets lines 302-303, 315-347, 431-432, 440-443."""
    
    def test_main_missing_load_type(self):
        """Test main with missing load_type."""
        from jobs.main_collection_data import main
        
        args = Mock()
        del args.load_type
        
        with pytest.raises(ValueError, match="load_type is required"):
            main(args)
    
    def test_main_missing_data_set(self):
        """Test main with missing data_set."""
        from jobs.main_collection_data import main
        
        args = Mock()
        args.load_type = "full"
        del args.data_set
        
        with pytest.raises(ValueError, match="data_set is required"):
            main(args)
    
    def test_main_missing_path(self):
        """Test main with missing path."""
        from jobs.main_collection_data import main
        
        args = Mock()
        args.load_type = "full"
        args.data_set = "test"
        del args.path
        
        with pytest.raises(ValueError, match="path is required"):
            main(args)
    
    def test_main_missing_env(self):
        """Test main with missing env."""
        from jobs.main_collection_data import main
        
        args = Mock()
        args.load_type = "full"
        args.data_set = "test"
        args.path = "s3://bucket/path"
        del args.env
        
        with pytest.raises(AttributeError):
            main(args)
    
    def test_main_invalid_load_type(self):
        """Test main with invalid load_type."""
        from jobs.main_collection_data import main
        
        args = Mock()
        args.load_type = "invalid"
        args.data_set = "test"
        args.path = "s3://bucket/path"
        args.env = "development"
        
        with pytest.raises(ValueError, match="Invalid load_type"):
            main(args)
    
    def test_main_invalid_env(self):
        """Test main with invalid env."""
        from jobs.main_collection_data import main
        
        args = Mock()
        args.load_type = "full"
        args.data_set = "test"
        args.path = "s3://bucket/path"
        args.env = "invalid"
        
        with pytest.raises(ValueError, match="Invalid env"):
            main(args)
    
    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.get_logger')
    def test_main_spark_session_failure(self, mock_logger, mock_create_spark, mock_validate, mock_init_logging):
        """Test main when Spark session creation fails."""
        from jobs.main_collection_data import main
        
        mock_logger.return_value = Mock()
        mock_create_spark.return_value = None  # Spark session creation fails
        
        args = Mock()
        args.load_type = "full"
        args.data_set = "test"
        args.path = "s3://bucket/path"
        args.env = "development"
        args.use_jdbc = False
        
        with pytest.raises(Exception, match="Failed to create Spark session"):
            main(args)
    
    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.write_to_s3_format')
    @patch('jobs.main_collection_data.transform_data')
    @patch('jobs.main_collection_data.write_to_s3')
    @patch('jobs.main_collection_data.write_dataframe_to_postgres_jdbc')
    @patch('jobs.main_collection_data.show_df')
    @patch('jobs.main_collection_data.count_df')
    @patch('jobs.main_collection_data.get_logger')
    def test_main_full_load_success(self, mock_logger, mock_count, mock_show, mock_postgres, 
                                   mock_write_s3, mock_transform, mock_write_format, 
                                   mock_create_spark, mock_validate, mock_init_logging):
        """Test main with successful full load."""
        from jobs.main_collection_data import main
        
        mock_logger.return_value = Mock()
        mock_count.return_value = 1000
        
        # Mock Spark session
        mock_spark = Mock()
        mock_create_spark.return_value = mock_spark
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.cache.return_value = mock_df
        mock_df.printSchema = Mock()
        mock_df.drop.return_value = mock_df
        mock_spark.read.option.return_value.csv.return_value = mock_df
        
        # Mock transformations
        mock_transform.return_value = mock_df
        mock_write_format.return_value = mock_df
        
        args = Mock()
        args.load_type = "full"
        args.data_set = "test"
        args.path = "s3://bucket/path"
        args.env = "development"
        args.use_jdbc = False
        
        # Mock datetime for timing
        with patch('jobs.main_collection_data.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
            
            main(args)
            
            # Verify Spark session was stopped
            mock_spark.stop.assert_called_once()
    
    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.get_logger')
    def test_main_invalid_load_type_in_processing(self, mock_logger, mock_create_spark, mock_validate, mock_init_logging):
        """Test main with invalid load_type during processing."""
        from jobs.main_collection_data import main
        
        mock_logger.return_value = Mock()
        mock_spark = Mock()
        mock_create_spark.return_value = mock_spark
        
        args = Mock()
        args.load_type = "delta"  # Not implemented yet
        args.data_set = "test"
        args.path = "s3://bucket/path"
        args.env = "development"
        args.use_jdbc = False
        
        with pytest.raises(ValueError, match="Invalid load type: delta"):
            main(args)
    
    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.get_logger')
    def test_main_spark_stop_error(self, mock_logger, mock_create_spark, mock_validate, mock_init_logging):
        """Test main when Spark stop raises error."""
        from jobs.main_collection_data import main
        
        mock_logger.return_value = Mock()
        mock_spark = Mock()
        mock_spark.stop.side_effect = Exception("Stop failed")
        mock_create_spark.return_value = mock_spark
        
        args = Mock()
        args.load_type = "delta"  # Will cause ValueError
        args.data_set = "test"
        args.path = "s3://bucket/path"
        args.env = "development"
        args.use_jdbc = False
        
        with pytest.raises(ValueError):
            main(args)
        
        # Verify stop was attempted despite error
        mock_spark.stop.assert_called_once()
    
    @patch('jobs.main_collection_data.initialize_logging')
    @patch('jobs.main_collection_data.validate_s3_path')
    @patch('jobs.main_collection_data.create_spark_session')
    @patch('jobs.main_collection_data.get_logger')
    def test_main_timing_calculation_error(self, mock_logger, mock_create_spark, mock_validate, mock_init_logging):
        """Test main when timing calculation fails."""
        from jobs.main_collection_data import main
        
        mock_logger.return_value = Mock()
        mock_spark = Mock()
        mock_create_spark.return_value = mock_spark
        
        args = Mock()
        args.load_type = "delta"  # Will cause ValueError
        args.data_set = "test"
        args.path = "s3://bucket/path"
        args.env = "development"
        args.use_jdbc = False
        
        # Mock datetime to cause error in duration calculation
        with patch('jobs.main_collection_data.datetime') as mock_datetime:
            mock_datetime.now.side_effect = [datetime(2023, 1, 1), TypeError("Time error")]
            
            with pytest.raises(ValueError):
                main(args)


class TestReadData:
    """Test read_data function."""
    
    @patch('jobs.main_collection_data.get_logger')
    def test_read_data_success(self, mock_logger):
        """Test successful read_data."""
        from jobs.main_collection_data import read_data
        
        mock_logger.return_value = Mock()
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.read.csv.return_value = mock_df
        
        result = read_data(mock_spark, "s3://bucket/data.csv")
        assert result == mock_df
    
    @patch('jobs.main_collection_data.get_logger')
    def test_read_data_failure(self, mock_logger):
        """Test read_data failure."""
        from jobs.main_collection_data import read_data
        
        mock_logger.return_value = Mock()
        mock_spark = Mock()
        mock_spark.read.csv.side_effect = Exception("Read failed")
        
        with pytest.raises(Exception, match="Read failed"):
            read_data(mock_spark, "s3://bucket/data.csv")