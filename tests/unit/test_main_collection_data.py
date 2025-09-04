"""
Unit tests for main_collection_data module.

This module provides comprehensive unit tests for the main collection data
processing job, including all core functions with proper mocking.
"""
import pytest
import os
import sys
import tempfile
import json
from unittest.mock import patch, Mock, MagicMock
from pyspark.sql import SparkSession

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from jobs.main_collection_data import (
    create_spark_session,
    load_metadata,
    read_data,
    transform_data,
    write_to_s3,
    write_dataframe_to_postgres,
    main
)


class TestCreateSparkSession:
    """Test Spark session creation."""
    
    @patch('jobs.main_collection_data.SparkSession')
    def test_create_spark_session_success(self, mock_spark_session_class):
        """Test successful Spark session creation."""
        # Setup mock
        mock_session = MagicMock()
        mock_builder = MagicMock()
        mock_spark_session_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session
        
        # Test
        result = create_spark_session("TestApp")
        
        # Verify
        assert result == mock_session
        mock_builder.appName.assert_called_with("TestApp")
        mock_builder.getOrCreate.assert_called_once()
    
    @patch('jobs.main_collection_data.SparkSession')
    def test_create_spark_session_failure(self, mock_spark_session_class):
        """Test Spark session creation failure."""
        # Setup mock to raise exception
        mock_builder = MagicMock()
        mock_spark_session_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.side_effect = Exception("Spark creation failed")
        
        # Test
        result = create_spark_session("TestApp")
        
        # Verify
        assert result is None


class TestLoadMetadata:
    """Test metadata loading functionality."""
    
    def test_load_metadata_local_file(self, tmp_path):
        """Test loading metadata from local file."""
        # Create test JSON file
        test_data = {"test_key": "test_value", "schema": ["field1", "field2"]}
        test_file = tmp_path / "test_config.json"
        test_file.write_text(json.dumps(test_data))
        
        # Test
        result = load_metadata(str(test_file))
        
        # Verify
        assert result == test_data
    
    def test_load_metadata_nonexistent_file(self):
        """Test loading metadata from non-existent file."""
        with pytest.raises(FileNotFoundError):
            load_metadata("/nonexistent/path/config.json")
    
    @patch('jobs.main_collection_data.boto3.client')
    def test_load_metadata_s3_uri(self, mock_boto3_client):
        """Test loading metadata from S3 URI."""
        # Setup mock
        test_data = {"test_key": "test_value"}
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock()}
        mock_response["Body"].read.return_value = json.dumps(test_data).encode()
        mock_s3.get_object.return_value = mock_response
        
        # Mock json.load to return our test data
        with patch('json.load', return_value=test_data):
            result = load_metadata("s3://test-bucket/config.json")
        
        # Verify
        assert result == test_data
        mock_s3.get_object.assert_called_with(Bucket="test-bucket", Key="config.json")
    
    @patch('jobs.main_collection_data.pkgutil.get_data')
    def test_load_metadata_package_resource(self, mock_get_data):
        """Test loading metadata from package resource."""
        test_data = {"test_key": "test_value"}
        mock_get_data.return_value = json.dumps(test_data).encode('utf-8')
        
        result = load_metadata("config/test.json")
        
        assert result == test_data


class TestReadData:
    """Test data reading functionality."""
    
    def test_read_data_success(self, spark):
        """Test successful data reading."""
        # Create test CSV data
        test_data = [
            {"col1": "value1", "col2": "value2"},
            {"col1": "value3", "col2": "value4"}
        ]
        df = spark.createDataFrame(test_data)
        
        # Mock spark.read.csv
        with patch.object(spark, 'read') as mock_read:
            mock_read.csv.return_value = df
            
            result = read_data(spark, "test_path.csv")
            
            assert result is not None
            mock_read.csv.assert_called_with("test_path.csv", header=True, inferSchema=True)
    
    def test_read_data_failure(self, spark):
        """Test data reading failure."""
        with patch.object(spark, 'read') as mock_read:
            mock_read.csv.side_effect = Exception("File not found")
            
            with pytest.raises(Exception, match="File not found"):
                read_data(spark, "nonexistent.csv")


class TestTransformData:
    """Test data transformation functionality."""
    
    def test_transform_data_fact(self, spark, sample_fact_dataframe, mock_configuration_files):
        """Test fact data transformation."""
        # Mock load_metadata to return our test config
        with patch('jobs.main_collection_data.load_metadata') as mock_load_metadata:
            mock_load_metadata.return_value = {
                "schema_fact_res_fact_entity": list(sample_fact_dataframe.columns)
            }
            
            # Mock the actual transformation function
            with patch('jobs.main_collection_data.transform_data_fact') as mock_transform:
                expected_df = sample_fact_dataframe.select("fact", "entity", "field")
                mock_transform.return_value = expected_df
                
                result = transform_data(sample_fact_dataframe, "fact", "test-dataset", spark)
                
                assert result is not None
                mock_transform.assert_called_once_with(sample_fact_dataframe)
    
    def test_transform_data_fact_res(self, spark, sample_fact_res_dataframe, mock_configuration_files):
        """Test fact resource data transformation."""
        with patch('jobs.main_collection_data.load_metadata') as mock_load_metadata:
            mock_load_metadata.return_value = {
                "schema_fact_res_fact_entity": list(sample_fact_res_dataframe.columns)
            }
            
            with patch('jobs.main_collection_data.transform_data_fact_res') as mock_transform:
                expected_df = sample_fact_res_dataframe.select("fact", "resource")
                mock_transform.return_value = expected_df
                
                result = transform_data(sample_fact_res_dataframe, "fact_res", "test-dataset", spark)
                
                assert result is not None
                mock_transform.assert_called_once_with(sample_fact_res_dataframe)
    
    def test_transform_data_entity(self, spark, sample_entity_dataframe, mock_configuration_files):
        """Test entity data transformation."""
        with patch('jobs.main_collection_data.load_metadata') as mock_load_metadata:
            mock_load_metadata.return_value = {
                "schema_fact_res_fact_entity": list(sample_entity_dataframe.columns)
            }
            
            with patch('jobs.main_collection_data.transform_data_entity') as mock_transform:
                expected_df = sample_entity_dataframe.select("entity", "field", "value")
                mock_transform.return_value = expected_df
                
                result = transform_data(sample_entity_dataframe, "entity", "test-dataset", spark)
                
                assert result is not None
                mock_transform.assert_called_once_with(sample_entity_dataframe, "test-dataset", spark)
    
    def test_transform_data_issue(self, spark, sample_issue_dataframe, mock_configuration_files):
        """Test issue data transformation."""
        with patch('jobs.main_collection_data.load_metadata') as mock_load_metadata:
            mock_load_metadata.return_value = {
                "schema_issue": list(sample_issue_dataframe.columns)
            }
            
            with patch('jobs.main_collection_data.transform_data_issue') as mock_transform:
                expected_df = sample_issue_dataframe.select("issue", "resource")
                mock_transform.return_value = expected_df
                
                result = transform_data(sample_issue_dataframe, "issue", "test-dataset", spark)
                
                assert result is not None
                mock_transform.assert_called_once_with(sample_issue_dataframe)
    
    def test_transform_data_invalid_schema(self, spark, sample_fact_dataframe):
        """Test transformation with invalid schema name."""
        with patch('jobs.main_collection_data.load_metadata') as mock_load_metadata:
            mock_load_metadata.return_value = {"schema_fact_res_fact_entity": []}
            
            with pytest.raises(ValueError, match="Unknown table name"):
                transform_data(sample_fact_dataframe, "invalid_schema", "test-dataset", spark)


class TestWriteToS3:
    """Test S3 writing functionality."""
    
    def test_write_to_s3_success(self, spark, sample_fact_dataframe, mock_spark_write_operations):
        """Test successful S3 write operation."""
        with patch('jobs.main_collection_data.cleanup_dataset_data') as mock_cleanup:
            mock_cleanup.return_value = {"deleted_files": 0}
            
            # Test - should not raise exception
            write_to_s3(sample_fact_dataframe, "s3://test-bucket/output", "test-dataset", "fact")
            
            # Verify cleanup was called
            mock_cleanup.assert_called_once()
    
    def test_write_to_s3_entity_table(self, spark, sample_entity_dataframe, mock_spark_write_operations):
        """Test S3 write for entity table (sets global variable)."""
        with patch('jobs.main_collection_data.cleanup_dataset_data') as mock_cleanup:
            mock_cleanup.return_value = {"deleted_files": 0}
            
            # Test entity table writing
            write_to_s3(sample_entity_dataframe, "s3://test-bucket/output", "test-dataset", "entity")
            
            # Verify the global df_entity is set (we can't easily test this directly)
            mock_cleanup.assert_called_once()


class TestWriteDataframeToPostgres:
    """Test PostgreSQL writing functionality."""
    
    def test_write_dataframe_to_postgres_entity(self, spark, sample_entity_dataframe):
        """Test writing entity data to PostgreSQL."""
        with patch('jobs.main_collection_data.get_performance_recommendations') as mock_perf, \
             patch('jobs.main_collection_data.write_to_postgres') as mock_write, \
             patch('jobs.main_collection_data.get_aws_secret') as mock_secret:
            
            # Setup mocks
            mock_perf.return_value = {
                "method": "jdbc_batch",
                "batch_size": 1000,
                "num_partitions": 4
            }
            mock_secret.return_value = {"host": "localhost"}
            
            # Test
            write_dataframe_to_postgres(sample_entity_dataframe, "entity", "test-dataset")
            
            # Verify
            mock_perf.assert_called_once()
            mock_write.assert_called_once()
    
    def test_write_dataframe_to_postgres_non_entity(self, spark, sample_fact_dataframe):
        """Test writing non-entity data to PostgreSQL (should be skipped)."""
        with patch('jobs.main_collection_data.write_to_postgres') as mock_write:
            # Test - should not call write_to_postgres for non-entity tables
            write_dataframe_to_postgres(sample_fact_dataframe, "fact", "test-dataset")
            
            # Verify write_to_postgres was not called
            mock_write.assert_not_called()


class TestMainFunction:
    """Test the main ETL function."""
    
    def test_main_full_load_success(self, spark, mock_s3, mock_secrets_manager, 
                                  mock_configuration_files, mock_spark_write_operations):
        """Test successful full load execution."""
        # Create mock arguments
        mock_args = Mock()
        mock_args.load_type = "full"
        mock_args.data_set = "transport-access-node"
        mock_args.path = "s3://development-collection-data/"
        mock_args.env = "development"
        
        # Mock all the dependencies
        with patch('jobs.main_collection_data.create_spark_session') as mock_create_spark, \
             patch('jobs.main_collection_data.transform_data') as mock_transform, \
             patch('jobs.main_collection_data.write_to_s3') as mock_write_s3, \
             patch('jobs.main_collection_data.write_dataframe_to_postgres') as mock_write_pg, \
             patch.object(spark, 'read') as mock_read:
            
            # Setup mocks
            mock_create_spark.return_value = spark
            mock_df = Mock()
            mock_df.rdd.isEmpty.return_value = False
            mock_df.cache.return_value = mock_df
            mock_df.printSchema.return_value = None
            mock_df.show.return_value = None
            mock_read.option.return_value.csv.return_value = mock_df
            mock_transform.return_value = mock_df
            
            # Mock the global df_entity for the final postgres write
            with patch('jobs.main_collection_data.df_entity', mock_df):
                mock_df.drop.return_value = mock_df
                mock_df.show.return_value = None
                
                # Test - should not raise exception
                main(mock_args)
            
            # Verify key operations were called
            mock_create_spark.assert_called_once()
            assert mock_transform.call_count >= 3  # Should be called for each table type
            assert mock_write_s3.call_count >= 3   # Should write to S3 for each table
    
    def test_main_sample_load_success(self, spark, mock_s3, mock_configuration_files):
        """Test successful sample load execution."""
        mock_args = Mock()
        mock_args.load_type = "sample"
        mock_args.data_set = "transport-access-node"
        mock_args.path = "s3://development-collection-data/"
        mock_args.env = "development"
        
        with patch('jobs.main_collection_data.create_spark_session') as mock_create_spark, \
             patch('jobs.main_collection_data.transform_data') as mock_transform, \
             patch('jobs.main_collection_data.write_to_s3') as mock_write_s3, \
             patch('jobs.main_collection_data.write_to_postgres') as mock_write_pg, \
             patch.object(spark, 'read') as mock_read:
            
            # Setup mocks
            mock_create_spark.return_value = spark
            mock_df = Mock()
            mock_df.rdd.isEmpty.return_value = False
            mock_df.cache.return_value = mock_df
            mock_df.printSchema.return_value = None
            mock_df.show.return_value = None
            mock_read.option.return_value.csv.return_value = mock_df
            mock_transform.return_value = mock_df
            
            # Test
            main(mock_args)
            
            # Verify operations were called
            mock_create_spark.assert_called_once()
    
    def test_main_delta_load(self, spark):
        """Test delta load (not implemented yet)."""
        mock_args = Mock()
        mock_args.load_type = "delta"
        mock_args.data_set = "transport-access-node"
        mock_args.path = "s3://development-collection-data/"
        mock_args.env = "development"
        
        with patch('jobs.main_collection_data.create_spark_session') as mock_create_spark:
            mock_create_spark.return_value = spark
            
            # Test - should handle delta load gracefully
            main(mock_args)
    
    def test_main_invalid_load_type(self, spark):
        """Test main function with invalid load type."""
        mock_args = Mock()
        mock_args.load_type = "invalid"
        mock_args.data_set = "transport-access-node"
        mock_args.path = "s3://development-collection-data/"
        mock_args.env = "development"
        
        with patch('jobs.main_collection_data.create_spark_session') as mock_create_spark:
            mock_create_spark.return_value = spark
            
            # Test - should handle invalid load type gracefully (logged as error)
            main(mock_args)
    
    def test_main_spark_creation_failure(self):
        """Test main function when Spark session creation fails."""
        mock_args = Mock()
        mock_args.load_type = "full"
        mock_args.data_set = "transport-access-node"
        mock_args.path = "s3://development-collection-data/"
        mock_args.env = "development"
        
        with patch('jobs.main_collection_data.create_spark_session') as mock_create_spark:
            mock_create_spark.return_value = None
            
            # Test - should handle None Spark session gracefully
            main(mock_args)


class TestIntegrationScenarios:
    """Integration-style tests that test multiple components together."""
    
    def test_full_etl_pipeline_mock(self, spark, sample_transport_dataframe, 
                                   mock_s3, mock_configuration_files):
        """Test a complete ETL pipeline with mocked external services."""
        # This test exercises the full pipeline but with mocked external dependencies
        
        # Setup test scenario
        dataset_name = "transport-access-node"
        
        # Mock the main dependencies
        with patch('jobs.main_collection_data.cleanup_dataset_data') as mock_cleanup, \
             patch('jobs.main_collection_data.transform_data_fact') as mock_transform_fact, \
             patch('jobs.main_collection_data.transform_data_fact_res') as mock_transform_fact_res, \
             patch('jobs.main_collection_data.transform_data_entity') as mock_transform_entity:
            
            # Setup mock returns
            mock_cleanup.return_value = {"deleted_files": 0}
            mock_transform_fact.return_value = sample_transport_dataframe
            mock_transform_fact_res.return_value = sample_transport_dataframe
            mock_transform_entity.return_value = sample_transport_dataframe
            
            # Test the transformation pipeline
            result_fact = transform_data(sample_transport_dataframe, "fact", dataset_name, spark)
            result_fact_res = transform_data(sample_transport_dataframe, "fact_res", dataset_name, spark)
            result_entity = transform_data(sample_transport_dataframe, "entity", dataset_name, spark)
            
            # Verify all transformations worked
            assert result_fact is not None
            assert result_fact_res is not None
            assert result_entity is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
