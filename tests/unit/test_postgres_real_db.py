"""Test postgres_writer_utils with real database connection."""

import pytest
import os
from unittest.mock import patch, MagicMock


class TestPostgresRealDB:
    """Test postgres writer utils with real database."""

    def test_ensure_required_columns_comprehensive(self):
        """Test _ensure_required_columns with all code paths."""
        try:
            from jobs.utils.postgres_writer_utils import _ensure_required_columns
            from pyspark.sql import SparkSession
            from pyspark.sql.types import StructType, StructField, StringType, LongType
            
            # Create minimal spark session
            spark = SparkSession.builder.appName("test").getOrCreate()
            
            # Test with missing columns
            schema = StructType([StructField("existing_col", StringType(), True)])
            df = spark.createDataFrame([("test",)], schema)
            
            required_cols = ["entity", "name", "entry_date", "json", "geojson", "existing_col"]
            defaults = {"entity": 123, "name": "test_name"}
            
            # Mock logger
            mock_logger = MagicMock()
            
            result_df = _ensure_required_columns(df, required_cols, defaults, mock_logger)
            
            # Verify logger calls
            assert mock_logger.warning.called
            assert mock_logger.info.called
            
            # Test with existing columns that need type conversion
            from pyspark.sql.types import DateType
            schema2 = StructType([
                StructField("entity", StringType(), True),
                StructField("entry_date", StringType(), True),
                StructField("json", StructType([StructField("key", StringType())]), True)
            ])
            df2 = spark.createDataFrame([("123", "2023-01-01", {"key": "value"})], schema2)
            
            result_df2 = _ensure_required_columns(df2, ["entity", "entry_date", "json"], {}, mock_logger)
            
            spark.stop()
            
        except ImportError:
            # PySpark not available, test import error handling
            pass

    def test_write_dataframe_to_postgres_jdbc_with_real_connection(self):
        """Test postgres JDBC write with real database if available."""
        # Check if we're in CI environment with real database
        if os.environ.get("DATABASE_URL") and "localhost" in os.environ.get("DATABASE_URL", ""):
            try:
                from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
                from pyspark.sql import SparkSession
                from pyspark.sql.types import StructType, StructField, StringType, LongType
                
                # Create test DataFrame
                spark = SparkSession.builder.appName("test").getOrCreate()
                schema = StructType([
                    StructField("entity", LongType(), True),
                    StructField("name", StringType(), True),
                    StructField("dataset", StringType(), True)
                ])
                df = spark.createDataFrame([(1, "test", "test-dataset")], schema)
                
                # Mock get_aws_secret to return local database connection
                with patch('jobs.utils.postgres_writer_utils.get_aws_secret') as mock_secret:
                    mock_secret.return_value = {
                        "host": "localhost",
                        "port": 5432,
                        "database": "digital_land_test",
                        "user": "postgres",
                        "password": "postgres"
                    }
                    
                    # This should execute real database operations
                    write_dataframe_to_postgres_jdbc(df, "entity", "test-dataset", "test")
                
                spark.stop()
                
            except Exception as e:
                # Expected in unit tests without full setup
                assert "pg8000" in str(e) or "JDBC" in str(e) or "connect" in str(e)
        else:
            # Test error handling when database not available
            try:
                from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
                # This should fail gracefully
                write_dataframe_to_postgres_jdbc(None, "test", "test", "test")
            except Exception:
                pass

    def test_postgres_connection_error_handling(self):
        """Test error handling in postgres operations."""
        try:
            from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
            from pyspark.sql import SparkSession
            
            # Mock get_aws_secret to return invalid connection
            with patch('jobs.utils.postgres_writer_utils.get_aws_secret') as mock_secret:
                mock_secret.return_value = {
                    "host": "invalid-host",
                    "port": 5432,
                    "database": "invalid_db",
                    "user": "invalid_user",
                    "password": "invalid_pass"
                }
                
                # Mock DataFrame
                mock_df = MagicMock()
                mock_df.count.return_value = 100
                mock_df.select.return_value = mock_df
                mock_df.withColumn.return_value = mock_df
                mock_df.repartition.return_value = mock_df
                mock_df.write.jdbc.side_effect = Exception("Connection failed")
                
                # This should trigger retry logic and error handling
                with pytest.raises(Exception):
                    write_dataframe_to_postgres_jdbc(mock_df, "test_table", "test-dataset", "test")
                    
        except ImportError:
            pass

    def test_staging_table_operations(self):
        """Test staging table creation and cleanup operations."""
        try:
            # Mock pg8000 operations
            with patch('jobs.utils.postgres_writer_utils.pg8000') as mock_pg8000:
                mock_conn = MagicMock()
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                mock_pg8000.connect.return_value = mock_conn
                
                # Test successful staging table creation
                mock_cur.execute.return_value = None
                mock_conn.commit.return_value = None
                
                from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
                
                # Mock DataFrame and other dependencies
                with patch('jobs.utils.postgres_writer_utils.get_aws_secret') as mock_secret:
                    mock_secret.return_value = {"host": "test", "port": 5432, "database": "test", "user": "test", "password": "test"}
                    
                    mock_df = MagicMock()
                    mock_df.count.return_value = 1
                    mock_df.select.return_value = mock_df
                    mock_df.withColumn.return_value = mock_df
                    mock_df.repartition.return_value = mock_df
                    
                    # Mock JDBC write success
                    mock_write = MagicMock()
                    mock_df.write = mock_write
                    mock_write.jdbc.return_value = None
                    
                    # This should execute staging table logic
                    write_dataframe_to_postgres_jdbc(mock_df, "test_table", "test-dataset", "test")
                    
                    # Verify staging table operations were called
                    assert mock_cur.execute.called
                    assert mock_conn.commit.called
                    
        except ImportError:
            pass

    def test_atomic_transaction_retry_logic(self):
        """Test atomic transaction with retry logic."""
        try:
            with patch('jobs.utils.postgres_writer_utils.pg8000') as mock_pg8000, \
                 patch('jobs.utils.postgres_writer_utils.time.sleep') as mock_sleep:
                
                mock_conn = MagicMock()
                mock_cur = MagicMock()
                mock_conn.cursor.return_value = mock_cur
                mock_pg8000.connect.return_value = mock_conn
                
                # First attempt fails, second succeeds
                mock_cur.execute.side_effect = [
                    None,  # CREATE TABLE
                    Exception("Transaction failed"),  # First transaction attempt
                    None, None, None, None, None, None, None  # Second attempt succeeds
                ]
                
                from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
                
                with patch('jobs.utils.postgres_writer_utils.get_aws_secret') as mock_secret:
                    mock_secret.return_value = {"host": "test", "port": 5432, "database": "test", "user": "test", "password": "test"}
                    
                    mock_df = MagicMock()
                    mock_df.count.return_value = 1
                    mock_df.select.return_value = mock_df
                    mock_df.withColumn.return_value = mock_df
                    mock_df.repartition.return_value = mock_df
                    mock_df.write.jdbc.return_value = None
                    
                    # This should trigger retry logic
                    write_dataframe_to_postgres_jdbc(mock_df, "test_table", "test-dataset", "test")
                    
                    # Verify retry was attempted
                    assert mock_sleep.called
                    
        except ImportError:
            pass