"""Unit tests for postgres_connectivity module."""
import pytest
import os
import sys
import json
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from jobs.dbaccess.postgres_connectivity import (
    get_aws_secret, cleanup_old_staging_tables, create_and_prepare_staging_table,
    commit_staging_to_production, create_table, calculate_centroid_wkt,
    write_to_postgres, get_performance_recommendations, _prepare_geometry_columns,
    _write_to_postgres_optimized, ENTITY_TABLE_NAME
)


class TestPostgresConnectivity:
    """Test suite for postgres_connectivity module."""

    def test_entity_table_name_constant(self):
        """Test that ENTITY_TABLE_NAME is properly defined."""
        assert ENTITY_TABLE_NAME == "entity"

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_success(self, mock_get_secret):
        """Test successful AWS secret retrieval."""
        mock_secret = {
            'username': 'test_user',
            'password': 'test_pass',
            'db_name': 'test_db',
            'host': 'test_host',
            'port': '5432'
        }
        mock_get_secret.return_value = json.dumps(mock_secret)
        
        result = get_aws_secret("development")
        
        assert result['user'] == 'test_user'
        assert result['password'] == 'test_pass'
        assert result['database'] == 'test_db'
        assert result['host'] == 'test_host'
        assert result['port'] == 5432
        assert 'ssl_context' in result

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_missing_fields(self, mock_get_secret):
        """Test AWS secret retrieval with missing required fields."""
        mock_secret = {'username': 'test_user'}  # Missing other fields
        mock_get_secret.return_value = json.dumps(mock_secret)
        
        with pytest.raises(ValueError, match="Missing required secret fields"):
            get_aws_secret("development")

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_invalid_json(self, mock_get_secret):
        """Test AWS secret retrieval with invalid JSON."""
        mock_get_secret.return_value = "invalid json"
        
        with pytest.raises(ValueError, match="Failed to parse secrets JSON"):
            get_aws_secret("development")

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_exception(self, mock_get_secret):
        """Test AWS secret retrieval with exception."""
        mock_get_secret.side_effect = Exception("Secret retrieval failed")
        
        with pytest.raises(Exception, match="Failed to retrieve AWS secrets"):
            get_aws_secret("development")

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_cleanup_old_staging_tables_success(self, mock_pg8000):
        """Test successful cleanup of old staging tables."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock finding staging tables
        mock_cursor.fetchall.return_value = [
            ('entity_staging_abc12345_20230101_120000',),
            ('entity_staging_def67890_20230102_130000',)
        ]
        
        conn_params = {'host': 'test', 'port': 5432}
        cleanup_old_staging_tables(conn_params, max_age_hours=1)
        
        mock_pg8000.connect.assert_called_once_with(**conn_params)
        mock_cursor.execute.assert_called()

    @patch('jobs.dbaccess.postgres_connectivity.pg8000', None)
    def test_cleanup_old_staging_tables_no_pg8000(self, caplog):
        """Test cleanup when pg8000 is not available."""
        conn_params = {'host': 'test', 'port': 5432}
        
        cleanup_old_staging_tables(conn_params)
        
        assert "pg8000 not available" in caplog.text

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_cleanup_old_staging_tables_exception(self, mock_pg8000):
        """Test cleanup with database exception."""
        mock_pg8000.connect.side_effect = Exception("Connection failed")
        
        conn_params = {'host': 'test', 'port': 5432}
        cleanup_old_staging_tables(conn_params, max_retries=1)
        
        # Should handle exception gracefully
        assert True

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    @patch('jobs.dbaccess.postgres_connectivity.cleanup_old_staging_tables')
    def test_create_and_prepare_staging_table_success(self, mock_cleanup, mock_pg8000):
        """Test successful staging table creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        conn_params = {'host': 'test', 'port': 5432}
        result = create_and_prepare_staging_table(conn_params, "test-dataset")
        
        assert result.startswith("entity_staging_")
        mock_pg8000.connect.assert_called()
        mock_cursor.execute.assert_called()

    @patch('jobs.dbaccess.postgres_connectivity.pg8000', None)
    def test_create_and_prepare_staging_table_no_pg8000(self, caplog):
        """Test staging table creation when pg8000 is not available."""
        conn_params = {'host': 'test', 'port': 5432}
        
        with pytest.raises(ImportError):
            create_and_prepare_staging_table(conn_params, "test-dataset")

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_and_prepare_staging_table_retry(self, mock_pg8000):
        """Test staging table creation with retry logic."""
        mock_pg8000.connect.side_effect = [Exception("First attempt"), Mock()]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('time.sleep'):
            result = create_and_prepare_staging_table(conn_params, "test-dataset", max_retries=2)
        
        assert result.startswith("entity_staging_")

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_staging_to_production_success(self, mock_pg8000):
        """Test successful staging to production commit."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock row counts
        mock_cursor.fetchone.side_effect = [(100,), None]  # staging count, then None
        mock_cursor.rowcount = 100  # for both delete and insert
        
        conn_params = {'host': 'test', 'port': 5432}
        result = commit_staging_to_production(
            conn_params, "entity_staging_test", "test-dataset"
        )
        
        assert result['success'] is True
        assert result['rows_inserted'] == 100
        assert result['rows_deleted'] == 100

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_staging_to_production_empty_staging(self, mock_pg8000):
        """Test commit with empty staging table."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock empty staging table
        mock_cursor.fetchone.return_value = (0,)
        
        conn_params = {'host': 'test', 'port': 5432}
        result = commit_staging_to_production(
            conn_params, "entity_staging_test", "test-dataset"
        )
        
        assert result['success'] is False
        assert "empty" in result['error']

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_staging_to_production_row_mismatch(self, mock_pg8000):
        """Test commit with row count mismatch."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock row count mismatch
        mock_cursor.fetchone.return_value = (100,)  # staging count
        mock_cursor.rowcount = 90  # inserted count (mismatch)
        
        conn_params = {'host': 'test', 'port': 5432}
        result = commit_staging_to_production(
            conn_params, "entity_staging_test", "test-dataset"
        )
        
        assert result['success'] is False
        assert "mismatch" in result['error']

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_success(self, mock_pg8000):
        """Test successful table creation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock no existing records
        mock_cursor.fetchone.return_value = (0,)
        
        conn_params = {'host': 'test', 'port': 5432}
        create_table(conn_params, "test-dataset")
        
        mock_pg8000.connect.assert_called_with(**conn_params)
        mock_cursor.execute.assert_called()

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_with_existing_data(self, mock_pg8000):
        """Test table creation with existing data deletion."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock existing records that need deletion
        mock_cursor.fetchone.side_effect = [(5000,), (0,)]  # count, then verify
        mock_cursor.rowcount = 5000
        
        conn_params = {'host': 'test', 'port': 5432}
        create_table(conn_params, "test-dataset")
        
        # Should execute DELETE operation
        assert mock_cursor.execute.call_count > 1

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_large_dataset_batch_deletion(self, mock_pg8000):
        """Test table creation with large dataset batch deletion."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock large dataset requiring batch deletion
        mock_cursor.fetchone.side_effect = [(50000,), (0,)]  # count, then verify
        mock_cursor.rowcount = 10000  # batch size
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('time.sleep'):
            create_table(conn_params, "test-dataset")
        
        # Should execute multiple DELETE operations
        assert mock_cursor.execute.call_count > 2

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_retry_logic(self, mock_pg8000):
        """Test table creation retry logic."""
        # First attempt fails, second succeeds
        mock_pg8000.connect.side_effect = [Exception("Connection failed"), Mock()]
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (0,)
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('time.sleep'):
            create_table(conn_params, "test-dataset", max_retries=2)
        
        assert mock_pg8000.connect.call_count == 2

    @patch('jobs.dbaccess.postgres_connectivity.pg8000', None)
    def test_create_table_no_pg8000(self, caplog):
        """Test table creation when pg8000 is not available."""
        conn_params = {'host': 'test', 'port': 5432}
        
        with pytest.raises(ImportError):
            create_table(conn_params, "test-dataset")

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_calculate_centroid_wkt_success(self, mock_pg8000):
        """Test successful centroid calculation."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 50
        
        conn_params = {'host': 'test', 'port': 5432}
        result = calculate_centroid_wkt(conn_params)
        
        assert result == 50
        mock_cursor.execute.assert_called()

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_calculate_centroid_wkt_with_target_table(self, mock_pg8000):
        """Test centroid calculation with specific target table."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.rowcount = 25
        
        conn_params = {'host': 'test', 'port': 5432}
        result = calculate_centroid_wkt(conn_params, target_table="staging_table")
        
        assert result == 25

    @patch('jobs.dbaccess.postgres_connectivity.pg8000', None)
    def test_calculate_centroid_wkt_no_pg8000(self, caplog):
        """Test centroid calculation when pg8000 is not available."""
        conn_params = {'host': 'test', 'port': 5432}
        
        calculate_centroid_wkt(conn_params)
        
        assert "pg8000 not available" in caplog.text

    def test_prepare_geometry_columns(self, spark):
        """Test geometry column preparation for PostgreSQL."""
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("point", StringType(), True)
        ])
        
        data = [
            ("123", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", "POINT(0.5 0.5)"),
            ("456", "", "POINT(1 1)")
        ]
        df = spark.createDataFrame(data, schema)
        
        result = _prepare_geometry_columns(df)
        
        # Check that entity column is cast to LongType
        entity_field = next(f for f in result.schema.fields if f.name == "entity")
        assert isinstance(entity_field.dataType, LongType)

    @patch('jobs.dbaccess.postgres_connectivity.create_table')
    @patch('jobs.dbaccess.postgres_connectivity._write_to_postgres_optimized')
    def test_write_to_postgres_optimized_method(self, mock_optimized, mock_create_table, spark):
        """Test write_to_postgres with optimized method."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([StructField("test_col", StringType(), True)])
        df = spark.createDataFrame([("test",)], schema)
        conn_params = {'host': 'test', 'port': 5432}
        
        write_to_postgres(df, "test-dataset", conn_params, method="optimized")
        
        mock_create_table.assert_called_once()
        mock_optimized.assert_called_once()

    @patch('jobs.dbaccess.postgres_connectivity.create_table')
    def test_write_to_postgres_optimized_implementation(self, mock_create_table, spark):
        """Test optimized PostgreSQL writer implementation."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([StructField("test_col", StringType(), True)])
        df = spark.createDataFrame([("test1",), ("test2",)], schema)
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        # Mock the JDBC write operation
        with patch.object(df.write, 'jdbc') as mock_jdbc:
            _write_to_postgres_optimized(df, "test-dataset", conn_params, batch_size=1000, num_partitions=1)
            
            mock_jdbc.assert_called_once()
            mock_create_table.assert_called_once()

    def test_write_to_postgres_optimized_retry_logic(self, spark):
        """Test optimized writer retry logic."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([StructField("test_col", StringType(), True)])
        df = spark.createDataFrame([("test",)], schema)
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        with patch('jobs.dbaccess.postgres_connectivity.create_table'):
            with patch.object(df.write, 'jdbc', side_effect=Exception("Connection timeout")) as mock_jdbc:
                with patch('time.sleep'):
                    with pytest.raises(Exception):
                        _write_to_postgres_optimized(df, "test-dataset", conn_params, max_retries=2)
                
                assert mock_jdbc.call_count == 2

    def test_get_performance_recommendations_small_dataset(self):
        """Test performance recommendations for small dataset."""
        recommendations = get_performance_recommendations(5000)
        
        assert recommendations['method'] == 'optimized'
        assert recommendations['batch_size'] == 1000
        assert recommendations['num_partitions'] == 1
        assert "Small dataset" in recommendations['notes'][0]

    def test_get_performance_recommendations_medium_dataset(self):
        """Test performance recommendations for medium dataset."""
        recommendations = get_performance_recommendations(500000)
        
        assert recommendations['method'] == 'optimized'
        assert recommendations['batch_size'] == 3000
        assert recommendations['num_partitions'] == 4

    def test_get_performance_recommendations_large_dataset(self):
        """Test performance recommendations for large dataset."""
        recommendations = get_performance_recommendations(5000000)
        
        assert recommendations['method'] == 'optimized'
        assert recommendations['batch_size'] == 4000
        assert recommendations['num_partitions'] == 8

    def test_get_performance_recommendations_very_large_dataset(self):
        """Test performance recommendations for very large dataset."""
        recommendations = get_performance_recommendations(15000000, available_memory_gb=16)
        
        assert recommendations['method'] == 'optimized'
        assert recommendations['batch_size'] == 5000
        assert recommendations['num_partitions'] <= 16
        assert "Very large dataset" in recommendations['notes'][0]

    def test_get_performance_recommendations_memory_constraint(self):
        """Test performance recommendations with memory constraints."""
        recommendations = get_performance_recommendations(20000000, available_memory_gb=4)
        
        assert recommendations['method'] == 'optimized'
        assert recommendations['num_partitions'] <= 6  # Limited by memory


@pytest.mark.unit
class TestPostgresConnectivityIntegration:
    """Integration-style tests for postgres_connectivity module."""

    def test_staging_table_workflow_simulation(self):
        """Test complete staging table workflow simulation."""
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('jobs.dbaccess.postgres_connectivity.pg8000') as mock_pg8000:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_pg8000.connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor
            
            # Simulate staging table creation
            staging_table = create_and_prepare_staging_table(conn_params, "test-dataset")
            assert staging_table.startswith("entity_staging_")
            
            # Simulate successful commit
            mock_cursor.fetchone.side_effect = [(100,), None]
            mock_cursor.rowcount = 100
            
            result = commit_staging_to_production(conn_params, staging_table, "test-dataset")
            assert result['success'] is True

    def test_error_handling_chain(self):
        """Test error handling through different function calls."""
        conn_params = {'host': 'test', 'port': 5432}
        
        # Test various error scenarios
        with patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible') as mock_secret:
            # Test JSON parsing error
            mock_secret.return_value = "invalid json"
            with pytest.raises(ValueError):
                get_aws_secret("development")
            
            # Test missing fields error
            mock_secret.return_value = json.dumps({'username': 'test'})
            with pytest.raises(ValueError):
                get_aws_secret("development")

    def test_performance_recommendations_workflow(self):
        """Test complete performance recommendations workflow."""
        test_cases = [
            (1000, 1000, 1),      # Small
            (50000, 2000, 2),     # Small-medium
            (500000, 3000, 4),    # Medium
            (5000000, 4000, 8),   # Large
            (50000000, 5000, 16)  # Very large
        ]
        
        for row_count, expected_batch, max_partitions in test_cases:
            recommendations = get_performance_recommendations(row_count)
            
            assert recommendations['method'] == 'optimized'
            assert recommendations['batch_size'] == expected_batch
            assert recommendations['num_partitions'] <= max_partitions