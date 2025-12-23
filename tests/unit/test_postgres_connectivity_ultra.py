"""
Ultra-targeted tests for postgres_connectivity.py to maximize coverage.
Targets specific uncovered lines and edge cases for 75%+ coverage.
"""

import pytest
from unittest.mock import Mock, patch, call
from datetime import datetime

from jobs.dbaccess.postgres_connectivity import (
    get_aws_secret,
    create_table,
    _write_to_postgres_optimized,
    commit_staging_to_production,
    cleanup_old_staging_tables,
    calculate_centroid_wkt
)


class TestGetAwsSecretEdgeCases:
    """Test get_aws_secret function edge cases."""
    
    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_missing_fields(self, mock_get_secret):
        """Test get_aws_secret with missing required fields."""
        # Mock incomplete secrets
        incomplete_secrets = '{"username": "test", "password": "test"}'  # Missing db_name, host, port
        mock_get_secret.return_value = incomplete_secrets
        
        with pytest.raises(ValueError, match="Missing required secret fields"):
            get_aws_secret("development")
    
    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_json_decode_error(self, mock_get_secret):
        """Test get_aws_secret with invalid JSON."""
        mock_get_secret.return_value = "invalid json {"
        
        with pytest.raises(ValueError, match="Failed to parse secrets JSON"):
            get_aws_secret("development")
    
    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_retrieval_error(self, mock_get_secret):
        """Test get_aws_secret with retrieval error."""
        mock_get_secret.side_effect = Exception("AWS error")
        
        with pytest.raises(Exception, match="Failed to retrieve AWS secrets"):
            get_aws_secret("development")


class TestCreateTableUltraEdgeCases:
    """Test create_table ultra edge cases."""
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_interface_error_all_retries_fail(self, mock_pg8000):
        """Test create_table when all retries fail with InterfaceError."""
        mock_interface_error = type('InterfaceError', (Exception,), {})
        mock_pg8000.exceptions.InterfaceError = mock_interface_error
        
        mock_pg8000.connect.side_effect = mock_interface_error("Network timeout")
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        with patch('time.sleep'), pytest.raises(mock_interface_error):
            create_table(conn_params, "test-dataset", max_retries=2)
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_non_retryable_database_error(self, mock_pg8000):
        """Test create_table with non-retryable database error."""
        mock_database_error = type('DatabaseError', (Exception,), {})
        mock_pg8000.exceptions.DatabaseError = mock_database_error
        
        mock_pg8000.connect.side_effect = mock_database_error("permission denied")
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        with pytest.raises(mock_database_error):
            create_table(conn_params, "test-dataset")
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_very_large_dataset_multiple_batches(self, mock_pg8000):
        """Test create_table with very large dataset requiring multiple batches."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchall.return_value = []  # No blocking queries
        
        # Simulate 10M records requiring multiple batches
        mock_cur.fetchone.side_effect = [
            (10000000,),  # Initial count - triggers batch deletion
            (8000000,),   # After first batch
            (6000000,),   # After second batch  
            (4000000,),   # After third batch
            (2000000,),   # After fourth batch
            (0,),         # After final batch
            (False,)      # Verification
        ]
        mock_cur.rowcount = 2000000  # 2M rows per batch
        
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        with patch('time.time', return_value=0):
            create_table(conn_params, "huge-dataset")
        
        # Should execute multiple batch deletions
        assert mock_cur.execute.call_count >= 6
        assert mock_conn.commit.call_count >= 5


class TestCommitStagingAdvanced:
    """Test commit_staging_to_production advanced scenarios."""
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_staging_interface_error_retry(self, mock_pg8000):
        """Test commit staging retries on InterfaceError."""
        mock_interface_error = type('InterfaceError', (Exception,), {})
        mock_pg8000.exceptions.InterfaceError = mock_interface_error
        
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchone.return_value = (100,)
        mock_cur.rowcount = 100
        mock_conn.cursor.return_value = mock_cur
        
        # First attempt fails, second succeeds
        mock_pg8000.connect.side_effect = [mock_interface_error("Network error"), mock_conn]
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('time.sleep'):
            result = commit_staging_to_production(conn_params, "staging_table", "dataset", max_retries=2)
        
        assert mock_pg8000.connect.call_count == 2
        assert result["success"] is True
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_staging_rollback_on_error(self, mock_pg8000):
        """Test commit staging rolls back on error."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchone.return_value = (100,)
        mock_cur.execute.side_effect = [None, Exception("Insert failed")]  # Count succeeds, insert fails
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with pytest.raises(Exception):
            commit_staging_to_production(conn_params, "staging_table", "dataset", max_retries=1)
        
        mock_conn.rollback.assert_called()


class TestCleanupStagingAdvanced:
    """Test cleanup_old_staging_tables advanced scenarios."""
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    @patch('jobs.dbaccess.postgres_connectivity.datetime')
    def test_cleanup_invalid_timestamp_format(self, mock_datetime, mock_pg8000):
        """Test cleanup skips tables with invalid timestamp format."""
        mock_conn = Mock()
        mock_cur = Mock()
        
        # Mock tables with invalid timestamp formats
        mock_cur.fetchall.return_value = [
            ('entity_staging_abc12345_invalid_timestamp',),
            ('entity_staging_xyz67890_20240101',),  # Missing time part
            ('entity_staging_def13579_20240101_123456',),  # Valid format
        ]
        
        mock_datetime.now.return_value = datetime(2024, 1, 15, 12, 0, 0)
        mock_datetime.strptime.return_value = datetime(2024, 1, 1, 12, 34, 56)
        
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        cleanup_old_staging_tables(conn_params, max_age_hours=24)
        
        # Should only drop the valid timestamp table
        drop_calls = [call for call in mock_cur.execute.call_args_list if 'DROP TABLE' in str(call)]
        assert len(drop_calls) == 1
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_cleanup_table_processing_error(self, mock_pg8000):
        """Test cleanup handles individual table processing errors."""
        mock_conn = Mock()
        mock_cur = Mock()
        
        mock_cur.fetchall.return_value = [('entity_staging_abc12345_20240101_123456',)]
        mock_cur.execute.side_effect = [None, Exception("DROP failed")]  # Find succeeds, DROP fails
        
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        
        # Should not raise exception, just log warning
        cleanup_old_staging_tables(conn_params)


class TestCalculateCentroidAdvanced:
    """Test calculate_centroid_wkt advanced scenarios."""
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_calculate_centroid_interface_error_retry(self, mock_pg8000):
        """Test centroid calculation retries on InterfaceError."""
        mock_interface_error = type('InterfaceError', (Exception,), {})
        mock_pg8000.exceptions.InterfaceError = mock_interface_error
        
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.rowcount = 25
        mock_conn.cursor.return_value = mock_cur
        
        # First attempt fails, second succeeds
        mock_pg8000.connect.side_effect = [mock_interface_error("Network error"), mock_conn]
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('time.sleep'):
            result = calculate_centroid_wkt(conn_params, max_retries=2)
        
        assert mock_pg8000.connect.call_count == 2
        assert result == 25


class TestWriteOptimizedUltraEdgeCases:
    """Test _write_to_postgres_optimized ultra edge cases."""
    
    @patch('jobs.dbaccess.postgres_connectivity.create_table')
    @patch('jobs.dbaccess.postgres_connectivity._prepare_geometry_columns')
    def test_write_optimized_retryable_connection_error(self, mock_prepare, mock_create):
        """Test write retries on connection timeout."""
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.rdd.getNumPartitions.return_value = 1
        
        # Mock retryable connection error
        mock_write = Mock()
        mock_write.side_effect = [
            Exception("connection timeout"),
            None  # Second attempt succeeds
        ]
        mock_df.write.mode.return_value.option.return_value.jdbc = mock_write
        mock_prepare.return_value = mock_df
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        with patch('time.sleep'):
            _write_to_postgres_optimized(mock_df, "test-dataset", conn_params, max_retries=2)
        
        assert mock_write.call_count == 2
    
    @patch('jobs.dbaccess.postgres_connectivity.create_table')
    @patch('jobs.dbaccess.postgres_connectivity._prepare_geometry_columns')
    def test_write_optimized_very_large_dataset_max_partitions(self, mock_prepare, mock_create):
        """Test write with very large dataset hitting max partitions."""
        mock_df = Mock()
        mock_df.count.return_value = 50000000  # 50M rows
        mock_df.rdd.getNumPartitions.return_value = 4
        mock_df.repartition.return_value = mock_df
        mock_df.write.mode.return_value.option.return_value.jdbc = Mock()
        
        mock_prepare.return_value = mock_df
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        _write_to_postgres_optimized(mock_df, "huge-dataset", conn_params)
        
        # Should repartition to optimal number for very large dataset
        mock_df.repartition.assert_called()
        # Should use conservative batch size for reliability
        mock_df.write.mode.return_value.option.return_value.jdbc.assert_called()


class TestModuleConstantsAndHelpers:
    """Test module constants and helper functions."""
    
    def test_entity_table_name_configuration(self):
        """Test ENTITY_TABLE_NAME configuration."""
        from jobs.dbaccess.postgres_connectivity import ENTITY_TABLE_NAME, dbtable_name
        
        # Verify configuration consistency
        assert ENTITY_TABLE_NAME == "entity"
        assert dbtable_name == ENTITY_TABLE_NAME
        assert isinstance(ENTITY_TABLE_NAME, str)
        assert len(ENTITY_TABLE_NAME) > 0
    
    def test_pyspark_entity_columns_schema(self):
        """Test pyspark_entity_columns schema definition."""
        from jobs.dbaccess.postgres_connectivity import pyspark_entity_columns
        
        # Verify required columns exist
        required_columns = ["dataset", "entity", "geometry", "point", "json", "geojson"]
        for col in required_columns:
            assert col in pyspark_entity_columns
        
        # Verify data types are strings
        for col, dtype in pyspark_entity_columns.items():
            assert isinstance(dtype, str)
            assert len(dtype) > 0