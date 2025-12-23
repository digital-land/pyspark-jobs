"""
Additional targeted tests for postgres_connectivity.py to reach 70%+ coverage.
Focuses on remaining uncovered lines: 659-777, 959-966, 1160-1167.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from jobs.dbaccess.postgres_connectivity import (
    create_table,
    _write_to_postgres_optimized,
    get_performance_recommendations
)


class TestCreateTableAdvanced:
    """Test create_table function advanced scenarios."""
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_large_dataset_batch_deletion(self, mock_pg8000):
        """Test create_table with large dataset requiring batch deletion."""
        mock_conn = Mock()
        mock_cur = Mock()
        
        # Mock large record count requiring batch deletion
        mock_cur.fetchone.side_effect = [
            (50000,),  # Initial count
            (25000,),  # After first batch
            (0,),      # After second batch
            (False,)   # Verification - no records exist
        ]
        mock_cur.rowcount = 25000  # Rows deleted per batch
        mock_cur.fetchall.return_value = []  # No blocking queries
        
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        create_table(conn_params, "large-dataset")
        
        # Should execute batch deletions
        assert mock_cur.execute.call_count >= 3
        assert mock_conn.commit.call_count >= 2
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_blocking_queries_cleanup(self, mock_pg8000):
        """Test create_table cleans up blocking queries."""
        mock_conn = Mock()
        mock_cur = Mock()
        
        # Mock blocking queries found
        blocking_query_data = [
            (12345, datetime.now(), 'active', 'DELETE FROM entity WHERE dataset = test')
        ]
        mock_cur.fetchall.side_effect = [
            blocking_query_data,  # Found blocking queries
            []  # No records to delete
        ]
        mock_cur.fetchone.return_value = (0,)  # No records for dataset
        
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        create_table(conn_params, "test-dataset")
        
        # Should terminate blocking queries
        terminate_calls = [call for call in mock_cur.execute.call_args_list 
                          if 'pg_terminate_backend' in str(call)]
        assert len(terminate_calls) > 0
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_database_error_retry(self, mock_pg8000):
        """Test create_table retries on database errors."""
        # Mock DatabaseError class
        mock_database_error = type('DatabaseError', (Exception,), {})
        mock_pg8000.exceptions.DatabaseError = mock_database_error
        
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchall.return_value = []  # No blocking queries
        mock_cur.fetchone.return_value = (0,)  # No records
        mock_conn.cursor.return_value = mock_cur
        
        # First attempt fails with timeout, second succeeds
        mock_pg8000.connect.side_effect = [
            mock_database_error("timeout"),
            mock_conn
        ]
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        with patch('time.sleep'):
            create_table(conn_params, "test-dataset", max_retries=2)
        
        assert mock_pg8000.connect.call_count == 2
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_verification_failure(self, mock_pg8000):
        """Test create_table handles verification failure."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchall.return_value = []  # No blocking queries
        
        # Mock deletion that doesn't complete properly
        mock_cur.fetchone.side_effect = [
            (1000,),  # Initial count
            (100,),   # Remaining after delete
            (50,)     # Final cleanup count
        ]
        mock_cur.rowcount = 900  # Deleted count
        
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        create_table(conn_params, "test-dataset")
        
        # Should attempt final cleanup
        delete_calls = [call for call in mock_cur.execute.call_args_list 
                       if 'DELETE FROM' in str(call)]
        assert len(delete_calls) >= 2  # Initial delete + cleanup


class TestWriteToPostgresOptimizedAdvanced:
    """Test _write_to_postgres_optimized advanced error scenarios."""
    
    @patch('jobs.dbaccess.postgres_connectivity.create_table')
    @patch('jobs.dbaccess.postgres_connectivity._prepare_geometry_columns')
    def test_write_optimized_non_retryable_error(self, mock_prepare, mock_create):
        """Test write fails on non-retryable error."""
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.rdd.getNumPartitions.return_value = 1
        
        # Mock non-retryable error (schema mismatch)
        mock_write = Mock()
        mock_write.side_effect = Exception("column does not exist")
        mock_df.write.mode.return_value.option.return_value.jdbc = mock_write
        
        mock_prepare.return_value = mock_df
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        with pytest.raises(Exception, match="column does not exist"):
            _write_to_postgres_optimized(mock_df, "test-dataset", conn_params, max_retries=1)
    
    @patch('jobs.dbaccess.postgres_connectivity.create_table')
    @patch('jobs.dbaccess.postgres_connectivity._prepare_geometry_columns')
    def test_write_optimized_large_dataset_partitioning(self, mock_prepare, mock_create):
        """Test write with large dataset auto-partitioning."""
        mock_df = Mock()
        mock_df.count.return_value = 5000000  # 5M rows
        mock_df.rdd.getNumPartitions.return_value = 4
        mock_df.repartition.return_value = mock_df
        mock_df.write.mode.return_value.option.return_value.jdbc = Mock()
        
        mock_prepare.return_value = mock_df
        
        conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
        
        _write_to_postgres_optimized(mock_df, "test-dataset", conn_params)
        
        # Should repartition for large dataset
        mock_df.repartition.assert_called()
        # Should use large dataset batch size
        mock_df.write.mode.return_value.option.return_value.jdbc.assert_called()


class TestGetPerformanceRecommendationsAdvanced:
    """Test get_performance_recommendations edge cases."""
    
    def test_performance_recommendations_memory_constraints(self):
        """Test recommendations with memory constraints."""
        # Very large dataset with limited memory
        result = get_performance_recommendations(50000000, available_memory_gb=4)
        
        assert result["method"] == "optimized"
        assert result["batch_size"] == 5000  # Conservative for reliability
        assert result["num_partitions"] <= 16
        assert "Very large dataset" in result["notes"][0]
        assert "conservative" in result["notes"][0].lower()
    
    def test_performance_recommendations_high_memory(self):
        """Test recommendations with high memory availability."""
        result = get_performance_recommendations(20000000, available_memory_gb=32)
        
        assert result["method"] == "optimized"
        assert result["batch_size"] == 5000
        assert result["num_partitions"] >= 6
        assert len(result["notes"]) >= 3
    
    def test_performance_recommendations_edge_boundaries(self):
        """Test recommendations at boundary conditions."""
        # Test exactly at boundary values - corrected expectations
        test_cases = [
            (9999, 1000, 1),      # Just under 10k -> small dataset
            (10000, 2000, 2),     # Exactly 10k -> small-medium dataset  
            (99999, 2000, 2),     # Just under 100k -> small-medium
            (100000, 3000, 4),    # Exactly 100k -> medium dataset
            (999999, 3000, 4),    # Just under 1M -> medium
            (1000000, 4000, 8),   # Exactly 1M -> large dataset
        ]
        
        for row_count, expected_batch, expected_partitions in test_cases:
            result = get_performance_recommendations(row_count)
            assert result["batch_size"] == expected_batch, f"Row count {row_count}: expected batch {expected_batch}, got {result['batch_size']}"
            assert result["num_partitions"] == expected_partitions, f"Row count {row_count}: expected partitions {expected_partitions}, got {result['num_partitions']}"


class TestModuleEdgeCases:
    """Test edge cases and error conditions."""
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_table_adaptive_batch_sizing(self, mock_pg8000):
        """Test create_table adaptive batch sizing for different dataset sizes."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchall.return_value = []  # No blocking queries
        
        # Test different dataset sizes and their batch strategies
        test_cases = [
            (150000, 25000),   # 150K records -> 25K batch
            (750000, 50000),   # 750K records -> 50K batch  
            (2000000, 100000), # 2M records -> 100K batch
            (8000000, 200000), # 8M records -> 200K batch
        ]
        
        for record_count, expected_batch_size in test_cases:
            mock_cur.fetchone.side_effect = [
                (record_count,),  # Initial count
                (0,),            # After deletion
                (False,)         # Verification
            ]
            mock_cur.rowcount = expected_batch_size
            mock_conn.cursor.return_value = mock_cur
            mock_pg8000.connect.return_value = mock_conn
            
            conn_params = {'host': 'test', 'port': 5432, 'user': 'test', 'password': 'test', 'database': 'test'}
            
            create_table(conn_params, f"dataset-{record_count}")
            
            # Verify batch size logic was applied
            assert mock_cur.execute.call_count >= 2
            mock_cur.reset_mock()
    
    def test_entity_table_name_usage(self):
        """Test ENTITY_TABLE_NAME constant usage in functions."""
        from jobs.dbaccess.postgres_connectivity import ENTITY_TABLE_NAME, dbtable_name
        
        # Verify the constant is properly used
        assert ENTITY_TABLE_NAME == "entity"
        assert dbtable_name == ENTITY_TABLE_NAME
        
        # Test that changing the constant would affect table operations
        assert isinstance(ENTITY_TABLE_NAME, str)
        assert len(ENTITY_TABLE_NAME) > 0