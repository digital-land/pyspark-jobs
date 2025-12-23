"""
Extended tests for postgres_connectivity.py to improve coverage.
Targets uncovered lines: 659-777, 780-805, 808-821, 1141-1167, etc.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
import json
import time
from datetime import datetime, timedelta

# Import the module under test
from jobs.dbaccess.postgres_connectivity import (
    cleanup_old_staging_tables,
    create_and_prepare_staging_table,
    commit_staging_to_production,
    calculate_centroid_wkt,
    _prepare_geometry_columns,
    get_performance_recommendations,
    _write_to_postgres_optimized,
    write_to_postgres,
    ENTITY_TABLE_NAME
)


class TestCleanupOldStagingTables:
    """Test cleanup_old_staging_tables function."""
    
    def test_cleanup_no_pg8000(self):
        """Test cleanup when pg8000 is not available."""
        with patch('jobs.dbaccess.postgres_connectivity.pg8000', None):
            # Should return without error
            cleanup_old_staging_tables({})
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_cleanup_no_staging_tables(self, mock_pg8000):
        """Test cleanup when no staging tables exist."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        cleanup_old_staging_tables(conn_params)
        
        mock_cur.execute.assert_called()
        mock_cur.fetchall.assert_called_once()
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    @patch('jobs.dbaccess.postgres_connectivity.datetime')
    def test_cleanup_drops_old_tables(self, mock_datetime, mock_pg8000):
        """Test cleanup drops old staging tables."""
        # Mock current time
        mock_now = datetime(2024, 1, 15, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strptime.return_value = datetime(2024, 1, 14, 10, 0, 0)  # 26 hours ago
        
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchall.return_value = [('entity_staging_abc12345_20240114_100000',)]
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        cleanup_old_staging_tables(conn_params, max_age_hours=24)
        
        # Should execute DROP TABLE
        drop_calls = [call for call in mock_cur.execute.call_args_list 
                     if 'DROP TABLE' in str(call)]
        assert len(drop_calls) > 0
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_cleanup_connection_retry(self, mock_pg8000):
        """Test cleanup retries on connection failure."""
        mock_pg8000.connect.side_effect = [Exception("Connection failed"), Exception("Still failing")]
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('time.sleep'):
            cleanup_old_staging_tables(conn_params, max_retries=2)
        
        assert mock_pg8000.connect.call_count == 2


class TestCreateAndPrepareStagingTable:
    """Test create_and_prepare_staging_table function."""
    
    def test_create_staging_no_pg8000(self):
        """Test staging table creation when pg8000 is not available."""
        with patch('jobs.dbaccess.postgres_connectivity.pg8000', None):
            with pytest.raises(ImportError, match="pg8000 required"):
                create_and_prepare_staging_table({}, "test-dataset")
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    @patch('jobs.dbaccess.postgres_connectivity.cleanup_old_staging_tables')
    def test_create_staging_success(self, mock_cleanup, mock_pg8000):
        """Test successful staging table creation."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        result = create_and_prepare_staging_table(conn_params, "test-dataset")
        
        assert result is not None
        assert "staging" in result
        mock_cleanup.assert_called_once()
        mock_cur.execute.assert_called()
        mock_conn.commit.assert_called()
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_create_staging_retry_on_interface_error(self, mock_pg8000):
        """Test staging table creation retries on InterfaceError."""
        # Mock InterfaceError class
        mock_interface_error = type('InterfaceError', (Exception,), {})
        mock_pg8000.exceptions.InterfaceError = mock_interface_error
        
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        # First call raises error, second succeeds
        mock_pg8000.connect.side_effect = [mock_interface_error("Network error"), mock_conn]
        
        conn_params = {'host': 'test', 'port': 5432}
        
        with patch('time.sleep'), patch('jobs.dbaccess.postgres_connectivity.cleanup_old_staging_tables'):
            result = create_and_prepare_staging_table(conn_params, "test-dataset", max_retries=2)
        
        assert mock_pg8000.connect.call_count == 2
        assert result is not None


class TestCommitStagingToProduction:
    """Test commit_staging_to_production function."""
    
    def test_commit_no_pg8000(self):
        """Test commit when pg8000 is not available."""
        with patch('jobs.dbaccess.postgres_connectivity.pg8000', None):
            with pytest.raises(ImportError, match="pg8000 required"):
                commit_staging_to_production({}, "staging_table", "dataset")
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_empty_staging_table(self, mock_pg8000):
        """Test commit with empty staging table."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchone.return_value = (0,)  # Empty staging table
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        result = commit_staging_to_production(conn_params, "staging_table", "test-dataset")
        
        assert result["success"] is False
        assert "empty" in result["error"]
        mock_conn.rollback.assert_called_once()
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_row_count_mismatch(self, mock_pg8000):
        """Test commit with row count mismatch."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchone.return_value = (100,)  # 100 rows in staging
        mock_cur.rowcount = 50  # Only 50 inserted
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        result = commit_staging_to_production(conn_params, "staging_table", "test-dataset")
        
        assert result["success"] is False
        assert "mismatch" in result["error"]
        mock_conn.rollback.assert_called_once()
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_success(self, mock_pg8000):
        """Test successful commit operation."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.fetchone.return_value = (100,)  # 100 rows in staging
        mock_cur.rowcount = 100  # 100 rows processed
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        result = commit_staging_to_production(conn_params, "staging_table", "test-dataset")
        
        assert result["success"] is True
        assert result["rows_inserted"] == 100
        mock_conn.commit.assert_called()


class TestCalculateCentroidWkt:
    """Test calculate_centroid_wkt function."""
    
    def test_calculate_centroid_no_pg8000(self):
        """Test centroid calculation when pg8000 is not available."""
        with patch('jobs.dbaccess.postgres_connectivity.pg8000', None):
            with pytest.raises(AttributeError):
                calculate_centroid_wkt({})
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_calculate_centroid_success(self, mock_pg8000):
        """Test successful centroid calculation."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.rowcount = 50
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        result = calculate_centroid_wkt(conn_params)
        
        assert result == 50
        mock_cur.execute.assert_called()
        mock_conn.commit.assert_called_once()
    
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_calculate_centroid_with_target_table(self, mock_pg8000):
        """Test centroid calculation with specific target table."""
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.rowcount = 25
        mock_conn.cursor.return_value = mock_cur
        mock_pg8000.connect.return_value = mock_conn
        
        conn_params = {'host': 'test', 'port': 5432}
        result = calculate_centroid_wkt(conn_params, target_table="staging_table")
        
        assert result == 25
        mock_cur.execute.assert_called()
        mock_conn.commit.assert_called_once()





class TestModuleConstants:
    """Test module-level constants and configurations."""
    
    def test_entity_table_name_constant(self):
        """Test ENTITY_TABLE_NAME constant is properly set."""
        assert ENTITY_TABLE_NAME == "entity"
        assert isinstance(ENTITY_TABLE_NAME, str)
        assert len(ENTITY_TABLE_NAME) > 0



class TestModuleConstants:
    """Test module-level constants and configurations."""
    
    def test_entity_table_name_constant(self):
        """Test ENTITY_TABLE_NAME constant is properly set."""
        assert ENTITY_TABLE_NAME == "entity"
        assert isinstance(ENTITY_TABLE_NAME, str)
        assert len(ENTITY_TABLE_NAME) > 0