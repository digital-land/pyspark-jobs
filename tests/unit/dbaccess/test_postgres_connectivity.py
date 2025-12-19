"""Unit tests for postgres_connectivity module."""
import pytest
import os
import sys
import json
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from jobs.dbaccess.postgres_connectivity import (
    get_aws_secret, cleanup_old_staging_tables, create_and_prepare_staging_table,
    commit_staging_to_production, get_performance_recommendations, ENTITY_TABLE_NAME
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

    @pytest.mark.parametrize("mock_data,expected_error", [
        ({'username': 'test_user'}, "Missing required secret fields"),
        ("invalid json", "Failed to parse secrets JSON"),
    ])
    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_errors(self, mock_get_secret, mock_data, expected_error):
        """Test AWS secret retrieval error cases."""
        if isinstance(mock_data, dict):
            mock_get_secret.return_value = json.dumps(mock_data)
        else:
            mock_get_secret.return_value = mock_data
        
        with pytest.raises(ValueError, match=expected_error):
            get_aws_secret("development")

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_exception(self, mock_get_secret):
        """Test AWS secret retrieval with exception."""
        mock_get_secret.side_effect = Exception("Secret retrieval failed")
        
        with pytest.raises(Exception, match="Secret retrieval failed"):
            get_aws_secret("development")

    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_cleanup_old_staging_tables_success(self, mock_pg8000):
        """Test successful cleanup of old staging tables."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [('entity_staging_test',)]
        
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
        
        result = create_and_prepare_staging_table(conn_params, "test-dataset")
        
        assert result is None
        assert "pg8000 not available" in caplog.text

    @pytest.mark.parametrize("staging_count,insert_count,expected_success,expected_error", [
        (100, 100, True, None),  # Success case
        (0, 0, False, "empty"),  # Empty staging
        (100, 90, False, "mismatch"),  # Row mismatch
    ])
    @patch('jobs.dbaccess.postgres_connectivity.pg8000')
    def test_commit_staging_to_production(self, mock_pg8000, staging_count, insert_count, expected_success, expected_error):
        """Test staging to production commit scenarios."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_pg8000.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        mock_cursor.fetchone.return_value = (staging_count,)
        mock_cursor.rowcount = insert_count
        
        conn_params = {'host': 'test', 'port': 5432}
        result = commit_staging_to_production(
            conn_params, "entity_staging_test", "test-dataset"
        )
        
        assert result['success'] is expected_success
        if expected_error:
            assert expected_error in result['error']
        else:
            assert result['rows_inserted'] == insert_count



    @pytest.mark.parametrize("row_count,expected_batch,expected_partitions", [
        (5000, 1000, 1),      # Small
        (500000, 3000, 4),    # Medium  
        (5000000, 4000, 8),   # Large
    ])
    def test_get_performance_recommendations(self, row_count, expected_batch, expected_partitions):
        """Test performance recommendations for different dataset sizes."""
        recommendations = get_performance_recommendations(row_count)
        
        assert recommendations['method'] == 'optimized'
        assert recommendations['batch_size'] == expected_batch
        assert recommendations['num_partitions'] == expected_partitions