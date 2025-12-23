"""
Unit tests for postgres_writer_utils module.
Tests database writing functionality with proper mocking.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

# Mock pg8000 before import
with patch.dict('sys.modules', {
    'pg8000': Mock(),
    'pg8000.exceptions': Mock()
}):
    from jobs.utils import postgres_writer_utils


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame."""
    df = Mock()
    df.columns = ['entity', 'name', 'dataset']
    df.count.return_value = 1000
    df.withColumn.return_value = df
    df.select.return_value = df
    df.repartition.return_value = df
    df.write = Mock()
    df.write.jdbc = Mock()
    return df


@pytest.fixture
def mock_conn_params():
    """Create mock connection parameters."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'testdb',
        'user': 'testuser',
        'password': 'testpass'
    }


@pytest.mark.unit
class TestEnsureRequiredColumns:
    """Test _ensure_required_columns function."""

    def test_ensure_required_columns_basic(self, mock_dataframe):
        """Test basic column addition."""
        required_cols = ['entity', 'name', 'dataset', 'missing_col']
        
        result = postgres_writer_utils._ensure_required_columns(
            mock_dataframe, required_cols
        )
        
        # Should add missing column
        mock_dataframe.withColumn.assert_called()
        assert result is not None

    def test_ensure_required_columns_with_defaults(self, mock_dataframe):
        """Test column addition with default values."""
        required_cols = ['entity', 'name', 'dataset', 'missing_col']
        defaults = {'missing_col': 'default_value'}
        
        result = postgres_writer_utils._ensure_required_columns(
            mock_dataframe, required_cols, defaults=defaults
        )
        
        mock_dataframe.withColumn.assert_called()
        assert result is not None

    def test_ensure_required_columns_with_logger(self, mock_dataframe):
        """Test column addition with logger warnings."""
        mock_logger = Mock()
        required_cols = ['entity', 'name', 'dataset', 'missing_col']
        mock_dataframe.columns = ['entity', 'name', 'extra_col']  # Missing dataset, has extra
        
        result = postgres_writer_utils._ensure_required_columns(
            mock_dataframe, required_cols, logger=mock_logger
        )
        
        # Should log warnings for missing and info for extra columns
        mock_logger.warning.assert_called()
        mock_logger.info.assert_called()

    def test_ensure_required_columns_entity_types(self, mock_dataframe):
        """Test entity column type casting."""
        required_cols = ['entity', 'organisation_entity']
        
        result = postgres_writer_utils._ensure_required_columns(
            mock_dataframe, required_cols
        )
        
        # Should cast entity columns to bigint
        mock_dataframe.withColumn.assert_called()

    def test_ensure_required_columns_date_types(self, mock_dataframe):
        """Test date column type casting."""
        required_cols = ['entry_date', 'start_date', 'end_date']
        
        result = postgres_writer_utils._ensure_required_columns(
            mock_dataframe, required_cols
        )
        
        # Should cast date columns
        mock_dataframe.withColumn.assert_called()

    def test_ensure_required_columns_json_serialization(self, mock_dataframe):
        """Test JSON column serialization."""
        required_cols = ['json', 'geojson']
        mock_dataframe.columns = ['json', 'geojson']  # Existing columns
        
        result = postgres_writer_utils._ensure_required_columns(
            mock_dataframe, required_cols
        )
        
        # Should serialize JSON columns
        mock_dataframe.withColumn.assert_called()

    def test_ensure_required_columns_string_types(self, mock_dataframe):
        """Test string column type casting."""
        required_cols = ['name', 'dataset', 'prefix', 'reference', 'typology', 'quality']
        mock_dataframe.columns = required_cols  # All exist
        
        result = postgres_writer_utils._ensure_required_columns(
            mock_dataframe, required_cols
        )
        
        # Should cast string columns
        mock_dataframe.withColumn.assert_called()


@pytest.mark.unit
class TestWriteDataframeToPostgresJdbc:
    """Test write_dataframe_to_postgres_jdbc function."""

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.logger')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    def test_write_dataframe_basic_success(self, mock_hashlib, mock_datetime, mock_pg8000, 
                                         mock_logger, mock_show_df, mock_get_secret, 
                                         mock_dataframe, mock_conn_params):
        """Test successful DataFrame write."""
        # Setup mocks
        mock_get_secret.return_value = mock_conn_params
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234efgh5678"
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Execute function
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "development"
        )
        
        # Verify staging table creation
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()
        
        # Verify JDBC write was attempted
        mock_dataframe.repartition.assert_called()

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.logger')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    def test_write_dataframe_staging_table_creation_failure(self, mock_hashlib, mock_datetime, 
                                                          mock_pg8000, mock_logger, mock_show_df, 
                                                          mock_get_secret, mock_dataframe, mock_conn_params):
        """Test staging table creation failure."""
        mock_get_secret.return_value = mock_conn_params
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234"
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Table creation failed")
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Should raise exception
        with pytest.raises(Exception, match="Table creation failed"):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_dataframe, "entity", "test-dataset", "development"
            )

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.logger')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.time')
    def test_write_dataframe_jdbc_retry_logic(self, mock_time, mock_hashlib, mock_datetime, 
                                            mock_pg8000, mock_logger, mock_show_df, 
                                            mock_get_secret, mock_dataframe, mock_conn_params):
        """Test JDBC write retry logic."""
        mock_get_secret.return_value = mock_conn_params
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234"
        
        # Mock staging table creation success
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Mock JDBC write failure then success
        mock_dataframe.repartition.return_value.write.jdbc.side_effect = [
            Exception("JDBC failed"),
            None  # Success on second attempt
        ]
        
        # Execute function
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "development"
        )
        
        # Verify retry was attempted
        assert mock_dataframe.repartition.return_value.write.jdbc.call_count == 2
        mock_time.sleep.assert_called()

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.logger')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.time')
    def test_write_dataframe_atomic_transaction_success(self, mock_time, mock_hashlib, mock_datetime, 
                                                      mock_pg8000, mock_logger, mock_show_df, 
                                                      mock_get_secret, mock_dataframe, mock_conn_params):
        """Test successful atomic transaction."""
        mock_get_secret.return_value = mock_conn_params
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234"
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 100  # Mock affected rows
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Execute function
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "development"
        )
        
        # Verify transaction operations
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        
        # Should have BEGIN, DELETE, INSERT, DROP, COMMIT
        assert any('BEGIN' in call for call in execute_calls)
        assert any('DELETE FROM entity' in call for call in execute_calls)
        assert any('INSERT INTO entity' in call for call in execute_calls)
        assert any('DROP TABLE' in call for call in execute_calls)
        assert any('COMMIT' in call for call in execute_calls)

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.logger')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    def test_write_dataframe_cleanup_staging_table(self, mock_hashlib, mock_datetime, mock_pg8000, 
                                                  mock_logger, mock_show_df, mock_get_secret, 
                                                  mock_dataframe, mock_conn_params):
        """Test cleanup of staging table in finally block."""
        mock_get_secret.return_value = mock_conn_params
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234"
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Execute function
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "development"
        )
        
        # Verify cleanup DROP TABLE IF EXISTS was called
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any('DROP TABLE IF EXISTS' in call for call in execute_calls)

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_write_dataframe_row_count_logging(self, mock_logger, mock_show_df, mock_get_secret, 
                                             mock_dataframe, mock_conn_params):
        """Test row count logging."""
        mock_get_secret.return_value = mock_conn_params
        mock_dataframe.count.return_value = 5000
        
        with patch('jobs.utils.postgres_writer_utils.pg8000'), \
             patch('jobs.utils.postgres_writer_utils.datetime'), \
             patch('jobs.utils.postgres_writer_utils.hashlib'):
            
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_dataframe, "entity", "test-dataset", "development"
            )
        
        # Verify row count was logged
        mock_logger.info.assert_called()
        log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any('5,000 rows' in call for call in log_calls)

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_write_dataframe_partition_calculation(self, mock_logger, mock_show_df, mock_get_secret, 
                                                 mock_dataframe, mock_conn_params):
        """Test partition calculation logic."""
        mock_get_secret.return_value = mock_conn_params
        
        # Test different row counts
        test_cases = [
            (1000, 1),      # Small dataset: 1 partition
            (100000, 2),    # Medium dataset: 2 partitions  
            (1000000, 20),  # Large dataset: max 20 partitions
        ]
        
        with patch('jobs.utils.postgres_writer_utils.pg8000'), \
             patch('jobs.utils.postgres_writer_utils.datetime'), \
             patch('jobs.utils.postgres_writer_utils.hashlib'):
            
            for row_count, expected_partitions in test_cases:
                mock_dataframe.count.return_value = row_count
                
                postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                    mock_dataframe, "entity", "test-dataset", "development"
                )
                
                # Verify repartition was called with expected number
                mock_dataframe.repartition.assert_called_with(expected_partitions)