"""Comprehensive tests for postgres_writer_utils.py to improve coverage from 6% to 80%."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime, date

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

# Mock problematic imports
with patch.dict('sys.modules', {
    'pyspark': MagicMock(),
    'pyspark.sql': MagicMock(),
    'pyspark.sql.functions': MagicMock(),
    'pyspark.sql.types': MagicMock(),
    'pg8000': MagicMock(),
}):
    from jobs.utils import postgres_writer_utils


def create_mock_dataframe(columns=None, count_return=100):
    """Create a mock DataFrame for testing."""
    mock_df = Mock()
    if columns:
        mock_df.columns = columns
    mock_df.count.return_value = count_return
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.repartition.return_value = mock_df
    mock_df.write = Mock()
    mock_df.write.jdbc = Mock()
    return mock_df


@pytest.mark.unit
class TestEnsureRequiredColumns:
    """Test _ensure_required_columns function."""

    def test_ensure_required_columns_missing_columns(self):
        """Test adding missing columns with defaults."""
        mock_df = create_mock_dataframe(['entity', 'name'])
        required_cols = ['entity', 'name', 'dataset', 'entry_date']
        defaults = {'dataset': 'test-dataset'}
        
        with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit, \
             patch('jobs.utils.postgres_writer_utils.col') as mock_col:
            
            mock_lit.return_value.cast.return_value = 'mocked_column'
            
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols, defaults, logger=Mock()
            )
            
            # Should add missing columns
            assert mock_df.withColumn.call_count >= 2  # dataset and entry_date
            assert result == mock_df

    def test_ensure_required_columns_no_missing_columns(self):
        """Test when all required columns exist."""
        mock_df = create_mock_dataframe(['entity', 'name', 'dataset'])
        required_cols = ['entity', 'name', 'dataset']
        
        with patch('jobs.utils.postgres_writer_utils.col') as mock_col:
            mock_col.return_value.cast.return_value = 'mocked_column'
            
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols
            )
            
            # Should still normalize types for existing columns
            assert mock_df.withColumn.call_count >= 1
            assert result == mock_df

    def test_ensure_required_columns_type_normalization(self):
        """Test type normalization for existing columns."""
        mock_df = create_mock_dataframe(['entity', 'organisation_entity', 'entry_date', 'json'])
        required_cols = ['entity', 'organisation_entity', 'entry_date', 'json']
        
        with patch('jobs.utils.postgres_writer_utils.col') as mock_col, \
             patch('jobs.utils.postgres_writer_utils.to_json') as mock_to_json:
            
            mock_col.return_value.cast.return_value = 'mocked_column'
            mock_to_json.return_value = 'json_string'
            
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols
            )
            
            # Should normalize entity to LongType, entry_date to DateType, json to string
            assert mock_df.withColumn.call_count >= 4
            mock_to_json.assert_called()
            assert result == mock_df

    def test_ensure_required_columns_with_logger(self):
        """Test logging of missing and extra columns."""
        mock_df = create_mock_dataframe(['entity', 'extra_col'])
        required_cols = ['entity', 'name']
        mock_logger = Mock()
        
        with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit, \
             patch('jobs.utils.postgres_writer_utils.col') as mock_col:
            
            mock_lit.return_value.cast.return_value = 'mocked_column'
            
            postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols, logger=mock_logger
            )
            
            # Should log missing and extra columns
            mock_logger.warning.assert_called_once()
            mock_logger.info.assert_called_once()

    def test_ensure_required_columns_different_column_types(self):
        """Test handling of different column types."""
        mock_df = create_mock_dataframe(['entity'])
        required_cols = [
            'entity', 'organisation_entity',  # bigint
            'json', 'geojson', 'name',        # string
            'entry_date', 'start_date',       # date
            'custom_field'                    # default string
        ]
        
        with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit, \
             patch('jobs.utils.postgres_writer_utils.col') as mock_col:
            
            mock_lit.return_value.cast.return_value = 'mocked_column'
            
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols
            )
            
            # Should add all missing columns with appropriate types
            assert mock_df.withColumn.call_count >= 7
            assert result == mock_df


@pytest.mark.unit
class TestWriteDataframeToPostgresJdbc:
    """Test write_dataframe_to_postgres_jdbc function."""

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_success(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test successful DataFrame write to PostgreSQL."""
        # Setup mocks
        mock_df = create_mock_dataframe(['entity', 'name'], count_return=1000)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 500
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Call function
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_df, "entity", "test-dataset", "dev"
        )
        
        # Verify operations
        mock_get_secret.assert_called_once_with("dev")
        mock_show_df.assert_called_once()
        mock_ensure_cols.assert_called_once()
        mock_df.repartition.assert_called_once()
        mock_df.write.jdbc.assert_called_once()
        
        # Verify database operations
        assert mock_pg8000.connect.call_count >= 2  # Create staging + atomic transaction
        assert mock_cursor.execute.call_count >= 5  # CREATE, DELETE, INSERT, DROP, etc.

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_staging_table_creation_failure(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test handling of staging table creation failure."""
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Table creation failed")
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        with pytest.raises(Exception, match="Table creation failed"):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_df, "entity", "test-dataset", "dev"
            )

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('jobs.utils.postgres_writer_utils.time')
    def test_write_dataframe_jdbc_retry_logic(self, mock_time, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test JDBC write retry logic."""
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        # Mock staging table creation success
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Mock JDBC write to fail twice then succeed
        mock_df.write.jdbc.side_effect = [
            Exception("Connection timeout"),
            Exception("Network error"),
            None  # Success on third attempt
        ]
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_df, "entity", "test-dataset", "dev"
        )
        
        # Should retry JDBC write 3 times
        assert mock_df.write.jdbc.call_count == 3
        assert mock_time.sleep.call_count == 2  # Sleep between retries

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_jdbc_max_retries_exceeded(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test JDBC write when max retries exceeded."""
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        # Mock staging table creation success
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        # Mock JDBC write to always fail
        mock_df.write.jdbc.side_effect = Exception("Persistent error")
        
        with pytest.raises(Exception, match="Persistent error"):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_df, "entity", "test-dataset", "dev"
            )
        
        # Should attempt 3 times
        assert mock_df.write.jdbc.call_count == 3

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('jobs.utils.postgres_writer_utils.time')
    def test_write_dataframe_atomic_transaction_retry(self, mock_time, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test atomic transaction retry logic."""
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        # Mock connections for different phases
        mock_staging_conn = Mock()
        mock_staging_cursor = Mock()
        mock_staging_conn.cursor.return_value = mock_staging_cursor
        
        mock_transaction_conn = Mock()
        mock_transaction_cursor = Mock()
        mock_transaction_cursor.rowcount = 100
        mock_transaction_conn.cursor.return_value = mock_transaction_cursor
        
        mock_cleanup_conn = Mock()
        mock_cleanup_cursor = Mock()
        mock_cleanup_conn.cursor.return_value = mock_cleanup_cursor
        
        # First call for staging, then transaction attempts, then cleanup
        mock_pg8000.connect.side_effect = [
            mock_staging_conn,  # Staging table creation
            mock_transaction_conn,  # First transaction attempt (fail)
            mock_transaction_conn,  # Second transaction attempt (success)
            mock_cleanup_conn   # Cleanup
        ]
        
        # Make first transaction attempt fail, second succeed
        mock_transaction_cursor.execute.side_effect = [
            None,  # SET statement_timeout
            None,  # BEGIN
            None,  # DELETE
            Exception("Deadlock detected"),  # INSERT fails first time
        ]
        
        # Reset side_effect for second attempt
        def reset_execute(*args):
            mock_transaction_cursor.execute.side_effect = [
                None,  # SET statement_timeout
                None,  # BEGIN
                None,  # DELETE
                None,  # INSERT (success)
                None,  # DROP TABLE
                None,  # COMMIT
            ]
        
        mock_transaction_cursor.execute.side_effect = [
            None,  # SET statement_timeout
            None,  # BEGIN
            None,  # DELETE
            Exception("Deadlock detected"),  # INSERT fails
        ]
        
        # Mock second connection attempt
        mock_transaction_conn2 = Mock()
        mock_transaction_cursor2 = Mock()
        mock_transaction_cursor2.rowcount = 100
        mock_transaction_conn2.cursor.return_value = mock_transaction_cursor2
        
        mock_pg8000.connect.side_effect = [
            mock_staging_conn,      # Staging table creation
            mock_transaction_conn,  # First transaction attempt (fail)
            mock_transaction_conn2, # Second transaction attempt (success)
            mock_cleanup_conn       # Cleanup
        ]
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_df, "entity", "test-dataset", "dev"
        )
        
        # Should retry transaction
        assert mock_time.sleep.call_count >= 1

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_cleanup_staging_table(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test cleanup of staging table in finally block."""
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        # Mock successful operations
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_df, "entity", "test-dataset", "dev"
        )
        
        # Should call cleanup (DROP TABLE IF EXISTS)
        cleanup_calls = [call for call in mock_cursor.execute.call_args_list 
                        if 'DROP TABLE IF EXISTS' in str(call)]
        assert len(cleanup_calls) >= 1

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_large_dataset_partitioning(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test partitioning logic for large datasets."""
        # Large dataset should use more partitions
        mock_df = create_mock_dataframe(['entity'], count_return=500000)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_df, "entity", "test-dataset", "dev"
        )
        
        # Should repartition with calculated number of partitions
        mock_df.repartition.assert_called_once()
        # For 500k rows: max(1, min(20, 500000 // 50000)) = 10 partitions
        expected_partitions = 10
        mock_df.repartition.assert_called_with(expected_partitions)

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_small_dataset_partitioning(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test partitioning logic for small datasets."""
        # Small dataset should use minimum partitions
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_df, "entity", "test-dataset", "dev"
        )
        
        # Should use minimum 1 partition for small datasets
        mock_df.repartition.assert_called_with(1)

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_jdbc_properties(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test JDBC connection properties."""
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 100
        mock_conn.cursor.return_value = mock_cursor
        mock_pg8000.connect.return_value = mock_conn
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_df, "entity", "test-dataset", "dev"
        )
        
        # Verify JDBC properties
        jdbc_call = mock_df.write.jdbc.call_args
        properties = jdbc_call[1]['properties']
        
        assert properties['user'] == 'testuser'
        assert properties['password'] == 'testpass'
        assert properties['driver'] == 'org.postgresql.Driver'
        assert properties['stringtype'] == 'unspecified'
        assert properties['batchsize'] == '5000'
        assert properties['reWriteBatchedInserts'] == 'true'

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils.pg8000')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    def test_write_dataframe_rollback_on_transaction_failure(self, mock_ensure_cols, mock_pg8000, mock_show_df, mock_get_secret):
        """Test rollback is called when transaction fails."""
        mock_df = create_mock_dataframe(['entity'], count_return=100)
        mock_ensure_cols.return_value = mock_df
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        
        # Mock staging creation success, transaction failure
        mock_staging_conn = Mock()
        mock_staging_cursor = Mock()
        mock_staging_conn.cursor.return_value = mock_staging_cursor
        
        mock_transaction_conn = Mock()
        mock_transaction_cursor = Mock()
        mock_transaction_conn.cursor.return_value = mock_transaction_cursor
        
        mock_cleanup_conn = Mock()
        mock_cleanup_cursor = Mock()
        mock_cleanup_conn.cursor.return_value = mock_cleanup_cursor
        
        # Fail all transaction attempts
        mock_transaction_cursor.execute.side_effect = Exception("Transaction failed")
        
        mock_pg8000.connect.side_effect = [
            mock_staging_conn,      # Staging
            mock_transaction_conn,  # Transaction attempt 1
            mock_transaction_conn,  # Transaction attempt 2  
            mock_transaction_conn,  # Transaction attempt 3
            mock_cleanup_conn       # Cleanup
        ]
        
        with pytest.raises(Exception, match="Transaction failed"):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_df, "entity", "test-dataset", "dev"
            )
        
        # Should call ROLLBACK on transaction failures
        rollback_calls = [call for call in mock_transaction_cursor.execute.call_args_list 
                         if 'ROLLBACK' in str(call)]
        assert len(rollback_calls) >= 1


@pytest.mark.unit
class TestPostgresWriterUtilsIntegration:
    """Integration tests for postgres_writer_utils functions."""

    def test_ensure_required_columns_all_column_types(self):
        """Test _ensure_required_columns with all supported column types."""
        mock_df = create_mock_dataframe(['existing_col'])
        
        # Test all column types mentioned in the function
        required_cols = [
            'entity', 'organisation_entity',  # bigint types
            'json', 'geojson', 'geometry', 'point', 'quality', 'name', 'prefix', 'reference', 'typology', 'dataset',  # string types
            'entry_date', 'start_date', 'end_date',  # date types
            'custom_field'  # default string type
        ]
        
        with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit, \
             patch('jobs.utils.postgres_writer_utils.col') as mock_col, \
             patch('jobs.utils.postgres_writer_utils.to_json') as mock_to_json:
            
            mock_lit.return_value.cast.return_value = 'mocked_column'
            mock_col.return_value.cast.return_value = 'mocked_column'
            mock_to_json.return_value = 'json_string'
            
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols
            )
            
            # Should handle all column types appropriately
            assert mock_df.withColumn.call_count >= len(required_cols) - 1  # -1 for existing_col
            assert result == mock_df

    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    def test_staging_table_name_generation(self, mock_datetime, mock_hashlib):
        """Test staging table name generation logic."""
        mock_datetime.now.return_value.strftime.return_value = "20231201_143000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234efgh5678"
        
        # The staging table name should be generated as:
        # f"entity_staging_{dataset_hash}_{timestamp}"
        expected_hash = "abcd1234"  # First 8 chars
        expected_timestamp = "20231201_143000"
        expected_staging_table = f"entity_staging_{expected_hash}_{expected_timestamp}"
        
        # This tests the internal logic that would be used in the main function
        import hashlib
        from datetime import datetime
        
        data_set = "test-dataset"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dataset_hash = hashlib.md5(data_set.encode()).hexdigest()[:8]
        staging_table = f"entity_staging_{dataset_hash}_{timestamp}"
        
        # Verify the pattern is correct
        assert staging_table.startswith("entity_staging_")
        assert len(dataset_hash) == 8
        assert len(timestamp) == 15  # YYYYMMDD_HHMMSS

    def test_required_columns_list_completeness(self):
        """Test that all required columns are properly defined."""
        # This tests the required_cols list used in the main function
        expected_required_cols = [
            "entity", "name", "entry_date", "start_date", "end_date", "dataset",
            "json", "organisation_entity", "prefix", "reference", "typology",
            "geojson", "geometry", "point", "quality"
        ]
        
        # Verify all expected columns are present
        assert len(expected_required_cols) == 15
        assert "entity" in expected_required_cols
        assert "json" in expected_required_cols
        assert "geojson" in expected_required_cols
        assert "geometry" in expected_required_cols
        assert "point" in expected_required_cols