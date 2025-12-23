"""
Targeted tests for missing lines in postgres_writer_utils.py
Focus on lines: 83-256 (write_dataframe_to_postgres_jdbc function)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import hashlib
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

try:
    from jobs.utils import postgres_writer_utils
except ImportError:
    # Mock PySpark if not available
    with patch.dict('sys.modules', {
        'pyspark': Mock(),
        'pyspark.sql': Mock(),
        'pyspark.sql.functions': Mock(),
        'pyspark.sql.types': Mock(),
        'pg8000': Mock()
    }):
        from jobs.utils import postgres_writer_utils


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame."""
    df = Mock()
    df.columns = ['entity', 'name', 'dataset', 'entry_date']
    df.count.return_value = 1000
    df.withColumn.return_value = df
    df.select.return_value = df
    df.repartition.return_value.write.jdbc.return_value = None
    return df


@pytest.fixture
def mock_connection():
    """Create mock database connection."""
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value = cursor
    cursor.rowcount = 100
    return conn, cursor


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
class TestWriteDataframeToPostgresJdbcMissingLines:
    """Test missing lines 83-256 in write_dataframe_to_postgres_jdbc function."""

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_staging_table_creation_success(self, mock_logger, mock_hashlib, mock_datetime, 
                                          mock_pg8000, mock_ensure_cols, mock_show_df, 
                                          mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 83-110: Successful staging table creation."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify staging table creation SQL is executed (lines 88-110)
        create_calls = [call for call in cursor.execute.call_args_list if 'CREATE TABLE' in str(call)]
        assert len(create_calls) > 0
        conn.commit.assert_called()

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_staging_table_creation_failure(self, mock_logger, mock_hashlib, mock_datetime, 
                                          mock_pg8000, mock_ensure_cols, mock_show_df, 
                                          mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 111-113: Staging table creation failure handling."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Mock staging table creation failure
        cursor.execute.side_effect = Exception("Table creation failed")
        
        with pytest.raises(Exception, match="Table creation failed"):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_dataframe, "entity", "test-dataset", "dev"
            )
        
        # Verify error logging (lines 112-113)
        mock_logger.error.assert_called_with("Failed to create staging table: Table creation failed")

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_dataframe_preparation_and_typing(self, mock_logger, mock_hashlib, mock_datetime, 
                                            mock_pg8000, mock_ensure_cols, mock_show_df, 
                                            mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 115-125: DataFrame preparation and type casting."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify DataFrame preparation (lines 115-125)
        required_cols = [
            "entity", "name", "entry_date", "start_date", "end_date", "dataset",
            "json", "organisation_entity", "prefix", "reference", "typology",
            "geojson", "geometry", "point", "quality"
        ]
        mock_ensure_cols.assert_called_with(mock_dataframe, required_cols, defaults=None, logger=mock_logger)
        mock_dataframe.select.assert_called_with(*required_cols)

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_jdbc_write_partition_calculation(self, mock_logger, mock_hashlib, mock_datetime, 
                                            mock_pg8000, mock_ensure_cols, mock_show_df, 
                                            mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 127-129: JDBC write partition calculation."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Test different row counts for partition calculation
        test_cases = [
            (1000, 1),      # Small dataset: 1 partition
            (100000, 2),    # Medium dataset: 2 partitions  
            (1000000, 20),  # Large dataset: 20 partitions (max)
        ]
        
        for row_count, expected_partitions in test_cases:
            mock_dataframe.count.return_value = row_count
            
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_dataframe, "entity", "test-dataset", "dev"
            )
            
            # Verify partition calculation (lines 127-129)
            mock_dataframe.repartition.assert_called_with(expected_partitions)

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.time.sleep')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_jdbc_write_retry_logic(self, mock_logger, mock_sleep, mock_hashlib, mock_datetime, 
                                  mock_pg8000, mock_ensure_cols, mock_show_df, 
                                  mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 131-155: JDBC write retry logic."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Mock JDBC write failure then success
        mock_dataframe.repartition.return_value.write.jdbc.side_effect = [
            Exception("Connection timeout"),
            None  # Success on second attempt
        ]
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify retry logic (lines 131-155)
        mock_logger.warning.assert_called()
        mock_sleep.assert_called_with(5)  # First retry delay
        mock_logger.info.assert_called_with("Retrying JDBC write in 5s...")

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.time.sleep')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_jdbc_write_max_retries_exceeded(self, mock_logger, mock_sleep, mock_hashlib, mock_datetime, 
                                           mock_pg8000, mock_ensure_cols, mock_show_df, 
                                           mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 150-155: JDBC write max retries exceeded."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Mock JDBC write always fails
        mock_dataframe.repartition.return_value.write.jdbc.side_effect = Exception("Persistent error")
        
        with pytest.raises(Exception, match="Persistent error"):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_dataframe, "entity", "test-dataset", "dev"
            )
        
        # Verify max retries error (lines 154-155)
        mock_logger.error.assert_called_with("Max retries reached for JDBC write. Failing job.")

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_atomic_transaction_delete_and_insert(self, mock_logger, mock_hashlib, mock_datetime, 
                                                mock_pg8000, mock_ensure_cols, mock_show_df, 
                                                mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 157-195: Atomic transaction delete and insert."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Mock successful transaction
        cursor.rowcount = 500  # Deleted rows
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify atomic transaction steps (lines 157-195)
        # Check for BEGIN, DELETE, INSERT, DROP TABLE, COMMIT
        execute_calls = [str(call) for call in cursor.execute.call_args_list]
        
        begin_calls = [call for call in execute_calls if 'BEGIN' in call]
        delete_calls = [call for call in execute_calls if 'DELETE FROM entity' in call]
        insert_calls = [call for call in execute_calls if 'INSERT INTO entity' in call]
        drop_calls = [call for call in execute_calls if 'DROP TABLE' in call]
        commit_calls = [call for call in execute_calls if 'COMMIT' in call]
        
        assert len(begin_calls) > 0
        assert len(delete_calls) > 0
        assert len(insert_calls) > 0
        assert len(drop_calls) > 0
        assert len(commit_calls) > 0

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.time.sleep')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_atomic_transaction_retry_logic(self, mock_logger, mock_sleep, mock_hashlib, mock_datetime, 
                                          mock_pg8000, mock_ensure_cols, mock_show_df, 
                                          mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 196-220: Atomic transaction retry logic."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Mock transaction failure then success
        call_count = 0
        def execute_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if 'DELETE FROM entity' in str(args[0]) and call_count <= 3:
                raise Exception("Transaction failed")
            return None
        
        cursor.execute.side_effect = execute_side_effect
        cursor.rowcount = 100
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify transaction retry logic (lines 196-220)
        mock_logger.warning.assert_called()
        mock_sleep.assert_called()

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.time.sleep')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_atomic_transaction_max_retries_exceeded(self, mock_logger, mock_sleep, mock_hashlib, mock_datetime, 
                                                   mock_pg8000, mock_ensure_cols, mock_show_df, 
                                                   mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 215-220: Atomic transaction max retries exceeded."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Mock transaction always fails
        def execute_side_effect(*args, **kwargs):
            if 'DELETE FROM entity' in str(args[0]):
                raise Exception("Persistent transaction error")
            return None
        
        cursor.execute.side_effect = execute_side_effect
        
        with pytest.raises(Exception, match="Persistent transaction error"):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_dataframe, "entity", "test-dataset", "dev"
            )
        
        # Verify max retries error (lines 219-220)
        mock_logger.error.assert_called_with("Max retries reached for atomic commit. Failing job.")

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_rollback_on_transaction_failure(self, mock_logger, mock_hashlib, mock_datetime, 
                                           mock_pg8000, mock_ensure_cols, mock_show_df, 
                                           mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 221-227: Rollback on transaction failure."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        # Mock transaction failure
        def execute_side_effect(*args, **kwargs):
            if 'DELETE FROM entity' in str(args[0]):
                raise Exception("Transaction failed")
            return None
        
        cursor.execute.side_effect = execute_side_effect
        
        with pytest.raises(Exception):
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_dataframe, "entity", "test-dataset", "dev"
            )
        
        # Verify ROLLBACK is called (lines 221-227)
        rollback_calls = [call for call in cursor.execute.call_args_list if 'ROLLBACK' in str(call)]
        assert len(rollback_calls) > 0

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_finally_block_connection_cleanup(self, mock_logger, mock_hashlib, mock_datetime, 
                                            mock_pg8000, mock_ensure_cols, mock_show_df, 
                                            mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 228-232: Finally block connection cleanup."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify connection cleanup in finally block (lines 228-232)
        cursor.close.assert_called()
        conn.close.assert_called()

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_safety_cleanup_staging_table(self, mock_logger, mock_hashlib, mock_datetime, 
                                         mock_pg8000, mock_ensure_cols, mock_show_df, 
                                         mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 234-256: Safety cleanup of staging table."""
        conn, cursor = mock_connection
        mock_pg8000.return_value = conn
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify safety cleanup (lines 234-256)
        drop_if_exists_calls = [call for call in cursor.execute.call_args_list 
                               if 'DROP TABLE IF EXISTS' in str(call)]
        assert len(drop_if_exists_calls) > 0
        mock_logger.info.assert_called_with("write_dataframe_to_postgres_jdbc: Finally drop staging table if it still exists")

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('jobs.utils.postgres_writer_utils.show_df')
    @patch('jobs.utils.postgres_writer_utils._ensure_required_columns')
    @patch('pg8000.connect')
    @patch('jobs.utils.postgres_writer_utils.datetime')
    @patch('jobs.utils.postgres_writer_utils.hashlib')
    @patch('jobs.utils.postgres_writer_utils.logger')
    def test_safety_cleanup_failure(self, mock_logger, mock_hashlib, mock_datetime, 
                                  mock_pg8000, mock_ensure_cols, mock_show_df, 
                                  mock_get_secret, mock_dataframe, mock_connection):
        """Test lines 256: Safety cleanup failure handling."""
        conn, cursor = mock_connection
        
        # Mock different connections for different stages
        def connect_side_effect(*args, **kwargs):
            if hasattr(connect_side_effect, 'call_count'):
                connect_side_effect.call_count += 1
            else:
                connect_side_effect.call_count = 1
            
            if connect_side_effect.call_count <= 2:  # First two calls succeed
                return conn
            else:  # Third call (safety cleanup) fails
                raise Exception("Cleanup connection failed")
        
        mock_pg8000.side_effect = connect_side_effect
        mock_get_secret.return_value = {'host': 'localhost', 'port': 5432, 'database': 'testdb', 'user': 'user', 'password': 'pass'}
        mock_ensure_cols.return_value = mock_dataframe
        mock_datetime.now.return_value.strftime.return_value = "20231201_120000"
        mock_hashlib.md5.return_value.hexdigest.return_value = "abcd1234abcd1234"
        
        postgres_writer_utils.write_dataframe_to_postgres_jdbc(
            mock_dataframe, "entity", "test-dataset", "dev"
        )
        
        # Verify cleanup failure is handled gracefully (line 256)
        mock_logger.warning.assert_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])