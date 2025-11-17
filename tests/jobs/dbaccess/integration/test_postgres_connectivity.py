import pytest
import json
from unittest.mock import patch, MagicMock
from jobs.dbaccess.postgres_connectivity import get_aws_secret, create_table, write_to_postgres

class TestPostgresConnectivity:

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret(self, mock_get_secret):
        mock_get_secret.return_value = json.dumps({
            "username": "postgres",
            "password": "postgres",
            "db_name": "postgres",
            "host": "localhost",
            "port": "5432"
        })

        conn_params = get_aws_secret("development")
        assert conn_params["user"] == "postgres"
        assert conn_params["host"] == "localhost"
        assert conn_params["port"] == 5432
        assert conn_params["database"] == "postgres"

    @patch('jobs.dbaccess.postgres_connectivity.pg8000.connect')
    def test_create_table_with_delete(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock fetchone to return different values: first 100 records, then 0 after delete
        mock_cursor.fetchone.side_effect = [(0,), (100,), (0,)]  # No stuck deletes, 100 records, then 0 after delete

        conn_params = {
            "database": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres"
        }

        create_table(conn_params, dataset_value="transport-access-node")
        
        # Verify that execute was called (don't check exact SQL as it may vary)
        assert mock_cursor.execute.called
        assert mock_conn.commit.called
        
        # Check that DELETE was called with the dataset value
        delete_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'DELETE' in str(call)]
        assert len(delete_calls) > 0, "DELETE statement should have been executed"

    @patch('jobs.dbaccess.postgres_connectivity.pg8000.connect')
    @patch('jobs.dbaccess.postgres_connectivity._prepare_geometry_columns')
    def test_write_to_postgres_optimized(self, mock_prepare_geom, mock_connect):
        # Create the original DataFrame mock
        mock_df = MagicMock()
        mock_df.count.return_value = 50000
        mock_df.rdd.getNumPartitions.return_value = 8
        mock_df.repartition.return_value = mock_df
        
        # Create the processed DataFrame mock that _prepare_geometry_columns returns
        mock_processed_df = MagicMock()
        mock_processed_df.count.return_value = 50000
        mock_prepare_geom.return_value = mock_processed_df

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock fetchone to return proper values for count queries
        mock_cursor.fetchone.return_value = (0,)  # Simulate 0 existing records

        conn_params = {
            "database": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres"
        }

        # Mock the write operation on the processed DataFrame
        mock_processed_df.write = MagicMock()
        mock_processed_df.write.mode.return_value = mock_processed_df.write
        mock_processed_df.write.option.return_value = mock_processed_df.write
        mock_processed_df.write.jdbc = MagicMock()

        write_to_postgres(mock_df, "transport-access-node", conn_params, method="optimized")
        
        # Verify that _prepare_geometry_columns was called with the original DataFrame
        mock_prepare_geom.assert_called_once_with(mock_df)
        
        # Verify that JDBC write was called on the processed DataFrame
        mock_processed_df.write.jdbc.assert_called_once()
