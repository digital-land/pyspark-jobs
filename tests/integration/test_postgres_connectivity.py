import pytest
import json
from unittest.mock import patch, MagicMock
from src.jobs.dbaccess.postgres_connectivity import get_aws_secret, create_table, write_to_postgres

class TestPostgresConnectivity:

    @patch('postgres_connectivity_2.get_secret_emr_compatible')
    def test_get_aws_secret(self, mock_get_secret):
        mock_get_secret.return_value = json.dumps({
            "username": "postgres",
            "password": "postgres",
            "dbName": "postgres",
            "host": "localhost",
            "port": "5432"
        })

        conn_params = get_aws_secret()
        assert conn_params["user"] == "postgres"
        assert conn_params["host"] == "localhost"
        assert conn_params["port"] == 5432
        assert conn_params["database"] == "postgres"

    @patch('pg8000.connect')
    def test_create_table_with_delete(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        conn_params = {
            "database": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres"
        }

        create_table(conn_params, dataset_value="planning-applications")
        mock_cursor.execute.assert_any_call(
            f"DELETE FROM pyspark_entity WHERE dataset = %s;",
            ("planning-applications",)
        )
        mock_conn.commit.assert_called()

    @patch('pg8000.connect')
    @patch('postgres_connectivity._prepare_geometry_columns')
    def test_write_to_postgres_optimized(self, mock_prepare_geom, mock_connect):
        mock_df = MagicMock()
        mock_df.count.return_value = 50000
        mock_prepare_geom.return_value = mock_df

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        conn_params = {
            "database": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "postgres"
        }

        mock_df.write = MagicMock()
        mock_df.write.jdbc = MagicMock()

        write_to_postgres(mock_df, "planning-applications", conn_params, method="optimized")
        mock_df.write.jdbc.assert_called()
