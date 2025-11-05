import pytest
from unittest.mock import patch, Mock
from pyspark.sql import SparkSession
import sys

# 
# Mock pg8000 and its connect method
mock_pg8000 = Mock()
mock_pg8000.connect = Mock()
sys.modules['pg8000'] = mock_pg8000


# Import the module under test
import jobs.dbaccess.postgres_connectivity as pg_module
import pg8000

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

@pytest.fixture
def sample_df(spark):
    data = [
        ("dataset1", "2023-01-01", "entity1", "2023-01-02", '{"type":"Feature"}', "POINT(1 1)", '{"key":"value"}', "name1", 123, "POINT(1 1)", "prefix1", "ref1", "2023-01-01", "typology1")
    ]
    # Ensure pg_module.pyspark_entity_columns exists and is a dict
    columns = list(pg_module.pyspark_entity_columns.keys())
    return spark.createDataFrame(data, columns)

def test_prepare_geometry_columns(sample_df):
    processed_df = pg_module._prepare_geometry_columns(sample_df)
    assert "geometry" in processed_df.columns
    assert "point" in processed_df.columns

@patch("jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible")
def test_get_aws_secret(mock_get_secret):
    mock_get_secret.return_value = '''
    {
        "username": "test_user",
        "password": "test_pass",
        "dbName": "test_db",
        "host": "localhost",
        "port": "5432"
    }
    '''
    conn_params = pg_module.get_aws_secret("development")
    assert conn_params["user"] == "test_user"
    assert conn_params["host"] == "localhost"

@patch("jobs.dbaccess.postgres_connectivity.pg8000.connect")
def test_create_table(mock_connect):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    conn_params = {
        "database": "test_db",
        "host": "localhost",
        "port": 5432,
        "user": "test_user",
        "password": "test_pass",
        "timeout": 30
    }

    pg_module.create_table(conn_params, dataset_value="dataset1")
    mock_cursor.execute.assert_called()
    mock_conn.commit.assert_called()

def test_get_performance_recommendations():
    recs = pg_module.get_performance_recommendations(row_count=500000, available_memory_gb=16)
    assert recs["batch_size"] == 3000
    assert recs["num_partitions"] == 4
