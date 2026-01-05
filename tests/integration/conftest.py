"""
Integration test specific configuration and fixtures.
"""

import os
import sqlite3
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def integration_spark():
    """
    Create a Spark session optimized for integration tests.

    Integration tests may involve more complex operations
    so this uses slightly more resources.
    """
    spark_session = (
        SparkSession.builder.appName("IntegrationTestSpark")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("WARN")

    yield spark_session

    spark_session.stop()


@pytest.fixture
def temp_directory():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def temp_sqlite_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    yield db_path

    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def sqlite_connection(temp_sqlite_db):
    """Create a SQLite connection for testing."""
    conn = sqlite3.connect(temp_sqlite_db)
    yield conn
    conn.close()


# Removed mock_postgres_connection fixture to allow real database connections in integration tests
# @pytest.fixture
# def mock_postgres_connection():
#     """Mock PostgreSQL connection for testing."""
#     with patch('psycopg2.connect') as mock_connect:
#         mock_conn = MagicMock()
#         mock_cursor = MagicMock()
#         mock_conn.cursor.return_value = mock_cursor
#         mock_connect.return_value = mock_conn
#         yield mock_conn, mock_cursor


@pytest.fixture
def mock_s3_client():
    """Mock AWS S3 client for testing."""
    with patch("boto3.client") as mock_boto_client:
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        yield mock_s3


@pytest.fixture
def sample_parquet_file(integration_spark, temp_directory):
    """Create a sample parquet file for testing."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )

    data = [
        (1, "test1", "value1", "2025 - 01 - 01"),
        (2, "test2", "value2", "2025 - 01 - 02"),
        (3, "test3", "value3", "2025 - 01 - 03"),
    ]

    df = integration_spark.createDataFrame(data, schema)
    parquet_path = os.path.join(temp_directory, "test_data.parquet")
    df.write.mode("overwrite").parquet(parquet_path)

    return parquet_path


@pytest.fixture
def sample_csv_file(integration_spark, temp_directory):
    """Create a sample CSV file for testing."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_date", StringType(), True),
        ]
    )

    data = [
        ("entity1", "name", "Test Entity 1", "2025 - 01 - 01"),
        ("entity1", "type", "test", "2025 - 01 - 01"),
        ("entity2", "name", "Test Entity 2", "2025 - 01 - 02"),
        ("entity2", "type", "test", "2025 - 01 - 02"),
    ]

    df = integration_spark.createDataFrame(data, schema)
    csv_path = os.path.join(temp_directory, "test_data")
    df.write.mode("overwrite").option("header", True).csv(csv_path)

    return csv_path


@pytest.fixture
def database_config():
    """Provide test database configuration."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password",
    }


@pytest.fixture
def s3_config():
    """Provide test S3 configuration."""
    return {
        "endpoint_url": "http://localhost:4566",  # LocalStack
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
        "region_name": "us - east - 1",
    }


# Integration test helper functions
def setup_test_database(connection, table_name, schema):
    """Set up a test database table."""
    cursor = connection.cursor()

    # Create table
    columns = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    cursor.execute(create_sql)
    connection.commit()

    return cursor


def cleanup_test_database(connection, table_name):
    """Clean up test database table."""
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    connection.commit()


@pytest.fixture
def db_setup_cleanup():
    """Provide database setup and cleanup functions."""
    return setup_test_database, cleanup_test_database


# Mock external service responses
@pytest.fixture
def mock_external_service_responses():
    """Provide mock responses for external services."""
    return {
        "organisation_api": [
            {
                "id": "600001",
                "name": "Test Organisation 1",
                "code": "local - authority:TEST1",
            },
            {
                "id": "600002",
                "name": "Test Organisation 2",
                "code": "local - authority:TEST2",
            },
        ],
        "dataset_api": [
            {
                "dataset": "test - dataset",
                "typology": "geography",
                "name": "Test Dataset",
            }
        ],
    }
