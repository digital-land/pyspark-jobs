"""
Shared pytest configuration and fixtures for the entire test suite.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Add src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tests.fixtures.mock_services import *
# Import fixtures from fixtures module
from tests.fixtures.sample_data import *


@pytest.fixture(scope="session")
def spark():
    """
    Create a shared Spark session for all tests.

    This fixture creates a single Spark session that is shared across
    all test modules to improve performance and reduce resource usage.
    """
    spark_session = (
        SparkSession.builder.appName("PySparkTestSuite")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    # Set log level to reduce noise during testing
    spark_session.sparkContext.setLogLevel("WARN")

    yield spark_session

    # Cleanup
    spark_session.stop()


@pytest.fixture
def spark_local():
    """
    Create a local Spark session for tests that need isolation.

    Use this fixture when tests need their own Spark session
    (e.g., for testing configuration changes).
    """
    spark_session = (
        SparkSession.builder.appName("PySparkIsolatedTest")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("WARN")

    yield spark_session

    spark_session.stop()


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "acceptance: mark test as an acceptance test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their location."""
    for item in items:
        # Get the test file path
        test_file = str(item.fspath)

        # Add markers based on directory structure
        if "unit" in test_file:
            item.add_marker(pytest.mark.unit)
        elif "integration" in test_file:
            item.add_marker(pytest.mark.integration)
        elif "acceptance" in test_file:
            item.add_marker(pytest.mark.acceptance)


# Pytest configuration options
pytest_plugins = []


class TestConfig:
    """Test configuration constants."""

    # Database settings for integration tests
    TEST_DB_HOST = "localhost"
    TEST_DB_PORT = 5432
    TEST_DB_NAME = "test_pyspark_jobs"
    TEST_DB_USER = "test_user"
    TEST_DB_PASSWORD = "test_password"

    # S3 settings for integration tests (use localstack or minio)
    TEST_S3_ENDPOINT = "http://localhost:4566"
    TEST_S3_BUCKET = "test-bucket"
    TEST_S3_ACCESS_KEY = "test"
    TEST_S3_SECRET_KEY = "test"

    # Test data paths
    TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

    # Spark configuration for tests
    SPARK_CONF = {
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.adaptive.enabled": "false",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }


@pytest.fixture
def test_config():
    """Provide test configuration constants."""
    return TestConfig


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """
    Automatically set up test environment variables.

    This fixture runs for every test and ensures consistent
    environment configuration.
    """
    # Set test environment variables
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", sys.executable)
    monkeypatch.setenv("SPARK_LOCAL_IP", "127.0.0.1")

    # Mock AWS environment variables for testing
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test_access_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test_secret_key")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # Set test database environment variables
    monkeypatch.setenv("TEST_MODE", "true")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://test_user:test_password@localhost:5432/test_pyspark_jobs",
    )


@pytest.fixture
def sample_csv_schema():
    """Provide a standard CSV schema for testing."""
    from pyspark.sql.types import (IntegerType, StringType, StructField,
                                   StructType)

    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )


@pytest.fixture
def sample_entity_schema():
    """Provide entity schema for testing."""
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
        ]
    )


# Helper functions for tests
def assert_dataframe_equal(df1, df2, check_dtype=True):
    """
    Assert that two DataFrames are equal.

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        check_dtype: Whether to check data types
    """
    # Check row counts
    assert df1.count() == df2.count(), "DataFrames have different row counts"

    # Check schemas
    if check_dtype:
        assert df1.schema == df2.schema, "DataFrames have different schemas"
    else:
        assert len(df1.columns) == len(
            df2.columns
        ), "DataFrames have different column counts"
        assert df1.columns == df2.columns, "DataFrames have different column names"

    # Check data - convert to sorted lists for comparison
    df1_data = sorted([row.asDict() for row in df1.collect()])
    df2_data = sorted([row.asDict() for row in df2.collect()])

    assert df1_data == df2_data, "DataFrames have different data"


@pytest.fixture
def assert_df_equal():
    """Provide the DataFrame equality assertion function."""
    return assert_dataframe_equal
