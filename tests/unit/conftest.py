"""
Unit test specific configuration and fixtures.
"""

import sys
from unittest.mock import MagicMock, Mock, patch

import pytest


@pytest.fixture(autouse=True)
def mock_sedona_dependencies():
    """Mock Sedona and geometry dependencies for unit tests."""
    # Mock sedona modules
    sedona_mock = MagicMock()
    sedona_spark_mock = MagicMock()
    sedona_context_mock = MagicMock()

    with patch.dict(
        "sys.modules",
        {
            "sedona": sedona_mock,
            "sedona.spark": sedona_spark_mock,
            "sedona.spark.SedonaContext": sedona_context_mock,
            "pandas": MagicMock(),
        },
    ):
        yield


@pytest.fixture(autouse=True)
def mock_geometry_utils():
    """Mock geometry utilities that depend on Sedona."""
    with patch("jobs.utils.geometry_utils.calculate_centroid") as mock_centroid:
        mock_centroid.return_value = "POINT(0 0)"
        yield mock_centroid


@pytest.fixture
def mock_dataset_typology():
    """Mock dataset typology function."""
    with patch("jobs.transform_collection_data.get_dataset_typology") as mock_func:
        mock_func.return_value = "test - typology"
        yield mock_func


@pytest.fixture
def mock_logging_setup():
    """Mock logging setup to avoid file system dependencies."""
    with patch("jobs.utils.logger_config.setup_logging") as mock_setup:
        yield mock_setup


@pytest.fixture
def mock_s3_operations():
    """Mock S3 operations for unit tests."""
    with patch("jobs.utils.s3_utils.upload_to_s3") as mock_upload, patch(
        "jobs.utils.s3_utils.download_from_s3"
    ) as mock_download, patch("jobs.utils.s3_utils.list_s3_objects") as mock_list:

        mock_upload.return_value = True
        mock_download.return_value = True
        mock_list.return_value = []

        yield {"upload": mock_upload, "download": mock_download, "list": mock_list}


@pytest.fixture
def mock_postgres_operations():
    """Mock PostgreSQL operations for unit tests."""
    with patch(
        "jobs.dbaccess.postgres_connectivity.get_postgres_connection"
    ) as mock_conn, patch(
        "jobs.utils.postgres_writer_utils.write_dataframe_to_postgres_jdbc"
    ) as mock_write:

        mock_conn.return_value = MagicMock()
        mock_write.return_value = True

        yield {"connection": mock_conn, "write": mock_write}


@pytest.fixture
def mock_athena_operations():
    """Mock Athena operations for unit tests."""
    with patch(
        "jobs.dbaccess.Athena - connectivity.get_athena_connection"
    ) as mock_conn:
        mock_conn.return_value = MagicMock()
        yield mock_conn


@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing."""
    return [
        ["entity", "field", "value", "entry_date"],
        ["entity1", "name", "Test Entity 1", "2023 - 01 - 01"],
        ["entity1", "type", "test", "2023 - 01 - 01"],
        ["entity2", "name", "Test Entity 2", "2023 - 01 - 02"],
    ]


@pytest.fixture
def sample_parquet_schema():
    """Sample Parquet schema for testing."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
            StructField("date", StringType(), True),
        ]
    )
