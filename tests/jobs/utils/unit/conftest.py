"""Fixtures for unit tests."""
import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_s3():
    """Mock S3 client."""
    return MagicMock()

@pytest.fixture
def mock_secrets_manager():
    """Mock Secrets Manager client."""
    return MagicMock()

@pytest.fixture
def mock_configuration_files():
    """Mock configuration files."""
    return MagicMock()

@pytest.fixture
def mock_spark_write_operations():
    """Mock Spark write operations."""
    return MagicMock()

@pytest.fixture
def sample_fact_dataframe(spark):
    """Sample fact dataframe."""
    data = [{"fact": "1", "entity": "e1", "field": "f1"}]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_fact_res_dataframe(spark):
    """Sample fact resource dataframe."""
    data = [{"fact": "1", "resource": "r1"}]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_entity_dataframe(spark):
    """Sample entity dataframe."""
    data = [{"entity": "e1", "field": "f1", "value": "v1"}]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_issue_dataframe(spark):
    """Sample issue dataframe."""
    data = [{"issue": "i1", "resource": "r1"}]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_transport_dataframe(spark):
    """Sample transport dataframe."""
    data = [{"id": "1", "name": "test"}]
    return spark.createDataFrame(data)
