"""
Pytest configuration for acceptance tests.

Provides fixtures and configuration for acceptance testing.
"""
import pytest
import os
import tempfile
from unittest.mock import Mock

@pytest.fixture(scope="session")
def acceptance_test_config():
    """Configuration for acceptance tests."""
    return {
        'TEST_S3_BUCKET': os.getenv('TEST_S3_BUCKET', 'test-acceptance-bucket'),
        'TEST_DB_HOST': os.getenv('TEST_DB_HOST', 'localhost'),
        'TEST_DB_PORT': int(os.getenv('TEST_DB_PORT', '5432')),
        'TEST_EMR_APPLICATION_ID': os.getenv('TEST_EMR_APPLICATION_ID', 'test-app-123'),
        'AWS_REGION': os.getenv('AWS_REGION', 'us-east-1')
    }

@pytest.fixture
def temp_workspace():
    """Create temporary workspace for acceptance tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield {
            'workspace': temp_dir,
            'input_dir': os.path.join(temp_dir, 'input'),
            'output_dir': os.path.join(temp_dir, 'output'),
            'config_dir': os.path.join(temp_dir, 'config')
        }

@pytest.fixture
def mock_spark_session():
    """Mock Spark session for acceptance tests."""
    mock_spark = Mock()
    mock_spark.read.option.return_value.csv.return_value = Mock()
    mock_spark.read.parquet.return_value = Mock()
    return mock_spark

@pytest.fixture
def sample_test_data():
    """Sample test data for acceptance tests."""
    return {
        'entity_data': [
            {'entity': 'entity-001', 'name': 'Test Entity 1', 'dataset': 'test-dataset'},
            {'entity': 'entity-002', 'name': 'Test Entity 2', 'dataset': 'test-dataset'}
        ],
        'expected_output_count': 2,
        'expected_columns': ['entity', 'name', 'dataset', 'json', 'geometry']
    }