"""Mock services for testing external dependencies."""

import json
from unittest.mock import MagicMock, Mock

import pytest


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing."""
    mock_client = Mock()
    mock_client.get_object.return_value = {
        "Body": Mock(read=Mock(return_value=b'{"test": "data"}'))
    }
    mock_client.list_objects_v2.return_value = {"Contents": []}
    mock_client.delete_objects.return_value = {"Deleted": []}
    return mock_client


@pytest.fixture
def mock_secrets_manager():
    """Mock AWS Secrets Manager for testing."""
    mock_client = Mock()
    mock_client.get_secret_value.return_value = {
        "SecretString": json.dumps(
            {
                "username": "test_user",
                "password": "test_password",
                "host": "test_host",
                "port": "5432",
                "db_name": "test_db",
            }
        )
    }
    return mock_client


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection for testing."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (100,)  # Mock row count
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn
