"""Unit tests for aws_secrets_manager module."""
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))

from jobs.utils.aws_secrets_manager import get_secret_emr_compatible, get_secret

@patch('jobs.utils.aws_secrets_manager.boto3.client')
def test_get_secret_success(mock_boto3):
    """Test get_secret successful retrieval."""
    mock_client = Mock()
    mock_boto3.return_value = mock_client
    mock_client.get_secret_value.return_value = {
        'SecretString': '{"key": "value"}'
    }
    
    result = get_secret("test-secret")
    assert result == '{"key": "value"}'
    mock_client.get_secret_value.assert_called_once_with(SecretId="test-secret")

@patch('jobs.utils.aws_secrets_manager.boto3.client')
def test_get_secret_client_error(mock_boto3):
    """Test get_secret with ClientError."""
    mock_client = Mock()
    mock_boto3.return_value = mock_client
    
    # Mock a generic exception instead of specific ClientError
    mock_client.get_secret_value.side_effect = Exception("Secret not found")
    
    with pytest.raises(Exception):
        get_secret("test-secret")

@patch('jobs.utils.aws_secrets_manager.boto3.client')
def test_get_secret_emr_compatible_success(mock_boto3):
    """Test get_secret_emr_compatible successful retrieval."""
    mock_client = Mock()
    mock_boto3.return_value = mock_client
    mock_client.get_secret_value.return_value = {
        'SecretString': '{"key": "value"}'
    }
    
    result = get_secret_emr_compatible("test-secret")
    assert result == '{"key": "value"}'

@patch('jobs.utils.aws_secrets_manager.os.getenv')
def test_get_secret_emr_compatible_env_fallback(mock_getenv):
    """Test get_secret_emr_compatible environment variable fallback."""
    mock_getenv.side_effect = lambda key, default=None: {
        'AWS_REGION': 'us-east-1',
        'SECRET_TEST_SECRET': '{"key": "value"}'
    }.get(key, default)
    
    result = get_secret_emr_compatible("test-secret")
    assert result == '{"key": "value"}'

def test_secret_name_validation():
    """Test secret name validation."""
    from jobs.utils.aws_secrets_manager import get_secret
    
    with pytest.raises(ValueError):
        get_secret("")
    
    with pytest.raises(ValueError):
        get_secret(None)