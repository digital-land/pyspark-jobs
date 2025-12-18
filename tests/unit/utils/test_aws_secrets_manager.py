"""Unit tests for aws_secrets_manager module."""
import pytest
import os
import sys
import json
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError, NoCredentialsError

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from jobs.utils.aws_secrets_manager import (
    SecretsManagerError, get_secret, get_secret_json, 
    get_database_credentials, get_secret_emr_compatible,
    get_secret_with_fallback, get_postgres_secret
)


class TestAWSSecretsManager:
    """Test suite for aws_secrets_manager module."""

    def test_secrets_manager_error_creation(self):
        """Test SecretsManagerError exception creation."""
        error = SecretsManagerError("Test error")
        assert str(error) == "Test error"

    def test_get_secret_empty_name(self):
        """Test get_secret with empty secret name."""
        with pytest.raises(ValueError, match="secret_name cannot be empty or None"):
            get_secret("")
        
        with pytest.raises(ValueError, match="secret_name cannot be empty or None"):
            get_secret(None)

    @patch('boto3.client')
    def test_get_secret_success(self, mock_boto3):
        """Test successful secret retrieval."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'test_secret_value'
        }
        
        result = get_secret("test/secret")
        
        assert result == "test_secret_value"
        mock_boto3.assert_called_once_with('secretsmanager', region_name='eu-west-2')
        mock_client.get_secret_value.assert_called_once_with(SecretId="test/secret")

    @patch('boto3.client')
    def test_get_secret_custom_region(self, mock_boto3):
        """Test secret retrieval with custom region."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'test_secret_value'
        }
        
        result = get_secret("test/secret", region_name="us-west-2")
        
        assert result == "test_secret_value"
        mock_boto3.assert_called_once_with('secretsmanager', region_name='us-west-2')

    @patch('boto3.client')
    def test_get_secret_no_credentials(self, mock_boto3):
        """Test secret retrieval with no AWS credentials."""
        mock_boto3.side_effect = NoCredentialsError()
        
        with pytest.raises(SecretsManagerError, match="AWS credentials not found"):
            get_secret("test/secret")

    @patch('boto3.client')
    def test_get_secret_client_error(self, mock_boto3):
        """Test secret retrieval with AWS client error."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        
        error_response = {
            'Error': {
                'Code': 'ResourceNotFoundException',
                'Message': 'Secret not found'
            }
        }
        mock_client.get_secret_value.side_effect = ClientError(error_response, 'GetSecretValue')
        
        with pytest.raises(SecretsManagerError, match="Failed to retrieve secret: ResourceNotFoundException"):
            get_secret("test/secret")

    @patch('boto3.client')
    def test_get_secret_no_string_value(self, mock_boto3):
        """Test secret retrieval when secret has no string value."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretBinary': b'binary_data'
        }
        
        with pytest.raises(SecretsManagerError, match="Secret does not contain a string value"):
            get_secret("test/secret")

    @patch('jobs.utils.aws_secrets_manager.get_secret')
    def test_get_secret_json_success(self, mock_get_secret):
        """Test successful JSON secret retrieval and parsing."""
        test_data = {"username": "test_user", "password": "test_pass"}
        mock_get_secret.return_value = json.dumps(test_data)
        
        result = get_secret_json("test/secret")
        
        assert result == test_data
        mock_get_secret.assert_called_once_with("test/secret", None)

    @patch('jobs.utils.aws_secrets_manager.get_secret')
    def test_get_secret_json_invalid_json(self, mock_get_secret):
        """Test JSON secret retrieval with invalid JSON."""
        mock_get_secret.return_value = "invalid json"
        
        with pytest.raises(SecretsManagerError, match="Secret is not valid JSON"):
            get_secret_json("test/secret")

    @patch('jobs.utils.aws_secrets_manager.get_secret_json')
    def test_get_database_credentials_success(self, mock_get_secret_json):
        """Test successful database credentials retrieval."""
        test_credentials = {
            "username": "db_user",
            "password": "db_pass",
            "host": "db_host",
            "port": "5432",
            "database": "db_name"
        }
        mock_get_secret_json.return_value = test_credentials
        
        result = get_database_credentials("test/db/secret")
        
        assert result == test_credentials
        mock_get_secret_json.assert_called_once_with("test/db/secret", None)

    @patch('jobs.utils.aws_secrets_manager.get_secret_json')
    def test_get_database_credentials_missing_required_keys(self, mock_get_secret_json):
        """Test database credentials retrieval with missing required keys."""
        incomplete_credentials = {
            "username": "db_user"
            # Missing password, host
        }
        mock_get_secret_json.return_value = incomplete_credentials
        
        with pytest.raises(SecretsManagerError, match="Database secret missing required keys"):
            get_database_credentials("test/db/secret")

    @patch('jobs.utils.aws_secrets_manager.get_secret_json')
    def test_get_database_credentials_default_port(self, mock_get_secret_json):
        """Test database credentials with default port assignment."""
        credentials_without_port = {
            "username": "db_user",
            "password": "db_pass",
            "host": "db_host",
            "engine": "postgres"
        }
        mock_get_secret_json.return_value = credentials_without_port
        
        result = get_database_credentials("test/db/secret")
        
        assert result["port"] == "5432"  # Default PostgreSQL port

    @patch('jobs.utils.aws_secrets_manager.get_secret_json')
    def test_get_database_credentials_mysql_default_port(self, mock_get_secret_json):
        """Test database credentials with MySQL default port."""
        credentials_mysql = {
            "username": "db_user",
            "password": "db_pass",
            "host": "db_host",
            "engine": "mysql"
        }
        mock_get_secret_json.return_value = credentials_mysql
        
        result = get_database_credentials("test/db/secret")
        
        assert result["port"] == "3306"  # Default MySQL port

    @patch('jobs.utils.aws_secrets_manager.get_secret_json')
    def test_get_database_credentials_port_conversion(self, mock_get_secret_json):
        """Test database credentials with integer port conversion."""
        credentials_int_port = {
            "username": "db_user",
            "password": "db_pass",
            "host": "db_host",
            "port": 5432  # Integer port
        }
        mock_get_secret_json.return_value = credentials_int_port
        
        result = get_database_credentials("test/db/secret")
        
        assert result["port"] == "5432"  # Should be converted to string

    def test_get_secret_emr_compatible_empty_name(self):
        """Test get_secret_emr_compatible with empty secret name."""
        with pytest.raises(ValueError, match="secret_name cannot be empty or None"):
            get_secret_emr_compatible("")

    @patch.dict(os.environ, {'SECRET_TEST_SECRET': 'env_secret_value'})
    def test_get_secret_emr_compatible_env_fallback(self):
        """Test EMR compatible secret retrieval with environment variable fallback."""
        result = get_secret_emr_compatible("test/secret")
        
        assert result == "env_secret_value"

    @patch('boto3.client')
    def test_get_secret_emr_compatible_aws_success(self, mock_boto3):
        """Test EMR compatible secret retrieval from AWS."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'aws_secret_value'
        }
        
        result = get_secret_emr_compatible("test/secret")
        
        assert result == "aws_secret_value"

    @patch('boto3.client')
    def test_get_secret_emr_compatible_no_credentials(self, mock_boto3):
        """Test EMR compatible secret retrieval with no credentials."""
        mock_boto3.side_effect = NoCredentialsError()
        
        with pytest.raises(SecretsManagerError, match="AWS credentials not found"):
            get_secret_emr_compatible("test/secret")

    @patch.dict(os.environ, {'TEST_ENV_VAR': 'fallback_value'})
    @patch('jobs.utils.aws_secrets_manager.get_secret')
    def test_get_secret_with_fallback_env_success(self, mock_get_secret):
        """Test secret retrieval with environment variable fallback success."""
        result = get_secret_with_fallback("test/secret", env_var_name="TEST_ENV_VAR")
        
        assert result == "fallback_value"
        # Should not call AWS Secrets Manager
        mock_get_secret.assert_not_called()

    @patch('jobs.utils.aws_secrets_manager.get_secret')
    def test_get_secret_with_fallback_aws_success(self, mock_get_secret):
        """Test secret retrieval with AWS success when no env var."""
        mock_get_secret.return_value = "aws_secret_value"
        
        result = get_secret_with_fallback("test/secret")
        
        assert result == "aws_secret_value"
        mock_get_secret.assert_called_once_with("test/secret", None)

    @patch('jobs.utils.aws_secrets_manager.get_secret')
    def test_get_secret_with_fallback_both_fail(self, mock_get_secret):
        """Test secret retrieval when both AWS and env var fail."""
        mock_get_secret.side_effect = SecretsManagerError("AWS failed")
        
        with pytest.raises(SecretsManagerError, match="Failed to retrieve secret"):
            get_secret_with_fallback("test/secret", env_var_name="NONEXISTENT_VAR")

    def test_get_secret_with_fallback_empty_name(self):
        """Test get_secret_with_fallback with empty secret name."""
        with pytest.raises(ValueError, match="secret_name cannot be empty or None"):
            get_secret_with_fallback("")

    @patch.dict(os.environ, {'POSTGRES_SECRET_NAME': 'test/postgres/secret'})
    @patch('jobs.utils.aws_secrets_manager.get_database_credentials')
    def test_get_postgres_secret_success(self, mock_get_db_creds):
        """Test successful PostgreSQL secret retrieval."""
        test_credentials = {
            "username": "postgres_user",
            "password": "postgres_pass",
            "host": "postgres_host",
            "port": "5432"
        }
        mock_get_db_creds.return_value = test_credentials
        
        result = get_postgres_secret()
        
        assert result == test_credentials
        mock_get_db_creds.assert_called_once_with('test/postgres/secret', None)

    def test_get_postgres_secret_no_env_var(self):
        """Test PostgreSQL secret retrieval without environment variable."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Environment variable POSTGRES_SECRET_NAME must be set"):
                get_postgres_secret()

    @patch('jobs.utils.aws_secrets_manager.get_database_credentials')
    def test_get_postgres_secret_custom_region(self, mock_get_db_creds):
        """Test PostgreSQL secret retrieval with custom region."""
        with patch.dict(os.environ, {'POSTGRES_SECRET_NAME': 'test/postgres/secret'}):
            test_credentials = {"username": "user"}
            mock_get_db_creds.return_value = test_credentials
            
            result = get_postgres_secret(region_name="us-west-1")
            
            assert result == test_credentials
            mock_get_db_creds.assert_called_once_with('test/postgres/secret', 'us-west-1')


@pytest.mark.unit
class TestAWSSecretsManagerIntegration:
    """Integration-style tests for aws_secrets_manager module."""

    @patch('boto3.client')
    def test_complete_database_workflow(self, mock_boto3):
        """Test complete database credentials workflow."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        
        # Mock complete database credentials
        db_secret = {
            "username": "app_user",
            "password": "secure_password",
            "host": "db.example.com",
            "port": 5432,
            "database": "application_db",
            "engine": "postgres"
        }
        
        mock_client.get_secret_value.return_value = {
            'SecretString': json.dumps(db_secret)
        }
        
        # Test the complete workflow
        result = get_database_credentials("prod/app/database")
        
        # Verify all fields are present and correctly formatted
        assert result["username"] == "app_user"
        assert result["password"] == "secure_password"
        assert result["host"] == "db.example.com"
        assert result["port"] == "5432"  # Should be string
        assert result["database"] == "application_db"
        
        # Verify AWS client was called correctly
        mock_boto3.assert_called_once_with('secretsmanager', region_name='eu-west-2')
        mock_client.get_secret_value.assert_called_once_with(SecretId="prod/app/database")

    @patch.dict(os.environ, {'AWS_REGION': 'us-east-1'})
    @patch('boto3.client')
    def test_region_from_environment(self, mock_boto3):
        """Test that region is correctly read from environment variable."""
        mock_client = Mock()
        mock_boto3.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'test_value'
        }
        
        get_secret("test/secret")
        
        mock_boto3.assert_called_once_with('secretsmanager', region_name='us-east-1')

    def test_error_handling_chain(self):
        """Test error handling through the call chain."""
        with patch('boto3.client') as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            
            # Test different error scenarios
            error_scenarios = [
                (NoCredentialsError(), "AWS credentials not found"),
                (ClientError({'Error': {'Code': 'AccessDenied'}}, 'GetSecretValue'), "Failed to retrieve secret: AccessDenied"),
                (Exception("Unexpected error"), "Unexpected error retrieving secret")
            ]
            
            for exception, expected_message in error_scenarios:
                mock_client.get_secret_value.side_effect = exception
                
                with pytest.raises(SecretsManagerError) as exc_info:
                    get_secret("test/secret")
                
                assert expected_message in str(exc_info.value)