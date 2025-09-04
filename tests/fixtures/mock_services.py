"""
Mock AWS services and external dependencies for testing.

This module provides comprehensive mocking of AWS services and other external
dependencies to enable complete local testing without actual AWS resources.
"""
import pytest
import boto3
import json
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
try:
    from moto import mock_s3, mock_secretsmanager
except ImportError:
    # For newer versions of moto
    from moto import mock_aws as mock_s3
    from moto import mock_aws as mock_secretsmanager
import responses


class MockAWSServices:
    """Mock AWS services for testing."""
    
    def __init__(self):
        self.s3_mock = None
        self.secrets_mock = None
        self.s3_client = None
        self.secrets_client = None
    
    def start_s3_mock(self):
        """Start S3 mock service."""
        self.s3_mock = mock_s3()
        self.s3_mock.start()
        
        # Create S3 client and setup test buckets
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Create test buckets
        test_buckets = [
            'development-collection-data',
            'development-pyspark-assemble-parquet',
            'test-bucket'
        ]
        
        for bucket in test_buckets:
            self.s3_client.create_bucket(Bucket=bucket)
        
        return self.s3_client
    
    def start_secrets_mock(self):
        """Start Secrets Manager mock service."""
        self.secrets_mock = mock_secretsmanager()
        self.secrets_mock.start()
        
        # Create secrets client and setup test secrets
        self.secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
        
        # Create test database secret
        db_secret = {
            "username": "test_user",
            "password": "test_password",
            "host": "localhost",
            "port": 5432,
            "dbname": "test_pyspark_jobs",
            "engine": "postgres"
        }
        
        self.secrets_client.create_secret(
            Name='rds-db-credentials/test-database',
            SecretString=json.dumps(db_secret)
        )
        
        return self.secrets_client
    
    def upload_test_data_to_s3(self, test_data_dir):
        """Upload test data files to mocked S3."""
        if not self.s3_client:
            raise RuntimeError("S3 mock not started")
        
        # Upload sample CSV files for different scenarios
        sample_datasets = ['transport-access-node', 'sample-transport-access-node']
        
        for dataset in sample_datasets:
            # Upload fact/entity/fact_res data
            for data_type in ['transformed', 'issue']:
                if data_type == 'transformed':
                    file_content = self._generate_sample_csv_content(dataset)
                    key = f"{dataset}-collection/{data_type}/{dataset}/{dataset}.csv"
                else:
                    file_content = self._generate_sample_issue_csv_content(dataset)
                    key = f"{dataset}-collection/{data_type}/{dataset}/{dataset}.csv"
                
                self.s3_client.put_object(
                    Bucket='development-collection-data',
                    Key=key,
                    Body=file_content
                )
    
    def _generate_sample_csv_content(self, dataset):
        """Generate sample CSV content for testing."""
        headers = ["entry_number", "entity", "field", "value", "entry_date", "start_date", "end_date", "organisation"]
        
        rows = []
        for i in range(5):
            rows.append([
                str(i + 1000),
                f"{dataset}:{i:06d}",
                "geometry",
                f"POINT({-0.1 + i*0.01} {51.5 + i*0.01})",
                "2024-01-01",
                "2024-01-01",
                "2024-12-31",
                "development:test-organisation"
            ])
            rows.append([
                str(i + 1000),
                f"{dataset}:{i:06d}",
                "name",
                f"Test Node {i:03d}",
                "2024-01-01",
                "2024-01-01",
                "2024-12-31",
                "development:test-organisation"
            ])
        
        csv_content = ",".join(headers) + "\n"
        for row in rows:
            csv_content += ",".join([f'"{val}"' if "," in str(val) or " " in str(val) else str(val) for val in row]) + "\n"
        
        return csv_content
    
    def _generate_sample_issue_csv_content(self, dataset):
        """Generate sample issue CSV content for testing."""
        headers = ["issue", "resource", "lineNumber", "entry_number", "field", "issue_type", "value", "message"]
        
        rows = []
        for i in range(3):
            rows.append([
                f"issue_{i:03d}",
                f"resource_{i:03d}",
                str(i + 1),
                str(i + 1000),
                "test_field",
                "validation_error",
                f"invalid_value_{i}",
                f"Test issue message {i}"
            ])
        
        csv_content = ",".join(headers) + "\n"
        for row in rows:
            csv_content += ",".join([f'"{val}"' if "," in str(val) or " " in str(val) else str(val) for val in row]) + "\n"
        
        return csv_content
    
    def stop_all_mocks(self):
        """Stop all mock services."""
        if self.s3_mock:
            self.s3_mock.stop()
        if self.secrets_mock:
            self.secrets_mock.stop()


@pytest.fixture(scope="session")
def mock_aws_services():
    """Provide mock AWS services for the session."""
    mock_services = MockAWSServices()
    yield mock_services
    mock_services.stop_all_mocks()


@pytest.fixture
def mock_s3(mock_aws_services):
    """Provide a mocked S3 service."""
    s3_client = mock_aws_services.start_s3_mock()
    mock_aws_services.upload_test_data_to_s3(None)
    yield s3_client
    

@pytest.fixture
def mock_secrets_manager(mock_aws_services):
    """Provide a mocked Secrets Manager service."""
    secrets_client = mock_aws_services.start_secrets_mock()
    yield secrets_client


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection for testing."""
    with patch('jobs.dbaccess.postgres_connectivity.pg8000.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock successful operations
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = None
        
        yield {
            'connection': mock_conn,
            'cursor': mock_cursor,
            'connect_mock': mock_connect
        }


@pytest.fixture
def mock_spark_write_operations():
    """Mock Spark DataFrame write operations."""
    with patch('pyspark.sql.DataFrameWriter.parquet') as mock_parquet, \
         patch('pyspark.sql.DataFrameWriter.jdbc') as mock_jdbc, \
         patch('pyspark.sql.DataFrameWriter.mode') as mock_mode, \
         patch('pyspark.sql.DataFrameWriter.option') as mock_option, \
         patch('pyspark.sql.DataFrameWriter.partitionBy') as mock_partition:
        
        # Chain method calls for fluent interface
        mock_writer = MagicMock()
        mock_mode.return_value = mock_writer
        mock_option.return_value = mock_writer
        mock_partition.return_value = mock_writer
        mock_writer.parquet.return_value = None
        mock_writer.jdbc.return_value = None
        
        yield {
            'parquet': mock_parquet,
            'jdbc': mock_jdbc,
            'mode': mock_mode,
            'option': mock_option,
            'partition': mock_partition,
            'writer': mock_writer
        }


@pytest.fixture
def mock_configuration_files(tmp_path):
    """Create mock configuration files for testing."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    
    # Create transformed_source.json
    transformed_source_config = {
        "schema_fact_res_fact_entity": [
            "entry_number", "entity", "field", "value", "entry_date", 
            "start_date", "end_date", "fact", "priority", "resource", "reference_entity"
        ],
        "schema_issue": [
            "issue", "resource", "lineNumber", "entry_number", "field", 
            "issue_type", "value", "message"
        ]
    }
    
    config_file = config_dir / "transformed_source.json"
    config_file.write_text(json.dumps(transformed_source_config, indent=2))
    
    return {
        "config_dir": str(config_dir),
        "transformed_source": str(config_file)
    }


@pytest.fixture
def mock_environment_variables(monkeypatch):
    """Set up mock environment variables for testing."""
    env_vars = {
        "AWS_ACCESS_KEY_ID": "test_access_key",
        "AWS_SECRET_ACCESS_KEY": "test_secret_key",
        "AWS_DEFAULT_REGION": "us-east-1",
        "LOG_LEVEL": "INFO",
        "ENVIRONMENT": "test",
        "TEST_MODE": "true"
    }
    
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    return env_vars


@pytest.fixture
def mock_datetime():
    """Mock datetime for consistent testing."""
    fixed_datetime = "2024-01-15 10:30:00"
    
    with patch('jobs.main_collection_data.datetime') as mock_dt:
        mock_dt.now.return_value.strftime.return_value = fixed_datetime
        mock_dt.now.return_value = mock_dt.now.return_value
        yield mock_dt


class MockSparkSession:
    """Mock Spark session for unit testing without full Spark."""
    
    def __init__(self):
        self.stopped = False
        self.read = MockDataFrameReader()
        self.conf = MockSparkConf()
        self.sparkContext = MockSparkContext()
    
    def stop(self):
        """Mock stop method."""
        self.stopped = True
    
    def createDataFrame(self, data, schema=None):
        """Mock createDataFrame method."""
        return MockDataFrame(data, schema)


class MockDataFrameReader:
    """Mock DataFrame reader."""
    
    def option(self, key, value):
        return self
    
    def csv(self, path):
        return MockDataFrame([], None)


class MockDataFrame:
    """Mock DataFrame for unit testing."""
    
    def __init__(self, data, schema=None):
        self.data = data
        self.schema = schema
        self._columns = []
        if data and isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                self._columns = list(data[0].keys())
    
    @property
    def columns(self):
        return self._columns
    
    def count(self):
        return len(self.data) if self.data else 0
    
    def show(self, n=20):
        pass
    
    def printSchema(self):
        pass
    
    def cache(self):
        return self
    
    def withColumn(self, col_name, col_expr):
        return self
    
    def withColumnRenamed(self, old_name, new_name):
        return self
    
    def select(self, *cols):
        return self
    
    def filter(self, condition):
        return self
    
    def drop(self, *cols):
        return self
    
    def coalesce(self, num_partitions):
        return self
    
    @property
    def write(self):
        return MockDataFrameWriter()
    
    @property
    def rdd(self):
        return MockRDD()


class MockRDD:
    """Mock RDD."""
    
    def isEmpty(self):
        return False


class MockDataFrameWriter:
    """Mock DataFrame writer."""
    
    def partitionBy(self, *cols):
        return self
    
    def mode(self, mode):
        return self
    
    def option(self, key, value):
        return self
    
    def parquet(self, path):
        pass
    
    def jdbc(self, url, table, **kwargs):
        pass


class MockSparkConf:
    """Mock Spark configuration."""
    
    def set(self, key, value):
        return self


class MockSparkContext:
    """Mock Spark context."""
    
    def setLogLevel(self, level):
        pass


@pytest.fixture
def mock_spark_session_minimal():
    """Provide a minimal mock Spark session for unit tests."""
    return MockSparkSession()
