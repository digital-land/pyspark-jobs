"""
Acceptance test specific configuration and fixtures.
"""
import pytest
import tempfile
import os
import shutil
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock


@pytest.fixture(scope="module")
def acceptance_spark():
    """
    Create a Spark session for acceptance tests.
    
    Acceptance tests simulate real-world scenarios so this
    uses configuration closer to production.
    """
    spark_session = SparkSession.builder \
        .appName("AcceptanceTestSpark") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark_session.sparkContext.setLogLevel("WARN")
    
    yield spark_session
    
    spark_session.stop()


@pytest.fixture
def test_data_directory():
    """Create a temporary directory structure for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create subdirectories to simulate real data structure
        input_dir = os.path.join(temp_dir, "input")
        output_dir = os.path.join(temp_dir, "output")
        reference_dir = os.path.join(temp_dir, "reference")
        
        os.makedirs(input_dir)
        os.makedirs(output_dir)
        os.makedirs(reference_dir)
        
        yield {
            "base": temp_dir,
            "input": input_dir,
            "output": output_dir,
            "reference": reference_dir
        }


@pytest.fixture
def end_to_end_test_data(acceptance_spark, test_data_directory):
    """
    Create comprehensive test data for end-to-end testing.
    
    This fixture sets up a complete data environment with:
    - Main entity data
    - Reference data (organisations, datasets)
    - Expected output samples
    """
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Main entity data
    entity_schema = StructType([
        StructField("entry-number", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("value", StringType(), True),
        StructField("entry-date", StringType(), True),
        StructField("start-date", StringType(), True),
        StructField("end-date", StringType(), True),
        StructField("organisation", StringType(), True)
    ])
    
    entity_data = [
        ("1", "entity-001", "name", "Test Boundary 1", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("1", "entity-001", "reference", "TB001", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("1", "entity-001", "prefix", "title-boundary", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("1", "entity-001", "boundary-type", "freehold", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("2", "entity-002", "name", "Test Boundary 2", "2025-01-02", "2025-01-02", "", "local-authority:CMD"),
        ("2", "entity-002", "reference", "TB002", "2025-01-02", "2025-01-02", "", "local-authority:CMD"),
        ("2", "entity-002", "prefix", "title-boundary", "2025-01-02", "2025-01-02", "", "local-authority:CMD"),
        ("2", "entity-002", "boundary-type", "leasehold", "2025-01-02", "2025-01-02", "", "local-authority:CMD"),
        ("3", "entity-003", "name", "Test Boundary 3", "2025-01-03", "2025-01-03", "", "local-authority:LBH"),
        ("3", "entity-003", "reference", "TB003", "2025-01-03", "2025-01-03", "", "local-authority:LBH"),
        ("3", "entity-003", "prefix", "title-boundary", "2025-01-03", "2025-01-03", "", "local-authority:LBH"),
        ("3", "entity-003", "boundary-type", "freehold", "2025-01-03", "2025-01-03", "", "local-authority:LBH"),
    ]
    
    entity_df = acceptance_spark.createDataFrame(entity_data, entity_schema)
    entity_path = os.path.join(test_data_directory["input"], "entities")
    entity_df.write.mode("overwrite").option("header", True).csv(entity_path)
    
    # Organisation reference data
    org_schema = StructType([
        StructField("organisation", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("name", StringType(), True)
    ])
    
    org_data = [
        ("local-authority:LBH", "600001", "London Borough of Hackney"),
        ("local-authority:CMD", "600002", "Camden Council"),
    ]
    
    org_df = acceptance_spark.createDataFrame(org_data, org_schema)
    org_path = os.path.join(test_data_directory["reference"], "organisations")
    org_df.write.mode("overwrite").option("header", True).csv(org_path)
    
    # Dataset reference data
    dataset_schema = StructType([
        StructField("dataset", StringType(), True),
        StructField("typology", StringType(), True),
        StructField("name", StringType(), True)
    ])
    
    dataset_data = [
        ("title-boundary", "geography", "Title Boundaries"),
    ]
    
    dataset_df = acceptance_spark.createDataFrame(dataset_data, dataset_schema)
    dataset_path = os.path.join(test_data_directory["reference"], "datasets")
    dataset_df.write.mode("overwrite").option("header", True).csv(dataset_path)
    
    return {
        "entity_path": entity_path,
        "organisation_path": org_path,
        "dataset_path": dataset_path,
        "output_path": test_data_directory["output"]
    }


@pytest.fixture
def mock_external_dependencies():
    """Mock external dependencies for acceptance tests."""
    mocks = {}
    
    # Mock S3 operations
    with patch('boto3.client') as mock_boto:
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3
        mocks['s3'] = mock_s3
        
        # Mock database connections
        with patch('psycopg2.connect') as mock_pg_connect:
            mock_pg_conn = MagicMock()
            mock_pg_cursor = MagicMock()
            mock_pg_conn.cursor.return_value = mock_pg_cursor
            mock_pg_connect.return_value = mock_pg_conn
            mocks['postgres'] = (mock_pg_conn, mock_pg_cursor)
            
            # Mock secrets manager
            with patch('boto3.session.Session') as mock_session:
                mock_secrets_client = MagicMock()
                mock_session.return_value.client.return_value = mock_secrets_client
                mocks['secrets'] = mock_secrets_client
                
                yield mocks


@pytest.fixture
def performance_config():
    """Configuration for performance-related acceptance tests."""
    return {
        "small_dataset_size": 100,
        "medium_dataset_size": 1000,
        "large_dataset_size": 10000,
        "max_processing_time_seconds": 30,
        "memory_threshold_mb": 500
    }


@pytest.fixture
def data_quality_rules():
    """Define data quality rules for acceptance tests."""
    return {
        "required_fields": ["entity", "field", "value"],
        "max_null_percentage": 0.05,  # 5% max nulls allowed
        "entity_id_pattern": r"^entity-\d{3}$",
        "date_format": "%Y-%m-%d",
        "valid_typologies": ["geography", "legislation", "document"]
    }


# Acceptance test helper functions
def validate_end_to_end_output(output_df, expected_schema, quality_rules):
    """Validate that end-to-end output meets expectations."""
    # Check schema
    assert output_df.schema.fieldNames() == expected_schema
    
    # Check data quality
    total_rows = output_df.count()
    assert total_rows > 0, "Output should contain data"
    
    # Check for required fields
    for field in quality_rules["required_fields"]:
        if field in output_df.columns:
            null_count = output_df.filter(output_df[field].isNull()).count()
            null_percentage = null_count / total_rows
            assert null_percentage <= quality_rules["max_null_percentage"], \
                f"Field {field} has too many nulls: {null_percentage:.2%}"


def simulate_production_load(spark, base_data, multiplier=100):
    """Simulate production-scale data load for performance testing."""
    # Replicate the base data multiple times
    large_data = []
    for i in range(multiplier):
        for row in base_data:
            # Modify entity IDs to make them unique
            modified_row = list(row)
            if len(modified_row) > 1:  # Assuming entity is in position 1
                original_entity = modified_row[1]
                modified_row[1] = f"{original_entity}-batch{i:04d}"
            large_data.append(tuple(modified_row))
    
    return large_data


@pytest.fixture
def e2e_validator():
    """Provide end-to-end validation function."""
    return validate_end_to_end_output


@pytest.fixture
def production_load_simulator():
    """Provide production load simulation function."""
    return simulate_production_load


# Test environment setup
@pytest.fixture(autouse=True)
def acceptance_test_environment(monkeypatch):
    """Set up environment for acceptance tests."""
    # Set acceptance test specific environment variables
    monkeypatch.setenv("TEST_ENVIRONMENT", "acceptance")
    monkeypatch.setenv("SPARK_CONF_DIR", "/tmp/spark-conf")
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([
        os.path.join(os.path.dirname(__file__), "..", "..", "src"),
        os.environ.get("PYTHONPATH", "")
    ]))


@pytest.fixture
def workflow_config():
    """Configuration for workflow testing."""
    return {
        "datasets": ["title-boundary", "transport-access-node"],
        "load_types": ["full", "delta"],
        "output_formats": ["parquet", "csv"],
        "partitioning_columns": ["year", "month", "day"],
        "expected_outputs": ["fact", "fact_res", "entity", "issue"]
    }
