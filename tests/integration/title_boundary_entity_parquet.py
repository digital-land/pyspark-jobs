
"""
Integration tests for reading Parquet files with PySpark.
"""
import pytest
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("ParquetTestSuite") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def sample_parquet_data(spark):
    """Create sample parquet data for testing."""
    schema = StructType([
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("value", StringType(), True),
        StructField("entry_number", IntegerType(), True),
        StructField("entry_date", StringType(), True)
    ])
    
    data = [
        ("entity1", "name", "Test Entity 1", 1, "2025-01-01"),
        ("entity1", "type", "boundary", 1, "2025-01-01"),
        ("entity2", "name", "Test Entity 2", 2, "2025-01-02"),
        ("entity2", "type", "area", 2, "2025-01-02"),
        ("entity3", "name", "Test Entity 3", 3, "2025-01-03"),
        ("entity3", "type", "point", 3, "2025-01-03"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def temp_parquet_file(sample_parquet_data):
    """Create a temporary parquet file for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, "test_data.parquet")
        sample_parquet_data.write.mode("overwrite").parquet(parquet_path)
        yield parquet_path


def test_read_parquet_file_success(spark, temp_parquet_file):
    """Test successful reading of parquet file."""
    df = spark.read.parquet(temp_parquet_file)
    
    assert df is not None
    assert df.count() == 6
    
    # Check schema
    expected_columns = ["entity", "field", "value", "entry_number", "entry_date"]
    assert df.columns == expected_columns


def test_parquet_file_schema_validation(spark, temp_parquet_file):
    """Test that parquet file has expected schema."""
    df = spark.read.parquet(temp_parquet_file)
    
    schema = df.schema
    field_names = [field.name for field in schema.fields]
    field_types = [type(field.dataType).__name__ for field in schema.fields]
    
    assert "entity" in field_names
    assert "value" in field_names
    assert "StringType" in field_types
    assert "IntegerType" in field_types


def test_parquet_data_content_validation(spark, temp_parquet_file):
    """Test that parquet file contains expected data."""
    df = spark.read.parquet(temp_parquet_file)
    
    # Test specific value selection
    value_df = df.select("value")
    values = [row.value for row in value_df.collect()]
    
    expected_values = ["Test Entity 1", "boundary", "Test Entity 2", "area", "Test Entity 3", "point"]
    assert set(values) == set(expected_values)


def test_parquet_file_filtering(spark, temp_parquet_file):
    """Test filtering data from parquet file."""
    df = spark.read.parquet(temp_parquet_file)
    
    # Filter for specific entity
    entity1_df = df.filter(df.entity == "entity1")
    assert entity1_df.count() == 2
    
    # Filter for specific field type
    name_df = df.filter(df.field == "name")
    assert name_df.count() == 3


def test_parquet_file_aggregation(spark, temp_parquet_file):
    """Test aggregation operations on parquet data."""
    df = spark.read.parquet(temp_parquet_file)
    
    # Count by entity
    entity_counts = df.groupBy("entity").count()
    counts = {row.entity: row.count for row in entity_counts.collect()}
    
    assert counts["entity1"] == 2
    assert counts["entity2"] == 2
    assert counts["entity3"] == 2


def test_read_nonexistent_parquet_file(spark):
    """Test reading non-existent parquet file."""
    non_existent_path = "/path/that/does/not/exist.parquet"
    
    with pytest.raises(Exception):  # PySpark raises AnalysisException
        spark.read.parquet(non_existent_path).collect()


def test_parquet_file_with_complex_data(spark):
    """Test parquet file with complex data types."""
    # Create a more complex schema
    complex_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("coordinates", StringType(), True),  # JSON-like string
        StructField("properties", StringType(), True)    # JSON-like string
    ])
    
    complex_data = [
        (1, "Location 1", '{"lat": 51.5074, "lon": -0.1278}', '{"type": "residential"}'),
        (2, "Location 2", '{"lat": 52.5074, "lon": -1.1278}', '{"type": "commercial"}'),
    ]
    
    df = spark.createDataFrame(complex_data, complex_schema)
    
    # Write and read back
    with tempfile.TemporaryDirectory() as temp_dir:
        parquet_path = os.path.join(temp_dir, "complex_data.parquet")
        df.write.mode("overwrite").parquet(parquet_path)
        
        read_df = spark.read.parquet(parquet_path)
        assert read_df.count() == 2
        assert "coordinates" in read_df.columns
        assert "properties" in read_df.columns


def test_parquet_file_partitioning(spark, sample_parquet_data):
    """Test reading partitioned parquet files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        partitioned_path = os.path.join(temp_dir, "partitioned_data")
        
        # Write partitioned by entry_date
        sample_parquet_data.write.mode("overwrite") \
            .partitionBy("entry_date") \
            .parquet(partitioned_path)
        
        # Read back partitioned data
        read_df = spark.read.parquet(partitioned_path)
        assert read_df.count() == 6
        
        # Check that partitioning column is present
        assert "entry_date" in read_df.columns


def test_parquet_file_compression(spark, sample_parquet_data):
    """Test parquet files with different compression."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test with snappy compression (default)
        snappy_path = os.path.join(temp_dir, "snappy_data.parquet")
        sample_parquet_data.write.mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(snappy_path)
        
        read_df = spark.read.parquet(snappy_path)
        assert read_df.count() == 6


@pytest.mark.integration
def test_large_parquet_file_reading(spark):
    """Test reading larger parquet files (performance test)."""
    # Create larger dataset
    large_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", StringType(), True)
    ])
    
    # Generate 1000 records
    large_data = [(i, f"value_{i}") for i in range(1000)]
    large_df = spark.createDataFrame(large_data, large_schema)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        large_parquet_path = os.path.join(temp_dir, "large_data.parquet")
        large_df.write.mode("overwrite").parquet(large_parquet_path)
        
        # Read and verify
        read_df = spark.read.parquet(large_parquet_path)
        assert read_df.count() == 1000
        
        # Test that we can perform operations efficiently
        max_id = read_df.agg({"id": "max"}).collect()[0][0]
        assert max_id == 999
