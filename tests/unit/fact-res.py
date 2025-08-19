"""
Unit tests for fact resource data transformation functionality.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from jobs.transform_collection_data import transform_data_fact_res


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("FactResTestSuite") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def sample_fact_res_data(spark):
    """Create sample fact resource data for testing."""
    schema = StructType([
        StructField("end_date", StringType(), True),
        StructField("fact", StringType(), True),
        StructField("entry_date", StringType(), True),
        StructField("entry_number", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("resource", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("extra_column", StringType(), True)  # Should be filtered out
    ])
    
    data = [
        ("2025-12-31", "fact1", "2025-01-01", "1", "1", "resource1", "2025-01-01", "extra1"),
        ("2025-12-31", "fact2", "2025-01-02", "2", "2", "resource2", "2025-01-01", "extra2"),
    ]
    
    return spark.createDataFrame(data, schema)


def test_transform_data_fact_res_columns_exist(sample_fact_res_data):
    """Test that transform_data_fact_res returns expected columns."""
    result = transform_data_fact_res(sample_fact_res_data)
    expected_columns = ["end_date", "fact", "entry_date", "entry_number", "priority", "resource", "start_date"]
    
    assert result.columns == expected_columns


def test_transform_data_fact_res_filters_columns(sample_fact_res_data):
    """Test that transform_data_fact_res only includes required columns."""
    result = transform_data_fact_res(sample_fact_res_data)
    
    # Should not include extra_column
    assert "extra_column" not in result.columns
    assert result.count() == 2


def test_transform_data_fact_res_with_hyphenated_columns(spark):
    """Test handling of hyphenated column names."""
    schema = StructType([
        StructField("end-date", StringType(), True),  # Hyphenated
        StructField("fact", StringType(), True),
        StructField("entry-date", StringType(), True),  # Hyphenated
        StructField("entry-number", StringType(), True),  # Hyphenated
        StructField("priority", StringType(), True),
        StructField("resource", StringType(), True),
        StructField("start-date", StringType(), True)  # Hyphenated
    ])
    
    data = [("2025-12-31", "fact1", "2025-01-01", "1", "1", "resource1", "2025-01-01")]
    df = spark.createDataFrame(data, schema)
    
    result = transform_data_fact_res(df)
    
    # Should return the expected columns (underscored)
    expected_columns = ["end_date", "fact", "entry_date", "entry_number", "priority", "resource", "start_date"]
    assert result.columns == expected_columns


def test_transform_data_fact_res_missing_columns(spark):
    """Test transform_data_fact_res with missing columns."""
    schema = StructType([
        StructField("fact", StringType(), True),
        StructField("entry_number", StringType(), True),
        StructField("resource", StringType(), True)
        # Missing other required columns
    ])
    
    data = [("fact1", "1", "resource1")]
    df = spark.createDataFrame(data, schema)
    
    # Should add missing columns with empty strings
    df_with_missing = df.withColumn("end_date", lit("").cast("string")) \
                        .withColumn("entry_date", lit("").cast("string")) \
                        .withColumn("priority", lit("").cast("string")) \
                        .withColumn("start_date", lit("").cast("string"))
    
    result = transform_data_fact_res(df_with_missing)
    expected_columns = ["end_date", "fact", "entry_date", "entry_number", "priority", "resource", "start_date"]
    assert result.columns == expected_columns


def test_transform_data_fact_res_empty_dataframe(spark):
    """Test transform_data_fact_res with empty DataFrame."""
    schema = StructType([
        StructField("end_date", StringType(), True),
        StructField("fact", StringType(), True),
        StructField("entry_date", StringType(), True),
        StructField("entry_number", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("resource", StringType(), True),
        StructField("start_date", StringType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    result = transform_data_fact_res(empty_df)
    
    assert result.count() == 0
    expected_columns = ["end_date", "fact", "entry_date", "entry_number", "priority", "resource", "start_date"]
    assert result.columns == expected_columns


def test_transform_data_fact_res_data_preservation(sample_fact_res_data):
    """Test that transform_data_fact_res preserves data values correctly."""
    result = transform_data_fact_res(sample_fact_res_data)
    
    # Check that data is preserved
    rows = result.collect()
    assert len(rows) == 2
    
    # Check first row values
    first_row = rows[0]
    assert first_row.fact == "fact1"
    assert first_row.entry_number == "1"
    assert first_row.resource == "resource1"
