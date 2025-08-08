"""
Unit tests for fact data transformation functionality.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, row_number
from pyspark.sql.types import StructType, StructField, StringType

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from jobs.transform_collection_data import transform_data_fact


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("FactTestSuite") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def sample_fact_data(spark):
    """Create sample fact data for testing."""
    schema = StructType([
        StructField("fact", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("entry_date", StringType(), True),
        StructField("entry_number", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("reference_entity", StringType(), True),
        StructField("resource", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("value", StringType(), True)
    ])
    
    data = [
        ("fact1", "2025-12-31", "entity1", "field1", "2025-01-01", "1", "1", "ref1", "res1", "2025-01-01", "value1"),
        ("fact1", "2025-12-31", "entity1", "field1", "2025-01-02", "2", "2", "ref1", "res1", "2025-01-01", "value2"),
        ("fact2", "2025-12-31", "entity2", "field2", "2025-01-01", "3", "1", "ref2", "res2", "2025-01-01", "value3"),
    ]
    
    return spark.createDataFrame(data, schema)


def test_transform_data_fact_columns_exist(sample_fact_data):
    """Test that transform_data_fact returns expected columns."""
    result = transform_data_fact(sample_fact_data)
    expected_columns = ["end_date", "entity", "fact", "field", "entry_date", "priority", "reference_entity", "start_date", "value"]
    
    assert result.columns == expected_columns


def test_transform_data_fact_deduplication(sample_fact_data):
    """Test that transform_data_fact removes duplicates correctly based on priority."""
    result = transform_data_fact(sample_fact_data)
    
    # Should have 2 rows (one per fact, keeping highest priority)
    assert result.count() == 2
    
    # Check that we kept the records with priority 1 (highest priority)
    fact1_records = result.filter(result.fact == "fact1").collect()
    fact2_records = result.filter(result.fact == "fact2").collect()
    
    assert len(fact1_records) == 1
    assert len(fact2_records) == 1
    assert fact1_records[0].priority == "1"
    assert fact2_records[0].priority == "1"


def test_transform_data_fact_with_hyphenated_columns(spark):
    """Test handling of hyphenated column names."""
    schema = StructType([
        StructField("fact", StringType(), True),
        StructField("end-date", StringType(), True),  # Hyphenated
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("entry-date", StringType(), True),  # Hyphenated
        StructField("entry-number", StringType(), True),  # Hyphenated
        StructField("priority", StringType(), True),
        StructField("reference-entity", StringType(), True),  # Hyphenated
        StructField("resource", StringType(), True),
        StructField("start-date", StringType(), True),  # Hyphenated
        StructField("value", StringType(), True)
    ])
    
    data = [("fact1", "2025-12-31", "entity1", "field1", "2025-01-01", "1", "1", "ref1", "res1", "2025-01-01", "value1")]
    df = spark.createDataFrame(data, schema)
    
    # The function should handle hyphenated columns
    result = transform_data_fact(df)
    
    # Should still return the expected columns
    expected_columns = ["end_date", "entity", "fact", "field", "entry_date", "priority", "reference_entity", "start_date", "value"]
    assert result.columns == expected_columns


def test_transform_data_fact_empty_dataframe(spark):
    """Test transform_data_fact with empty DataFrame."""
    schema = StructType([
        StructField("fact", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("entry_date", StringType(), True),
        StructField("entry_number", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("reference_entity", StringType(), True),
        StructField("resource", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("value", StringType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    result = transform_data_fact(empty_df)
    
    assert result.count() == 0
    expected_columns = ["end_date", "entity", "fact", "field", "entry_date", "priority", "reference_entity", "start_date", "value"]
    assert result.columns == expected_columns
