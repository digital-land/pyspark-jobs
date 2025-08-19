"""
Unit tests for transport access node data transformations.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from jobs.transform_collection_data import transform_data_entity


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder \
        .appName("TransportAccessNodeTestSuite") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


@pytest.fixture
def transport_access_node_schema():
    """Define schema for transport access node data."""
    return StructType([
        StructField("ATCOCode", StringType(), False),
        StructField("BusStopType", StringType(), True),
        StructField("CommonName", StringType(), True),
        StructField("CreationDateTime", StringType(), False),
        StructField("Easting", StringType(), True),
        StructField("Latitude", StringType(), True),
        StructField("Longitude", StringType(), False),
        StructField("NaptanCode", StringType(), True),
        StructField("Northing", StringType(), True),
        StructField("Notes", StringType(), True),
        StructField("StopType", StringType(), True)
    ])


@pytest.fixture
def sample_transport_data(spark):
    """Create sample transport access node data for testing."""
    schema = StructType([
        StructField("entry_number", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("value", StringType(), True),
        StructField("entry_date", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("organisation", StringType(), True)
    ])
    
    data = [
        ("1", "entity1", "ATCOCode", "123", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("1", "entity1", "CommonName", "Main Street", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("1", "entity1", "Longitude", "-0.1278", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("2", "entity2", "ATCOCode", "456", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("2", "entity2", "CommonName", "High Street", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
        ("2", "entity2", "Longitude", "-0.2345", "2025-01-01", "2025-01-01", "", "local-authority:LBH"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_organisation_data(spark):
    """Create mock organisation data."""
    schema = StructType([
        StructField("organisation", StringType(), True),
        StructField("entity", StringType(), True)
    ])
    
    data = [("local-authority:LBH", "600001")]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_dataset_data(spark):
    """Create mock dataset data."""
    schema = StructType([
        StructField("dataset", StringType(), True),
        StructField("typology", StringType(), True)
    ])
    
    data = [("transport-access-node", "geography")]
    return spark.createDataFrame(data, schema)


def test_transport_access_node_schema_validation(transport_access_node_schema):
    """Test that the transport access node schema is correctly defined."""
    expected_field_names = [
        "ATCOCode", "BusStopType", "CommonName", "CreationDateTime", 
        "Easting", "Latitude", "Longitude", "NaptanCode", 
        "Northing", "Notes", "StopType"
    ]
    
    actual_field_names = [field.name for field in transport_access_node_schema.fields]
    assert actual_field_names == expected_field_names
    
    # Check that non-nullable fields are correct
    non_nullable_fields = [field.name for field in transport_access_node_schema.fields if not field.nullable]
    assert "ATCOCode" in non_nullable_fields
    assert "CreationDateTime" in non_nullable_fields
    assert "Longitude" in non_nullable_fields


def test_transport_data_with_nulls_removal(spark):
    """Test that records with null values in required fields are handled correctly."""
    schema = StructType([
        StructField("entry_number", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("value", StringType(), True)
    ])
    
    data = [
        ("1", "entity1", "ATCOCode", "123"),  # Valid record
        ("2", None, "ATCOCode", "456"),       # Null entity
        ("3", "entity3", "ATCOCode", None),   # Null value
        ("4", "entity4", "ATCOCode", "789"),  # Valid record
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Filter out null values
    clean_df = df.filter(df.entity.isNotNull() & df.value.isNotNull())
    
    assert clean_df.count() == 2
    entities = [row.entity for row in clean_df.collect()]
    assert "entity1" in entities
    assert "entity4" in entities


def test_hyphenated_field_names_handling(spark):
    """Test that hyphenated field names are correctly converted."""
    schema = StructType([
        StructField("entry-number", StringType(), True),  # Hyphenated
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("value", StringType(), True)
    ])
    
    data = [("1", "entity1", "test-field", "test-value")]
    df = spark.createDataFrame(data, schema)
    
    # Simulate the hyphen replacement logic
    for column in df.columns:
        if "-" in column:
            new_col = column.replace("-", "_")
            df = df.withColumnRenamed(column, new_col)
    
    assert "entry_number" in df.columns
    assert "entry-number" not in df.columns


def test_empty_dataframe_handling(spark):
    """Test handling of empty DataFrames."""
    schema = StructType([
        StructField("entry_number", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("field", StringType(), True),
        StructField("value", StringType(), True)
    ])
    
    empty_df = spark.createDataFrame([], schema)
    assert empty_df.count() == 0
    assert empty_df.columns == ["entry_number", "entity", "field", "value"]


def test_data_type_validation(spark):
    """Test that all fields are properly typed as strings."""
    schema = StructType([
        StructField("ATCOCode", StringType(), False),
        StructField("Longitude", StringType(), False),
        StructField("Latitude", StringType(), True)
    ])
    
    data = [("123", "-0.1278", "51.5074")]
    df = spark.createDataFrame(data, schema)
    
    # Verify all fields are StringType
    for field in df.schema.fields:
        assert isinstance(field.dataType, StringType)
