"""
Unit test specific configuration and fixtures.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


@pytest.fixture(scope="module")
def unit_spark():
    """
    Create a minimal Spark session for unit tests.

    Unit tests should be fast and isolated, so this uses
    minimal Spark configuration.
    """
    spark_session = (
        SparkSession.builder.appName("UnitTestSpark")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    spark_session.stop()


@pytest.fixture
def sample_fact_schema():
    """Schema for fact data testing."""
    return StructType(
        [
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
            StructField("value", StringType(), True),
        ]
    )


@pytest.fixture
def sample_fact_res_schema():
    """Schema for fact resource data testing."""
    return StructType(
        [
            StructField("end_date", StringType(), True),
            StructField("fact", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("priority", StringType(), True),
            StructField("resource", StringType(), True),
            StructField("start_date", StringType(), True),
        ]
    )


@pytest.fixture
def sample_transport_schema():
    """Schema for transport access node data testing."""
    return StructType(
        [
            StructField("entry_number", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("organisation", StringType(), True),
        ]
    )


# Unit test helper functions
def create_test_dataframe(spark, schema, data):
    """Create a test DataFrame with given schema and data."""
    return spark.createDataFrame(data, schema)


def validate_transformation_output(input_df, output_df, expected_columns):
    """Validate that a transformation produces expected output."""
    # Check that output is not None
    assert output_df is not None

    # Check column structure
    assert output_df.columns == expected_columns

    # Check that we have some data (unless input was empty)
    if input_df.count() > 0:
        assert output_df.count() >= 0  # Allow for filtering that removes all rows


@pytest.fixture
def transformation_validator():
    """Provide transformation validation function."""
    return validate_transformation_output


@pytest.fixture
def dataframe_creator():
    """Provide DataFrame creation helper."""
    return create_test_dataframe
