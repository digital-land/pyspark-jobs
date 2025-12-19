"""Sample data fixtures for testing."""

import pytest
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField,
                               StructType)


@pytest.fixture
def sample_fact_data(spark):
    """Sample fact data for testing."""
    schema = StructType(
        [
            StructField("fact", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("reference_entity", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
        ]
    )

    data = [
        (
            "fact1",
            "2023-12-31",
            "entity1",
            "field1",
            "2023-01-01",
            1,
            "ref1",
            "2023-01-01",
            "value1",
            "1",
        ),
        (
            "fact1",
            "2023-12-31",
            "entity1",
            "field1",
            "2023-01-02",
            2,
            "ref1",
            "2023-01-01",
            "value2",
            "2",
        ),
        (
            "fact2",
            "2023-12-31",
            "entity2",
            "field2",
            "2023-01-01",
            1,
            "ref2",
            "2023-01-01",
            "value3",
            "3",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_entity_data(spark):
    """Sample entity data for testing."""
    schema = StructType(
        [
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("priority", IntegerType(), True),
        ]
    )

    data = [
        ("entity1", "name", "Test Entity 1", "1", "2023-01-01", "2023-01-01", "", 1),
        ("entity1", "reference", "REF001", "2", "2023-01-01", "2023-01-01", "", 1),
        ("entity2", "name", "Test Entity 2", "3", "2023-01-01", "2023-01-01", "", 1),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_issue_data(spark):
    """Sample issue data for testing."""
    schema = StructType(
        [
            StructField("entity", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("field", StringType(), True),
            StructField("issue_type", StringType(), True),
            StructField("line_number", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("resource", StringType(), True),
            StructField("value", StringType(), True),
            StructField("message", StringType(), True),
        ]
    )

    data = [
        (
            "entity1",
            "1",
            "field1",
            "validation",
            "1",
            "test-dataset",
            "resource1",
            "invalid_value",
            "Invalid value",
        ),
        (
            "entity2",
            "2",
            "field2",
            "format",
            "2",
            "test-dataset",
            "resource2",
            "bad_format",
            "Format error",
        ),
    ]

    return spark.createDataFrame(data, schema)
