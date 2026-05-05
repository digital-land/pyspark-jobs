"""
Integration tests for DatasetSchema and the schema registry.

Uses a real Spark session to verify schema enforcement on DataFrames.
"""

import pytest

from jobs.config.schema import DatasetSchema, FieldSchema, get_schema


class TestDatasetSchema:
    def test_enforce_adds_missing_field_as_null(self, spark):
        """A field present in the schema but absent from the DataFrame is added as null."""
        schema = DatasetSchema(
            name="test",
            fields=[
                FieldSchema(field="a", datatype="string"),
                FieldSchema(field="b", datatype="bigint"),
            ],
        )
        df = spark.createDataFrame([("hello",)], schema=["a"])

        result = schema.enforce(df)
        row = result.collect()[0]

        assert row["a"] == "hello"
        assert row["b"] is None

    def test_enforce_selects_fields_in_schema_order(self, spark):
        """Output columns are in the order defined by the schema."""
        schema = DatasetSchema(
            name="test",
            fields=[
                FieldSchema(field="z", datatype="string"),
                FieldSchema(field="a", datatype="string"),
            ],
        )
        df = spark.createDataFrame([("first", "second")], schema=["a", "z"])

        result = schema.enforce(df)

        assert result.columns == ["z", "a"]

    def test_enforce_drops_fields_not_in_schema(self, spark):
        """Columns present in the DataFrame but not in the schema are excluded."""
        schema = DatasetSchema(
            name="test",
            fields=[FieldSchema(field="a", datatype="string")],
        )
        df = spark.createDataFrame([("hello", "extra")], schema=["a", "b"])

        result = schema.enforce(df)

        assert result.columns == ["a"]
        assert "b" not in result.columns

    def test_enforce_casts_missing_field_to_correct_type(self, spark):
        """A missing bigint field is added with the correct Spark type."""
        from pyspark.sql.types import LongType

        schema = DatasetSchema(
            name="test",
            fields=[FieldSchema(field="entity", datatype="bigint")],
        )
        df = spark.createDataFrame([("unused",)], schema=["other"])

        result = schema.enforce(df)

        assert isinstance(result.schema["entity"].dataType, LongType)


def test_get_schema_returns_registered_schema():
    """get_schema returns the schema registered under a given name."""
    schema = get_schema("issue")
    assert schema.name == "issue"


def test_get_schema_raises_for_unknown_name():
    """get_schema raises KeyError for an unregistered schema name."""
    with pytest.raises(KeyError):
        get_schema("nonexistent-schema")


def test_issue_schema_contains_expected_fields():
    """The issue schema includes all required field IDs."""
    schema = get_schema("issue")
    field_ids = [f.field for f in schema.fields]

    for expected in [
        "entity",
        "dataset",
        "resource",
        "field",
        "value",
        "issue_type",
        "line_number",
        "entry_number",
        "start_date",
        "entry_date",
        "end_date",
        "message",
    ]:
        assert expected in field_ids
