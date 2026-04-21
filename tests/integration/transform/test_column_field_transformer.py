"""Integration tests for ColumnFieldTransformer."""

from jobs.config.schema import get_schema
from jobs.transform.column_field_transformer import ColumnFieldTransformer


def _build_df(spark, rows, columns):
    return spark.createDataFrame(rows, schema=columns)


class TestColumnFieldTransformer:
    def test_transform_selects_schema_fields_in_order(self, spark):
        """Output columns match the column_field schema field order, plus processed_timestamp."""
        df = _build_df(
            spark,
            [("res-001", "my-field", "my-column")],
            ["resource", "field", "column"],
        )

        result = ColumnFieldTransformer.transform(df, "ds-a")
        expected = [f.field for f in get_schema("column_field").fields] + [
            "processed_timestamp"
        ]

        assert result.columns == expected

    def test_transform_sets_dataset_column(self, spark):
        """Dataset column is set to the provided dataset value."""
        df = _build_df(
            spark,
            [("res-001", "my-field", "my-column")],
            ["resource", "field", "column"],
        )

        result = ColumnFieldTransformer.transform(df, "my-dataset")
        row = result.collect()[0]

        assert row["dataset"] == "my-dataset"

    def test_transform_adds_missing_fields_as_null(self, spark):
        """Fields absent from the source data are added as typed nulls."""
        df = _build_df(
            spark,
            [("res-001", "my-field", "my-column")],
            ["resource", "field", "column"],
        )

        result = ColumnFieldTransformer.transform(df, "ds-a")
        row = result.collect()[0]

        assert row["entry_date"] is None

    def test_transform_preserves_existing_field_values(self, spark):
        """Existing field values are not overwritten by schema enforcement."""
        df = _build_df(
            spark,
            [("res-001", "my-field", "my-column", "2024-01-01")],
            ["resource", "field", "column", "entry_date"],
        )

        result = ColumnFieldTransformer.transform(df, "ds-a")
        row = result.collect()[0]

        assert row["field"] == "my-field"
        assert row["column"] == "my-column"
        assert row["entry_date"] == "2024-01-01"

    def test_transform_adds_processed_timestamp(self, spark):
        """processed_timestamp column is added to the output."""
        df = _build_df(
            spark,
            [("res-001", "my-field", "my-column")],
            ["resource", "field", "column"],
        )

        result = ColumnFieldTransformer.transform(df, "ds-a")

        assert "processed_timestamp" in result.columns
        assert result.collect()[0]["processed_timestamp"] is not None
