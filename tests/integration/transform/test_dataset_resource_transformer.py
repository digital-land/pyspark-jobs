"""Integration tests for DatasetResourceTransformer."""

from jobs.config.schema import get_schema
from jobs.transform.dataset_resource_transformer import DatasetResourceTransformer


def _build_df(spark, rows, columns):
    return spark.createDataFrame(rows, schema=columns)


class TestDatasetResourceTransformer:
    def test_transform_selects_schema_fields_in_order(self, spark):
        """Output columns match the dataset_resource schema field order, plus processed_timestamp."""
        df = _build_df(
            spark,
            [("res-001", "2024-01-01")],
            ["resource", "entry_date"],
        )

        result = DatasetResourceTransformer.transform(df, "ds-a")
        expected = [f.field for f in get_schema("dataset_resource").fields] + [
            "processed_timestamp"
        ]

        assert result.columns == expected

    def test_transform_sets_dataset_column(self, spark):
        """Dataset column is set to the provided dataset value."""
        df = _build_df(spark, [("res-001", "2024-01-01")], ["resource", "entry_date"])

        result = DatasetResourceTransformer.transform(df, "my-dataset")
        row = result.collect()[0]

        assert row["dataset"] == "my-dataset"

    def test_transform_adds_missing_fields_as_null(self, spark):
        """Fields absent from the source data are added as typed nulls."""
        df = _build_df(spark, [("res-001", "2024-01-01")], ["resource", "entry_date"])

        result = DatasetResourceTransformer.transform(df, "ds-a")
        row = result.collect()[0]

        assert row["mime_type"] is None
        assert row["internal_path"] is None
        assert row["entity_count"] is None

    def test_transform_preserves_existing_field_values(self, spark):
        """Existing field values are not overwritten by schema enforcement."""
        df = _build_df(
            spark,
            [("res-001", "2024-01-01", "text/csv", 42)],
            ["resource", "entry_date", "mime_type", "entity_count"],
        )

        result = DatasetResourceTransformer.transform(df, "ds-a")
        row = result.collect()[0]

        assert row["mime_type"] == "text/csv"
        assert row["entity_count"] == 42

    def test_transform_adds_processed_timestamp(self, spark):
        """processed_timestamp column is added to the output."""
        df = _build_df(spark, [("res-001", "2024-01-01")], ["resource", "entry_date"])

        result = DatasetResourceTransformer.transform(df, "ds-a")

        assert "processed_timestamp" in result.columns
        assert result.collect()[0]["processed_timestamp"] is not None
