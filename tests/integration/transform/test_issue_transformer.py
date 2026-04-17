"""
Integration tests for IssueTransformer.

Uses a real Spark session to verify schema enforcement, null resource
filtering, and timestamp addition.
"""

from jobs.transform.issue_transformer import IssueTransformer


def _build_df(spark, rows, columns):
    return spark.createDataFrame(rows, schema=columns)


class TestIssueTransformer:
    def test_transform_adds_missing_fields_as_null(self, spark):
        """Fields absent from the source data are added as typed nulls."""
        df = _build_df(
            spark,
            [("ds-a", "res-001", "field-a", "some value")],
            ["dataset", "resource", "field", "value"],
        )

        result = IssueTransformer.transform(df, "ds-a")

        assert "entity" in result.columns
        assert "message" in result.columns
        row = result.collect()[0]
        assert row["entity"] is None
        assert row["message"] is None

    def test_transform_removes_null_resource_rows(self, spark):
        """Rows with a null resource column are removed."""
        df = _build_df(
            spark,
            [
                ("ds-a", "res-001", "field-a", "value"),
                ("ds-a", None, "field-b", "value"),
            ],
            ["dataset", "resource", "field", "value"],
        )

        result = IssueTransformer.transform(df, "ds-a")
        resources = [row["resource"] for row in result.collect()]

        assert "res-001" in resources
        assert None not in resources

    def test_transform_selects_schema_fields_in_order(self, spark):
        """Output columns match the issue schema field order, plus processed_timestamp."""
        from jobs.config.schema import get_schema

        df = _build_df(
            spark,
            [("ds-a", "res-001", "field-a", "value")],
            ["dataset", "resource", "field", "value"],
        )

        result = IssueTransformer.transform(df, "ds-a")
        expected = [f.field for f in get_schema("issue").fields] + [
            "processed_timestamp"
        ]

        assert result.columns == expected

    def test_transform_preserves_existing_field_values(self, spark):
        """Existing field values are not overwritten by schema enforcement."""
        df = _build_df(
            spark,
            [("ds-a", "res-001", "field-a", "my-value", "42")],
            ["dataset", "resource", "field", "value", "line_number"],
        )

        result = IssueTransformer.transform(df, "ds-a")
        row = result.collect()[0]

        assert row["value"] == "my-value"
        assert row["line_number"] == "42"
