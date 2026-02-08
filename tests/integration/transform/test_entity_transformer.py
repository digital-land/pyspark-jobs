"""
Integration tests for EntityTransformer.

These tests run the full transform pipeline on real Spark DataFrames
with only external HTTP calls (typology lookup) mocked.
"""

from jobs.transform.entity_transformer import EntityTransformer


def _build_organisation_df(spark):
    """Create a minimal organisation reference DataFrame."""
    return spark.createDataFrame(
        [{"organisation": "local-authority:ABC", "entity": "100"}]
    )


class TestEntityTransformer:
    """Integration tests for EntityTransformer."""

    def test_transform_point_preserved_when_geometry_absent(self, spark, mocker):
        """When the transformed input contains a 'point' field but no 'geometry'
        field, the output entity DataFrame should carry the point value through
        and geometry should be null."""
        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )

        rows = [
            {
                "entity": "2001",
                "field": "name",
                "value": "Place A",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2001",
                "field": "reference",
                "value": "REF-A",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2001",
                "field": "prefix",
                "value": "test",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2001",
                "field": "organisation",
                "value": "local-authority:ABC",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2001",
                "field": "entry-date",
                "value": "2024-03-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2001",
                "field": "start-date",
                "value": "2024-01-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2001",
                "field": "point",
                "value": "POINT(-0.1234 51.5678)",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
        ]

        df = spark.createDataFrame(rows)
        organisation_df = _build_organisation_df(spark)

        transformer = EntityTransformer()
        result = transformer.transform(
            df,
            "test-dataset",
            spark,
            organisation_df,
        )

        result_row = result.collect()[0]

        assert "point" in result.columns
        assert result_row["point"] == "POINT(-0.1234 51.5678)"
        assert result_row["geometry"] is None
