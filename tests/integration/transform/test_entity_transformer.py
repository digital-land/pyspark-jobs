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
        result = transformer.transform(df, "test-dataset", organisation_df)

        result_row = result.collect()[0]

        assert "point" in result.columns
        assert result_row["point"] == "POINT(-0.1234 51.5678)"
        assert result_row["geometry"] is None
        assert result_row["quality"] == "same"

    def test_transform_dataset_column_set(self, spark, mocker):
        """The output entity DataFrame should have the dataset column
        set to the dataset name passed to transform."""
        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )

        rows = [
            {
                "entity": "2002",
                "field": "name",
                "value": "Place B",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2002",
                "field": "reference",
                "value": "REF-B",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2002",
                "field": "prefix",
                "value": "test",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2002",
                "field": "organisation",
                "value": "local-authority:ABC",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2002",
                "field": "entry-date",
                "value": "2024-03-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "2002",
                "field": "start-date",
                "value": "2024-01-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
        ]

        df = spark.createDataFrame(rows)
        organisation_df = _build_organisation_df(spark)

        transformer = EntityTransformer()
        result = transformer.transform(df, "my-dataset", organisation_df)

        result_row = result.collect()[0]

        assert "dataset" in result.columns
        assert result_row["dataset"] == "my-dataset"
        assert result_row["quality"] == "same"

    def test_transform_quality_same_when_priority_one(self, spark, mocker):
        """priority=1 should produce quality='same'."""
        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )

        rows = [
            {
                "entity": "3001",
                "field": "name",
                "value": "Place C",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "3001",
                "field": "reference",
                "value": "REF-C",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "3001",
                "field": "prefix",
                "value": "test",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "3001",
                "field": "organisation",
                "value": "local-authority:ABC",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "3001",
                "field": "entry-date",
                "value": "2024-03-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
            {
                "entity": "3001",
                "field": "start-date",
                "value": "2024-01-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "1",
            },
        ]

        df = spark.createDataFrame(rows)
        organisation_df = _build_organisation_df(spark)

        result = EntityTransformer().transform(df, "test-dataset", organisation_df)
        result_row = result.collect()[0]

        assert result_row["quality"] == "same"

    def test_transform_quality_authoritative_when_priority_two(self, spark, mocker):
        """priority=2 should produce quality='authoritative'."""
        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )

        rows = [
            {
                "entity": "3002",
                "field": "name",
                "value": "Place D",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "2",
            },
            {
                "entity": "3002",
                "field": "reference",
                "value": "REF-D",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "2",
            },
            {
                "entity": "3002",
                "field": "prefix",
                "value": "test",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "2",
            },
            {
                "entity": "3002",
                "field": "organisation",
                "value": "local-authority:ABC",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "2",
            },
            {
                "entity": "3002",
                "field": "entry-date",
                "value": "2024-03-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "2",
            },
            {
                "entity": "3002",
                "field": "start-date",
                "value": "2024-01-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
                "priority": "2",
            },
        ]

        df = spark.createDataFrame(rows)
        organisation_df = _build_organisation_df(spark)

        result = EntityTransformer().transform(df, "test-dataset", organisation_df)
        result_row = result.collect()[0]

        assert result_row["quality"] == "authoritative"

    def test_transform_quality_blank_when_no_priority(self, spark, mocker):
        """When input has no priority column, quality should be blank."""
        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )

        rows = [
            {
                "entity": "3003",
                "field": "name",
                "value": "Place E",
                "entry_date": "2024-03-01",
                "entry_number": "1",
            },
            {
                "entity": "3003",
                "field": "reference",
                "value": "REF-E",
                "entry_date": "2024-03-01",
                "entry_number": "1",
            },
            {
                "entity": "3003",
                "field": "prefix",
                "value": "test",
                "entry_date": "2024-03-01",
                "entry_number": "1",
            },
            {
                "entity": "3003",
                "field": "organisation",
                "value": "local-authority:ABC",
                "entry_date": "2024-03-01",
                "entry_number": "1",
            },
            {
                "entity": "3003",
                "field": "entry-date",
                "value": "2024-03-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
            },
            {
                "entity": "3003",
                "field": "start-date",
                "value": "2024-01-01",
                "entry_date": "2024-03-01",
                "entry_number": "1",
            },
        ]

        df = spark.createDataFrame(rows)
        organisation_df = _build_organisation_df(spark)

        result = EntityTransformer().transform(df, "test-dataset", organisation_df)
        result_row = result.collect()[0]

        assert result_row["quality"] == ""
