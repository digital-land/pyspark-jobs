"""
Integration tests for transform_entity.

These tests run the full transform pipeline on real Spark DataFrames
with only external HTTP calls (typology lookup) mocked.
"""

from jobs.transform.entity_transformer import transform_entity


def _build_organisation_df(spark):
    """Create a minimal organisation reference DataFrame."""
    return spark.createDataFrame(
        [{"organisation": "local-authority:ABC", "entity": "100"}]
    )


def _base_rows(entity, priority=None):
    fields = [
        ("name", "Place A"),
        ("reference", "REF-A"),
        ("prefix", "test"),
        ("organisation", "local-authority:ABC"),
        ("entry-date", "2024-03-01"),
        ("start-date", "2024-01-01"),
    ]
    rows = []
    for field, value in fields:
        row = {
            "entity": entity,
            "field": field,
            "value": value,
            "entry_date": "2024-03-01",
            "entry_number": "1",
        }
        if priority is not None:
            row["priority"] = priority
        rows.append(row)
    return rows


def test_transform_entity_point_preserved_when_geometry_absent(spark, mocker):
    """When the input has a 'point' field but no 'geometry', point is preserved and geometry is null."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    rows = _base_rows("2001", priority="1") + [
        {
            "entity": "2001",
            "field": "point",
            "value": "POINT(-0.1234 51.5678)",
            "entry_date": "2024-03-01",
            "entry_number": "1",
            "priority": "1",
        }
    ]

    df = spark.createDataFrame(rows)
    result = transform_entity(df, "test-dataset", _build_organisation_df(spark))
    row = result.collect()[0]

    assert "point" in result.columns
    assert row["point"] == "POINT(-0.1234 51.5678)"
    assert row["geometry"] is None
    assert row["quality"] == "same"


def test_transform_entity_dataset_column_set(spark, mocker):
    """The dataset column is set to the value passed to transform_entity."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    df = spark.createDataFrame(_base_rows("2002", priority="1"))
    result = transform_entity(df, "my-dataset", _build_organisation_df(spark))
    row = result.collect()[0]

    assert "dataset" in result.columns
    assert row["dataset"] == "my-dataset"
    assert row["quality"] == "same"


def test_transform_entity_quality_same_when_priority_one(spark, mocker):
    """priority=1 produces quality='same'."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    df = spark.createDataFrame(_base_rows("3001", priority="1"))
    result = transform_entity(df, "test-dataset", _build_organisation_df(spark))

    assert result.collect()[0]["quality"] == "same"


def test_transform_entity_quality_authoritative_when_priority_two(spark, mocker):
    """priority=2 produces quality='authoritative'."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    df = spark.createDataFrame(_base_rows("3002", priority="2"))
    result = transform_entity(df, "test-dataset", _build_organisation_df(spark))

    assert result.collect()[0]["quality"] == "authoritative"


def test_transform_entity_quality_blank_when_no_priority(spark, mocker):
    """When the input has no priority column, quality is blank."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    df = spark.createDataFrame(_base_rows("3003", priority=None))
    result = transform_entity(df, "test-dataset", _build_organisation_df(spark))

    assert result.collect()[0]["quality"] == ""
