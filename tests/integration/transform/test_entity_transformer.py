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


def _build_resource_df(spark, resources=None):
    """Create a resource reference DataFrame.

    Mirrors resource.csv after column normalisation. A null end_date means the
    resource is live and must outrank superseded ones.
    """
    if resources is None:
        resources = [("res-a", "2024-01-01", None)]
    return spark.createDataFrame(
        resources, "resource string, start_date string, end_date string"
    )


def _base_rows(entity, priority=None, entry_date="2024-03-01", resource="res-a"):
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
            "entry_date": entry_date,
            "entry_number": "1",
            "resource": resource,
            "fact": f"fact-{entity}-{field}",
        }
        if priority is not None:
            row["priority"] = priority
        rows.append(row)
    return rows


def _competing_row(
    entity,
    value,
    priority,
    field="name",
    entry_date="2024-03-01",
    entry_number="1",
    resource="res-a",
    fact="fact-competing",
):
    """A second row for the same (entity, field), to contest the base row."""
    return {
        "entity": entity,
        "field": field,
        "value": value,
        "entry_date": entry_date,
        "entry_number": entry_number,
        "priority": priority,
        "resource": resource,
        "fact": fact,
    }


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
            "resource": "res-a",
            "fact": "fact-2001-point",
        }
    ]

    df = spark.createDataFrame(rows)
    result = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), _build_resource_df(spark)
    )
    row = result.collect()[0]

    assert "point" in result.columns
    assert row["point"] == "POINT(-0.1234 51.5678)"
    assert row["geometry"] is None
    assert row["quality"] == "some"


def test_transform_entity_dataset_column_set(spark, mocker):
    """The dataset column is set to the value passed to transform_entity."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    df = spark.createDataFrame(_base_rows("2002", priority="1"))
    result = transform_entity(
        df, "my-dataset", _build_organisation_df(spark), _build_resource_df(spark)
    )
    row = result.collect()[0]

    assert "dataset" in result.columns
    assert row["dataset"] == "my-dataset"
    assert row["quality"] == "some"


def test_transform_entity_quality_some_when_priority_one(spark, mocker):
    """priority=1 produces quality='some'."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    df = spark.createDataFrame(_base_rows("3001", priority="1"))
    result = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), _build_resource_df(spark)
    )

    assert result.collect()[0]["quality"] == "some"


def test_transform_entity_quality_authoritative_when_priority_two(spark, mocker):
    """priority=2 produces quality='authoritative'."""
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    df = spark.createDataFrame(_base_rows("3002", priority="2"))
    result = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), _build_resource_df(spark)
    )

    assert result.collect()[0]["quality"] == "authoritative"


def test_transform_entity_priority_beats_recency(spark, mocker):
    """An authoritative source wins over a more recent non-authoritative one.

    Pins the criterion that priority ranks before entry_date. Under the previous
    ordering entry_date ranked first, so the newer priority-1 value won.
    """
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    rows = _base_rows(
        "4001", priority="1", entry_date="2026-01-01", resource="res-new"
    ) + [
        _competing_row(
            "4001",
            "AUTHORITATIVE",
            priority="2",
            entry_date="2024-01-01",
            resource="res-old",
            fact="fact-4001-name-auth",
        )
    ]

    resources = _build_resource_df(
        spark,
        [("res-new", "2026-01-01", None), ("res-old", "2024-01-01", None)],
    )

    df = spark.createDataFrame(rows)
    result = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), resources
    )

    assert result.collect()[0]["name"] == "AUTHORITATIVE"


def test_transform_entity_live_resource_beats_ended(spark, mocker):
    """With priority and entry_date tied, a live resource wins over a superseded one.

    Pins resource_end_date. A null end-date means the resource has not been
    superseded, so it must sort first rather than last.
    """
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    rows = _base_rows("4002", priority="2", resource="res-ended") + [
        _competing_row(
            "4002",
            "FROM-LIVE",
            priority="2",
            resource="res-live",
            fact="fact-4002-name-live",
        )
    ]

    resources = _build_resource_df(
        spark,
        [("res-ended", "2024-01-01", "2025-01-01"), ("res-live", "2024-01-01", None)],
    )

    df = spark.createDataFrame(rows)
    result = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), resources
    )

    assert result.collect()[0]["name"] == "FROM-LIVE"


def test_transform_entity_later_start_date_wins_between_live_resources(spark, mocker):
    """With two live resources, the one that started later wins.

    Pins resource_start_date. Both resources coalesce to the same end-date
    sentinel, so end-date cannot separate them.
    """
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    rows = _base_rows("4003", priority="2", resource="res-early") + [
        _competing_row(
            "4003",
            "FROM-LATER-START",
            priority="2",
            resource="res-later",
            fact="fact-4003-name-later",
        )
    ]

    resources = _build_resource_df(
        spark,
        [("res-early", "2024-01-01", None), ("res-later", "2026-01-01", None)],
    )

    df = spark.createDataFrame(rows)
    result = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), resources
    )

    assert result.collect()[0]["name"] == "FROM-LATER-START"


def test_transform_entity_entry_number_sorts_as_string(spark, mocker):
    """entry_number is a VARCHAR and sorts lexicographically, matching duckdb.

    '9' beats '712' as text, where numerically it would lose. Pinned because a
    numeric cast here would silently disagree with dataset_parquet.py.
    """
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    rows = _base_rows("4004", priority="2") + [
        _competing_row(
            "4004",
            "ENTRY-NINE",
            priority="2",
            entry_number="9",
            fact="fact-4004-name-nine",
        ),
        _competing_row(
            "4004",
            "ENTRY-SEVEN-TWELVE",
            priority="2",
            entry_number="712",
            fact="fact-4004-name-712",
        ),
    ]

    df = spark.createDataFrame(rows)
    result = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), _build_resource_df(spark)
    )

    assert result.collect()[0]["name"] == "ENTRY-NINE"


def test_transform_entity_is_deterministic_under_repartition(spark, mocker):
    """Identical input produces identical output regardless of partitioning.

    The trailing resource and fact criteria give the window a total order. Without
    them the winner of a full tie depends on how Spark happened to partition.
    """
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    rows = _base_rows("4005", priority="2", resource="res-b") + [
        _competing_row(
            "4005",
            "FROM-RES-A",
            priority="2",
            resource="res-a",
            fact="fact-4005-name-a",
        )
    ]

    resources = _build_resource_df(
        spark,
        [("res-a", "2024-01-01", None), ("res-b", "2024-01-01", None)],
    )

    df = spark.createDataFrame(rows)
    first = transform_entity(
        df, "test-dataset", _build_organisation_df(spark), resources
    ).collect()
    second = transform_entity(
        df.repartition(4), "test-dataset", _build_organisation_df(spark), resources
    ).collect()

    assert first[0]["name"] == "FROM-RES-A"
    assert [r["name"] for r in first] == [r["name"] for r in second]
