"""Integration tests for flatten_csv helpers that need a real Spark session."""

from jobs.utils.flatten_csv import flatten_json_column


def test_flatten_json_column_keeps_fields_only_some_rows_have(spark):
    """A field missing from the first row is still kept for the rows that have it."""
    df = spark.createDataFrame(
        [
            (1, '{"notes": "first"}'),
            (2, '{"notes": "second", "documentation_url": "https://example.com/2"}'),
            (3, '{"description": "third"}'),
        ],
        "entity int, json string",
    )

    result = flatten_json_column(df)

    assert result.columns == ["entity", "description", "documentation_url", "notes"]
    rows = {row["entity"]: row.asDict() for row in result.collect()}
    assert rows[1] == {
        "entity": 1,
        "description": None,
        "documentation_url": None,
        "notes": "first",
    }
    assert rows[2]["documentation_url"] == "https://example.com/2"
    assert rows[3]["description"] == "third"


def test_flatten_json_column_with_empty_json_adds_no_columns(spark):
    df = spark.createDataFrame([(1, "{}"), (2, None)], "entity int, json string")

    assert flatten_json_column(df).columns == ["entity"]


def test_flatten_json_column_without_json_column_returns_df_unchanged(spark):
    df = spark.createDataFrame([(1, "x")], "entity int, name string")

    assert flatten_json_column(df).columns == ["entity", "name"]
