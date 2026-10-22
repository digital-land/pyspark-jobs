"""Integration tests for transform_fact."""

from jobs.transform.fact_transformer import transform_fact

COLUMNS = [
    "fact",
    "entity",
    "field",
    "value",
    "priority",
    "entry_date",
    "entry_number",
    "resource",
]


def _build_df(spark, rows):
    return spark.createDataFrame(rows, schema=COLUMNS)


def test_transform_fact_keeps_latest_entry_date_when_priorities_match(spark):
    """A fact supplied by several resources at the same priority keeps the latest entry date."""
    df = _build_df(
        spark,
        [
            ("fact-1", 700000, "name", "I", 1, "2023-09-28", "1", "res-old"),
            ("fact-1", 700000, "name", "I", 1, "2024-10-17", "1", "res-new"),
        ],
    )

    row = transform_fact(df, "ds-a").collect()[0]

    assert row["entry_date"] == "2024-10-17"


def test_transform_fact_prefers_lower_priority_over_later_entry_date(spark):
    """Priority is compared before entry date, so priority 1 wins over a later priority 2 entry."""
    df = _build_df(
        spark,
        [
            ("fact-1", 1, "organisation", "org-a", 1, "2018-06-04", "1", "res-a"),
            ("fact-1", 1, "organisation", "org-a", 2, "2021-11-30", "1", "res-b"),
        ],
    )

    row = transform_fact(df, "ds-a").collect()[0]

    assert row["entry_date"] == "2018-06-04"
    assert row["priority"] == 1


def test_transform_fact_returns_one_row_per_fact(spark):
    """Each fact appears once in the output however many resources supplied it."""
    df = _build_df(
        spark,
        [
            ("fact-1", 1, "name", "A", 1, "2024-01-01", "1", "res-a"),
            ("fact-1", 1, "name", "A", 1, "2024-02-01", "1", "res-b"),
            ("fact-2", 1, "reference", "R", 1, "2024-01-01", "1", "res-a"),
        ],
    )

    result = transform_fact(df, "ds-a")

    assert sorted(r["fact"] for r in result.collect()) == ["fact-1", "fact-2"]


def test_transform_fact_keeps_blank_value(spark):
    """A blank value is what the provider supplied, such as an empty end-date, so it stays an empty string."""
    df = _build_df(
        spark,
        [("fact-1", 700000, "end-date", "", 1, "2024-10-17", "1", "res-a")],
    )

    row = transform_fact(df, "ds-a").collect()[0]

    assert row["value"] == ""
