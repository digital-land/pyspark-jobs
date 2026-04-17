"""
Integration tests for transform/filter.py.

Uses a real Spark session to verify filtering behaviour on DataFrames.
"""

from jobs.transform.filter import filter_old_resources


def _build_issue_df(spark, rows):
    return spark.createDataFrame(rows, schema=["resource", "value"])


def _build_old_resources_df(spark, rows):
    return spark.createDataFrame(rows, schema=["resource", "status"])


def test_filter_removes_410_resources(spark):
    """Rows matching a 410-status resource hash are removed."""
    issue_df = _build_issue_df(
        spark, [("abc123", "a"), ("def456", "b"), ("ghi789", "c")]
    )
    old_resources_df = _build_old_resources_df(spark, [("abc123", "410")])

    result = filter_old_resources(issue_df, old_resources_df)
    resources = [row["resource"] for row in result.collect()]

    assert "abc123" not in resources
    assert "def456" in resources
    assert "ghi789" in resources


def test_filter_keeps_non_410_resources(spark):
    """Resources with a status other than 410 are not removed."""
    issue_df = _build_issue_df(spark, [("abc123", "a"), ("def456", "b")])
    old_resources_df = _build_old_resources_df(
        spark, [("abc123", "301"), ("def456", "200")]
    )

    result = filter_old_resources(issue_df, old_resources_df)
    resources = [row["resource"] for row in result.collect()]

    assert "abc123" in resources
    assert "def456" in resources


def test_filter_no_410s_returns_df_unchanged(spark):
    """When no old resources have status 410, the DataFrame is returned unchanged."""
    issue_df = _build_issue_df(spark, [("abc123", "a"), ("def456", "b")])
    old_resources_df = _build_old_resources_df(spark, [("abc123", "301")])

    result = filter_old_resources(issue_df, old_resources_df)

    assert result.count() == issue_df.count()


def test_filter_does_not_remove_null_resources(spark):
    """Null resource rows are not removed by this function — that is the transformer's concern."""
    issue_df = _build_issue_df(spark, [(None, "a"), ("abc123", "b")])
    old_resources_df = _build_old_resources_df(spark, [("abc123", "410")])

    result = filter_old_resources(issue_df, old_resources_df)
    resources = [row["resource"] for row in result.collect()]

    assert None in resources
