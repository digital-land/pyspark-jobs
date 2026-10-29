"""
Integration tests for transform/filter.py.

Uses a real Spark session to verify filtering behaviour on DataFrames.
"""

from jobs.transform.filter import filter_dataset_resources, filter_old_resources


def _build_issue_df(spark, rows):
    return spark.createDataFrame(rows, schema=["resource", "value"])


def _build_old_resources_df(spark, rows):
    return spark.createDataFrame(rows, schema=["old_resource", "status"])


def _build_resources_df(spark, rows):
    return spark.createDataFrame(rows, schema=["resource", "datasets"])


def _build_redirects_df(spark, rows):
    return spark.createDataFrame(rows, schema=["old_resource", "status", "resource"])


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


def test_filter_dataset_resources_removes_resources_not_listed_for_dataset(spark):
    """A transformed file left behind for a resource now listed only for another dataset is removed."""
    transformed_df = _build_issue_df(spark, [("abc123", "a"), ("def456", "b")])
    resources_df = _build_resources_df(
        spark, [("abc123", "tree-preservation-order"), ("def456", "tree")]
    )

    result = filter_dataset_resources(
        transformed_df, resources_df, "tree-preservation-order"
    )
    resources = [row["resource"] for row in result.collect()]

    assert resources == ["abc123"]


def test_filter_dataset_resources_keeps_resource_listed_for_several_datasets(spark):
    """A resource whose datasets include this one among others is kept."""
    transformed_df = _build_issue_df(spark, [("abc123", "a")])
    resources_df = _build_resources_df(
        spark, [("abc123", "tree;tree-preservation-order")]
    )

    result = filter_dataset_resources(
        transformed_df, resources_df, "tree-preservation-order"
    )

    assert result.count() == 1


def test_filter_dataset_resources_matches_whole_dataset_names(spark):
    """A dataset whose name contains this dataset's name does not count as a match."""
    transformed_df = _build_issue_df(spark, [("abc123", "a")])
    resources_df = _build_resources_df(spark, [("abc123", "tree-preservation-zone")])

    result = filter_dataset_resources(transformed_df, resources_df, "tree")

    assert result.count() == 0


def test_filter_dataset_resources_removes_resources_missing_from_resource_csv(spark):
    """A transformed file whose resource has no row in resource.csv is removed."""
    transformed_df = _build_issue_df(spark, [("abc123", "a"), ("def456", "b")])
    resources_df = _build_resources_df(spark, [("abc123", "tree")])

    result = filter_dataset_resources(transformed_df, resources_df, "tree")
    resources = [row["resource"] for row in result.collect()]

    assert resources == ["abc123"]


def test_filter_dataset_resources_keeps_replacement_of_redirected_resource(spark):
    """Rows carrying the replacement a listed resource is redirected (301) to are
    kept, though resource.csv lists only the redirected resource."""
    transformed_df = _build_issue_df(spark, [("replacement", "a"), ("def456", "b")])
    resources_df = _build_resources_df(
        spark, [("redirected", "tree"), ("def456", "tree")]
    )
    old_resources_df = _build_redirects_df(
        spark, [("redirected", "301", "replacement")]
    )

    result = filter_dataset_resources(
        transformed_df, resources_df, "tree", old_resources_df
    )
    resources = sorted(row["resource"] for row in result.collect())

    assert resources == ["def456", "replacement"]


def test_filter_dataset_resources_removes_replacement_of_resource_not_listed(spark):
    """A replacement is only kept when the resource redirected to it is listed for
    this dataset."""
    transformed_df = _build_issue_df(spark, [("replacement", "a")])
    resources_df = _build_resources_df(
        spark, [("redirected", "tree-preservation-order")]
    )
    old_resources_df = _build_redirects_df(
        spark, [("redirected", "301", "replacement")]
    )

    result = filter_dataset_resources(
        transformed_df, resources_df, "tree", old_resources_df
    )

    assert result.count() == 0
