"""
Integration tests for transform_log_to_tasks and transform_issues_to_tasks.

Uses a real Spark session. Tests the full transformer functions with real
DataFrames including filtering, grouping, and reference hash generation.
For tests of the underlying UDF functions without Spark, see:
tests/unit/transform/test_task_transformer.py
"""

import json

from jobs.transform.task_transformer import (
    transform_issues_to_tasks,
    transform_log_to_tasks,
)


def _build_df(spark, rows, columns):
    return spark.createDataFrame(rows, schema=columns)


class TestTransformLogToTasks:

    def test_filters_out_200_rows_keeps_failures(self, spark):
        df = _build_df(
            spark,
            [
                ("endpoint-aaa", "resource-aaa", "200", "", "dataset-a"),
                ("endpoint-bbb", "resource-bbb", "404", "", "dataset-a"),
                (
                    "endpoint-ccc",
                    "resource-ccc",
                    "500",
                    "Connection refused",
                    "dataset-a",
                ),
            ],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        result = transform_log_to_tasks(df)
        assert result is not None
        rows = result.collect()
        assert len(rows) == 2

    def test_returns_none_when_all_rows_are_200(self, spark):
        df = _build_df(
            spark,
            [("endpoint-aaa", "resource-aaa", "200", "", "dataset-a")],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        assert transform_log_to_tasks(df) is None

    def test_output_has_correct_columns(self, spark):
        df = _build_df(
            spark,
            [("endpoint-aaa", "resource-aaa", "404", "", "dataset-a")],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        result = transform_log_to_tasks(df)
        expected = {
            "dataset",
            "organisation",
            "endpoint",
            "resource",
            "details",
            "severity",
            "responsibility",
            "task_source",
            "entry_date",
            "reference",
        }
        assert set(result.columns) == expected

    def test_task_source_is_log(self, spark):
        df = _build_df(
            spark,
            [("endpoint-aaa", "resource-aaa", "404", "", "dataset-a")],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        result = transform_log_to_tasks(df)
        rows = result.collect()
        assert all(row["task_source"] == "log" for row in rows)

    def test_severity_and_responsibility_are_fixed(self, spark):
        df = _build_df(
            spark,
            [("endpoint-aaa", "resource-aaa", "500", "", "dataset-a")],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        result = transform_log_to_tasks(df)
        row = result.collect()[0]
        assert row["severity"] == "error"
        assert row["responsibility"] == "external"

    def test_details_json_is_valid_and_contains_status(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "500",
                    "Connection refused",
                    "dataset-a",
                )
            ],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        result = transform_log_to_tasks(df)
        details = json.loads(result.collect()[0]["details"])
        assert details["status"] == 500
        assert details["exception"] == "Connection refused"

    def test_reference_is_16_chars(self, spark):
        df = _build_df(
            spark,
            [("endpoint-aaa", "resource-aaa", "404", "", "dataset-a")],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        result = transform_log_to_tasks(df)
        assert len(result.collect()[0]["reference"]) == 16

    def test_reference_is_stable_for_same_input(self, spark):
        df = _build_df(
            spark,
            [("endpoint-aaa", "resource-aaa", "404", "", "dataset-a")],
            ["endpoint", "resource", "status", "exception", "dataset"],
        )
        ref1 = transform_log_to_tasks(df).collect()[0]["reference"]
        ref2 = transform_log_to_tasks(df).collect()[0]["reference"]
        assert ref1 == ref2


class TestTransformIssuesToTasks:

    def test_filters_out_warning_and_internal_rows(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "name",
                    "missing-value",
                    "warning",
                    "internal",
                ),
            ],
            [
                "dataset",
                "resource",
                "field",
                "issue_type",
                "severity",
                "responsibility",
            ],
        )
        result = transform_issues_to_tasks(df)
        assert result is not None
        assert result.count() == 1

    def test_returns_none_when_no_matching_rows(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "dataset-a",
                    "resource-aaa",
                    "name",
                    "missing-value",
                    "warning",
                    "internal",
                )
            ],
            [
                "dataset",
                "resource",
                "field",
                "issue_type",
                "severity",
                "responsibility",
            ],
        )
        assert transform_issues_to_tasks(df) is None

    def test_groups_by_issue_type_and_field_and_counts(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "name",
                    "missing-value",
                    "error",
                    "external",
                ),
            ],
            [
                "dataset",
                "resource",
                "field",
                "issue_type",
                "severity",
                "responsibility",
            ],
        )
        result = transform_issues_to_tasks(df)
        assert result.count() == 2
        rows = result.collect()
        geom_row = next(
            r
            for r in rows
            if json.loads(r["details"])["issue_type"] == "invalid-geometry"
        )
        assert json.loads(geom_row["details"])["count"] == 2

    def test_output_has_correct_columns(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                )
            ],
            [
                "dataset",
                "resource",
                "field",
                "issue_type",
                "severity",
                "responsibility",
            ],
        )
        result = transform_issues_to_tasks(df)
        expected = {
            "dataset",
            "organisation",
            "endpoint",
            "resource",
            "details",
            "severity",
            "responsibility",
            "task_source",
            "entry_date",
            "reference",
        }
        assert set(result.columns) == expected

    def test_task_source_is_issue(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                )
            ],
            [
                "dataset",
                "resource",
                "field",
                "issue_type",
                "severity",
                "responsibility",
            ],
        )
        result = transform_issues_to_tasks(df)
        assert all(row["task_source"] == "issue" for row in result.collect())

    def test_details_json_has_correct_structure(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                )
            ],
            [
                "dataset",
                "resource",
                "field",
                "issue_type",
                "severity",
                "responsibility",
            ],
        )
        result = transform_issues_to_tasks(df)
        details = json.loads(result.collect()[0]["details"])
        assert details["issue_type"] == "invalid-geometry"
        assert details["field"] == "geometry"
        assert isinstance(details["count"], int)

    def test_reference_is_16_chars(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                )
            ],
            [
                "dataset",
                "resource",
                "field",
                "issue_type",
                "severity",
                "responsibility",
            ],
        )
        result = transform_issues_to_tasks(df)
        assert len(result.collect()[0]["reference"]) == 16
