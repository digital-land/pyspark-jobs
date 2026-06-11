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


LOG_COLUMNS = ["endpoint", "resource", "status", "exception", "dataset", "organisation"]


class TestTransformLogToTasks:

    def test_filters_out_200_rows_keeps_failures(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "200",
                    "",
                    "dataset-a",
                    "organisation-x",
                ),
                (
                    "endpoint-bbb",
                    "resource-bbb",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                ),
                (
                    "endpoint-ccc",
                    "resource-ccc",
                    "500",
                    "Connection refused",
                    "dataset-a",
                    "organisation-x",
                ),
            ],
            LOG_COLUMNS,
        )
        result = transform_log_to_tasks(df)
        assert result is not None
        rows = result.collect()
        assert len(rows) == 2

    def test_returns_none_when_all_rows_are_200(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "200",
                    "",
                    "dataset-a",
                    "organisation-x",
                )
            ],
            LOG_COLUMNS,
        )
        assert transform_log_to_tasks(df) is None

    def test_output_has_correct_columns(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                )
            ],
            LOG_COLUMNS,
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
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                )
            ],
            LOG_COLUMNS,
        )
        result = transform_log_to_tasks(df)
        rows = result.collect()
        assert all(row["task_source"] == "log" for row in rows)

    def test_severity_and_responsibility_are_fixed(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "500",
                    "",
                    "dataset-a",
                    "organisation-x",
                )
            ],
            LOG_COLUMNS,
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
                    "organisation-x",
                )
            ],
            LOG_COLUMNS,
        )
        result = transform_log_to_tasks(df)
        details = json.loads(result.collect()[0]["details"])
        assert details["status"] == 500
        assert details["exception"] == "Connection refused"

    def test_reference_is_16_chars(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                )
            ],
            LOG_COLUMNS,
        )
        result = transform_log_to_tasks(df)
        assert len(result.collect()[0]["reference"]) == 16

    def test_reference_is_stable_for_same_input(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                )
            ],
            LOG_COLUMNS,
        )
        ref1 = transform_log_to_tasks(df).collect()[0]["reference"]
        ref2 = transform_log_to_tasks(df).collect()[0]["reference"]
        assert ref1 == ref2

    def test_same_endpoint_failing_repeatedly_produces_one_task(self, spark):
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                    "2026-01-01",
                    "200",
                    "1.2",
                ),
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                    "2026-01-02",
                    "200",
                    "1.1",
                ),
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                    "2026-01-03",
                    "201",
                    "0.9",
                ),
            ],
            LOG_COLUMNS + ["entry_date", "bytes", "elapsed"],
        )
        result = transform_log_to_tasks(df)
        assert result.count() == 1

    def test_references_are_unique(self, spark):
        """No two log tasks should share a reference."""
        df = _build_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                ),
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "404",
                    "",
                    "dataset-a",
                    "organisation-x",
                ),
                (
                    "endpoint-bbb",
                    "resource-bbb",
                    "500",
                    "Connection refused",
                    "dataset-a",
                    "organisation-x",
                ),
            ],
            LOG_COLUMNS,
        )
        result = transform_log_to_tasks(df)
        references = [row["reference"] for row in result.collect()]
        assert len(references) == len(set(references))


ISSUE_COLUMNS = [
    "dataset",
    "resource",
    "field",
    "issue_type",
    "severity",
    "responsibility",
    "organisation",
    "endpoint",
]


class TestTransformIssuesToTasks:

    def test_includes_internal_responsibility_rows(self, spark):
        """responsibility is no longer filtered — internal issues are now included."""
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
                    "organisation-x",
                    "endpoint-aaa",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "name",
                    "missing-value",
                    "notice",
                    "internal",
                    "organisation-x",
                    "endpoint-aaa",
                ),
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        assert result is not None
        assert result.count() == 2

    def test_excludes_info_severity_rows(self, spark):
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
                    "organisation-x",
                    "endpoint-aaa",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "name",
                    "missing-value",
                    "info",
                    "internal",
                    "organisation-x",
                    "endpoint-aaa",
                ),
            ],
            ISSUE_COLUMNS,
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
                    "info",
                    "internal",
                    "organisation-x",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
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
                    "organisation-x",
                    "endpoint-aaa",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                    "organisation-x",
                    "endpoint-aaa",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "name",
                    "missing-value",
                    "error",
                    "external",
                    "organisation-x",
                    "endpoint-aaa",
                ),
            ],
            ISSUE_COLUMNS,
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
                    "organisation-x",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
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
                    "organisation-x",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
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
                    "organisation-x",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
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
                    "organisation-x",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        assert len(result.collect()[0]["reference"]) == 16

    def test_organisation_and_endpoint_are_carried_through(self, spark):
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
                    "organisation:1",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        row = result.collect()[0]
        assert row["organisation"] == "organisation:1"
        assert row["endpoint"] == "endpoint-aaa"

    def test_references_are_unique(self, spark):
        """No two issue tasks should share a reference."""
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
                    "organisation-x",
                    "endpoint-aaa",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                    "organisation-x",
                    "endpoint-aaa",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "name",
                    "missing-value",
                    "error",
                    "external",
                    "organisation-x",
                    "endpoint-aaa",
                ),
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        references = [row["reference"] for row in result.collect()]
        assert len(references) == len(set(references))

    def test_explodes_multi_org_into_one_task_per_organisation(self, spark):
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
                    "organisation-x;organisation-y",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        rows = result.collect()
        assert len(rows) == 2
        assert {row["organisation"] for row in rows} == {
            "organisation-x",
            "organisation-y",
        }

    def test_duplicate_organisation_in_list_is_not_double_counted(self, spark):
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
                    "organisation-x;organisation-x",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        rows = result.collect()
        assert len(rows) == 1
        assert json.loads(rows[0]["details"])["count"] == 1

    def test_per_organisation_counts_after_explode(self, spark):
        """An issue on a resource shared by two orgs, plus another issue on the
        same resource/field affecting only one of them, should produce
        per-organisation counts rather than one combined count."""
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
                    "organisation-x;organisation-y",
                    "endpoint-aaa",
                ),
                (
                    "dataset-a",
                    "resource-aaa",
                    "geometry",
                    "invalid-geometry",
                    "error",
                    "external",
                    "organisation-x",
                    "endpoint-aaa",
                ),
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        by_org = {row["organisation"]: row for row in result.collect()}
        assert json.loads(by_org["organisation-x"]["details"])["count"] == 2
        assert json.loads(by_org["organisation-y"]["details"])["count"] == 1

    def test_exploded_org_rows_have_distinct_references(self, spark):
        """organisation is part of the reference hash, so per-org rows from the
        same source issue don't collide on the Postgres task_pkey."""
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
                    "organisation-x;organisation-y",
                    "endpoint-aaa",
                )
            ],
            ISSUE_COLUMNS,
        )
        result = transform_issues_to_tasks(df)
        references = [row["reference"] for row in result.collect()]
        assert len(references) == len(set(references))
