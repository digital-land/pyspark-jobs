"""
Integration tests for transform_log_to_tasks and transform_issues_to_tasks.

Uses a real Spark session. Tests the full transformer functions with real
DataFrames including filtering, grouping, and reference hash generation.
To see the tests of the underlying UDF functions without Spark, see:
tests/unit/transform/test_task_transformer.py
"""

import json

from jobs.transform.task_transformer import (
    transform_expectations_to_tasks,
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


EXPECTATION_COLUMNS = [
    "dataset",
    "organisation",
    "operation",
    "passed",
    "severity",
    "responsibility",
    "details",
]

ORG_COLUMNS = ["organisation_entity", "organisation"]


def _details(failures=None, field="name", error=None):
    """Build a details JSON blob in the shape the expectation parquet uses."""
    blob = {"actual": len(failures or []), "expected": 0}
    if field is not None:
        blob["field"] = field
    if error is not None:
        blob["error"] = error
    if failures is not None:
        blob["failures"] = failures
    return json.dumps(blob)


def _org_df(spark):
    return _build_df(
        spark,
        [("366", "local-authority:BRO"), ("202", "local-authority:LBH")],
        ORG_COLUMNS,
    )


def _expectation_row(
    operation="duplicate_name_check",
    passed="False",
    severity="warning",
    details=None,
    dataset="dataset-a",
):
    # top-level organisation is blank on dataset-wide checks; attribution lives
    # inside details.failures
    return (
        dataset,
        "",
        operation,
        passed,
        severity,
        "external",
        details if details is not None else _details([{"organisation_entity": "366"}]),
    )


class TestTransformExpectationsToTasks:

    def test_excludes_rows_that_passed(self, spark):
        """`passed` is written as a string, so the filter casts rather than
        comparing to a bool. Either shape yields the same rows."""
        df = _build_df(
            spark,
            [
                _expectation_row(passed="False"),
                _expectation_row(passed="True"),
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result is not None
        assert result.count() == 1

    def test_excludes_info_severity_rows(self, spark):
        df = _build_df(
            spark,
            [
                _expectation_row(severity="warning"),
                _expectation_row(severity="info", operation="other_check"),
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.count() == 1

    def test_includes_internal_responsibility_rows(self, spark):
        """Control A copies the issue filter, which has no responsibility
        filter — duplicate_geometry_check is internal and still produces tasks."""
        row = list(_expectation_row(operation="duplicate_geometry_check"))
        row[5] = "internal"
        df = _build_df(spark, [tuple(row)], EXPECTATION_COLUMNS)
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.count() == 1
        assert result.collect()[0]["responsibility"] == "internal"

    def test_drops_rows_whose_check_errored(self, spark):
        """Since digital-land-python#587 a failed geometry fetch reports
        {"error": ...} rather than an empty details dict."""
        df = _build_df(
            spark,
            [
                _expectation_row(
                    operation="count_lpa_boundary",
                    details=_details(error="404 Client Error", field=None),
                ),
                _expectation_row(),
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.count() == 1
        assert json.loads(result.collect()[0]["details"])["operation"] == (
            "duplicate_name_check"
        )

    def test_error_takes_precedence_over_failures(self, spark):
        """A check that errored is dropped even if it somehow reported
        failures — its findings cannot be trusted. Without this guard the
        no-failures filter alone would let such a row through."""
        df = _build_df(
            spark,
            [
                _expectation_row(
                    operation="count_lpa_boundary",
                    details=json.dumps(
                        {
                            "error": "404 Client Error",
                            "failures": [{"organisation_entity": "366", "count": 3}],
                        }
                    ),
                ),
                _expectation_row(),
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.count() == 1
        assert json.loads(result.collect()[0]["details"])["operation"] == (
            "duplicate_name_check"
        )

    def test_drops_rows_with_no_failures(self, spark):
        """No failures means nothing attributable to an organisation."""
        df = _build_df(
            spark,
            [
                _expectation_row(details=_details(failures=[])),
                _expectation_row(details=_details(failures=None, field=None)),
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result is None

    def test_returns_none_when_no_matching_rows(self, spark):
        df = _build_df(spark, [_expectation_row(passed="True")], EXPECTATION_COLUMNS)
        assert transform_expectations_to_tasks(df, _org_df(spark)) is None

    def test_sums_per_failure_count_where_present(self, spark):
        """A duplicate-name failure carries the number of entities sharing the
        name, so count sums them while groups counts the failures."""
        df = _build_df(
            spark,
            [
                _expectation_row(
                    details=_details(
                        [
                            {"organisation_entity": "366", "count": 5},
                            {"organisation_entity": "366", "count": 4},
                        ]
                    )
                )
            ],
            EXPECTATION_COLUMNS,
        )
        details = json.loads(
            transform_expectations_to_tasks(df, _org_df(spark)).collect()[0]["details"]
        )
        assert details["count"] == 9
        assert details["groups"] == 2

    def test_counts_failures_where_no_per_failure_count(self, spark):
        """name_is_a_code_check emits one failure per entity with no count, so
        count and groups come out equal — which reads as 'no grouping'."""
        df = _build_df(
            spark,
            [
                _expectation_row(
                    operation="name_is_a_code_check",
                    details=_details(
                        [
                            {"organisation_entity": "366", "entity": 1},
                            {"organisation_entity": "366", "entity": 2},
                            {"organisation_entity": "366", "entity": 3},
                        ]
                    ),
                )
            ],
            EXPECTATION_COLUMNS,
        )
        details = json.loads(
            transform_expectations_to_tasks(df, _org_df(spark)).collect()[0]["details"]
        )
        assert details["count"] == 3
        assert details["groups"] == 3

    def test_groups_per_organisation_and_operation(self, spark):
        df = _build_df(
            spark,
            [
                _expectation_row(
                    details=_details(
                        [
                            {"organisation_entity": "366", "count": 2},
                            {"organisation_entity": "202", "count": 7},
                        ]
                    )
                ),
                _expectation_row(
                    operation="name_is_a_code_check",
                    details=_details([{"organisation_entity": "366"}]),
                ),
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.count() == 3
        by_org = {
            (r["organisation"], json.loads(r["details"])["operation"]): json.loads(
                r["details"]
            )["count"]
            for r in result.collect()
        }
        assert by_org[("local-authority:BRO", "duplicate_name_check")] == 2
        assert by_org[("local-authority:LBH", "duplicate_name_check")] == 7
        assert by_org[("local-authority:BRO", "name_is_a_code_check")] == 1

    def test_resolves_organisation_entity_to_curie(self, spark):
        df = _build_df(spark, [_expectation_row()], EXPECTATION_COLUMNS)
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.collect()[0]["organisation"] == "local-authority:BRO"

    def test_drops_failures_with_an_unknown_organisation(self, spark):
        """Inner join: an unresolvable entity is dropped rather than becoming an
        unattributed task no LPA can see."""
        df = _build_df(
            spark,
            [
                _expectation_row(
                    details=_details(
                        [
                            {"organisation_entity": "366", "count": 2},
                            {"organisation_entity": "999999", "count": 3},
                        ]
                    )
                )
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.count() == 1
        assert result.collect()[0]["organisation"] == "local-authority:BRO"

    def test_output_has_correct_columns(self, spark):
        df = _build_df(spark, [_expectation_row()], EXPECTATION_COLUMNS)
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.columns == [
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
        ]

    def test_task_source_is_expectation(self, spark):
        df = _build_df(spark, [_expectation_row()], EXPECTATION_COLUMNS)
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert result.collect()[0]["task_source"] == "expectation"

    def test_endpoint_and_resource_are_blank(self, spark):
        """Expectations run on the assembled dataset, so there is no endpoint or
        resource to attribute them to."""
        df = _build_df(spark, [_expectation_row()], EXPECTATION_COLUMNS)
        row = transform_expectations_to_tasks(df, _org_df(spark)).collect()[0]
        assert row["endpoint"] == ""
        assert row["resource"] == ""

    def test_details_uses_operation_not_issue_type(self, spark):
        """submit's task list only renders tasks whose details carry issue_type
        and field, so keying on operation keeps these out of that path."""
        df = _build_df(spark, [_expectation_row()], EXPECTATION_COLUMNS)
        details = json.loads(
            transform_expectations_to_tasks(df, _org_df(spark)).collect()[0]["details"]
        )
        assert details["operation"] == "duplicate_name_check"
        assert "issue_type" not in details

    def test_reference_is_16_chars(self, spark):
        df = _build_df(spark, [_expectation_row()], EXPECTATION_COLUMNS)
        result = transform_expectations_to_tasks(df, _org_df(spark))
        assert len(result.collect()[0]["reference"]) == 16

    def test_references_are_unique(self, spark):
        df = _build_df(
            spark,
            [
                _expectation_row(
                    details=_details(
                        [
                            {"organisation_entity": "366", "count": 2},
                            {"organisation_entity": "202", "count": 7},
                        ]
                    )
                ),
                _expectation_row(operation="name_is_a_code_check"),
            ],
            EXPECTATION_COLUMNS,
        )
        result = transform_expectations_to_tasks(df, _org_df(spark))
        refs = [r["reference"] for r in result.collect()]
        assert len(refs) == len(set(refs))
