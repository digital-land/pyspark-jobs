"""
Unit tests for task transformer UDF functions.

These functions are pure Python with no Spark dependency so can be tested
without a Spark session. For tests of the full transformer functions
(transform_log_to_tasks, transform_issues_to_tasks) with real DataFrames,
see tests/integration/transform/test_task_transformer.py.
"""

import json

from jobs.transform.task_transformer import _issue_details_udf, _log_details_udf


class TestLogDetailsUdf:

    def test_numeric_status_is_converted_to_int(self):
        result = json.loads(_log_details_udf("500", ""))
        assert result["status"] == 500
        assert isinstance(result["status"], int)

    def test_non_numeric_status_is_kept_as_string(self):
        result = json.loads(_log_details_udf("unknown", ""))
        assert result["status"] == "unknown"
        assert isinstance(result["status"], str)

    def test_exception_message_is_preserved(self):
        result = json.loads(_log_details_udf("500", "Connection refused"))
        assert result["exception"] == "Connection refused"

    def test_none_exception_becomes_empty_string(self):
        result = json.loads(_log_details_udf("404", None))
        assert result["exception"] == ""

    def test_output_has_only_status_and_exception_keys(self):
        result = json.loads(_log_details_udf("404", "Not found"))
        assert set(result.keys()) == {"status", "exception"}


class TestIssueDetailsUdf:

    def test_normal_case_produces_correct_structure(self):
        result = json.loads(_issue_details_udf("invalid-geometry", "3", "geometry"))
        assert result == {
            "issue_type": "invalid-geometry",
            "count": 3,
            "field": "geometry",
        }

    def test_count_is_converted_to_int(self):
        result = json.loads(_issue_details_udf("missing-value", "10", "name"))
        assert result["count"] == 10
        assert isinstance(result["count"], int)

    def test_none_count_becomes_zero(self):
        result = json.loads(_issue_details_udf("missing-value", None, "name"))
        assert result["count"] == 0

    def test_none_issue_type_becomes_empty_string(self):
        result = json.loads(_issue_details_udf(None, "1", "name"))
        assert result["issue_type"] == ""

    def test_none_field_becomes_empty_string(self):
        result = json.loads(_issue_details_udf("missing-value", "1", None))
        assert result["field"] == ""

    def test_output_has_only_expected_keys(self):
        result = json.loads(_issue_details_udf("invalid-geometry", "2", "geometry"))
        assert set(result.keys()) == {"issue_type", "count", "field"}
