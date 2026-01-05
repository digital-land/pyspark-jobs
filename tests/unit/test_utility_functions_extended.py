"""Additional utility tests to increase coverage without PySpark dependencies."""

import os
import sys
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import pytest

from jobs.utils.path_utils import (
    load_json_from_repo,
    resolve_desktop_path,
    resolve_repo_path,
)

# Import utility functions that don't require PySpark
from jobs.utils.s3_format_utils import parse_possible_json


class TestUtilityFunctions:
    """Test utility functions that don't require PySpark."""

    def test_parse_possible_json_edge_cases(self):
        """Test edge cases for JSON parsing."""
        # Test numeric strings
        assert parse_possible_json("123") == 123
        assert parse_possible_json("123.45") == 123.45
        assert parse_possible_json("0") == 0

        # Test boolean strings
        assert parse_possible_json("true") is True
        assert parse_possible_json("false") is False

        # Test null
        assert parse_possible_json("null") is None

        # Test arrays
        assert parse_possible_json("[1,2,3]") == [1, 2, 3]
        assert parse_possible_json('["a","b","c"]') == ["a", "b", "c"]

        # Test nested objects
        nested = '{"outer": {"inner": {"deep": "value"}}}'
        expected = {"outer": {"inner": {"deep": "value"}}}
        assert parse_possible_json(nested) == expected

    def test_parse_possible_json_malformed(self):
        """Test malformed JSON handling."""
        malformed_cases = [
            "{key: value}",  # Unquoted keys
            "{'key': 'value'}",  # Single quotes
            '{"key": value}',  # Unquoted value
            '{"key": "value",}',  # Trailing comma
            "undefined",
            "{",  # Incomplete
            "}",  # Just closing brace
        ]

        for case in malformed_cases:
            assert parse_possible_json(case) is None

    def test_parse_possible_json_infinity(self):
        """Test Infinity handling separately."""
        import math

        result = parse_possible_json("Infinity")
        assert math.isinf(result)

    def test_parse_possible_json_nan(self):
        """Test NaN handling separately."""
        import math

        result = parse_possible_json("NaN")
        assert math.isnan(result)

    def test_resolve_desktop_path(self):
        """Test desktop path resolution."""
        result = resolve_desktop_path("test_file.txt")
        assert "Desktop" in result
        assert "test_file.txt" in result
        assert os.path.expanduser("~") in result

    def test_resolve_repo_path(self):
        """Test repo path resolution."""
        result = resolve_repo_path("src/jobs/main.py")
        assert "pyspark - jobs" in result
        assert "src/jobs/main.py" in result
        assert os.path.expanduser("~") in result

    @patch("builtins.open")
    @patch("json.load")
    def test_load_json_from_repo(self, mock_json_load, mock_open):
        """Test JSON loading from repo path."""
        mock_json_load.return_value = {"test": "data"}

        result = load_json_from_repo("config/test.json")

        assert result == {"test": "data"}
        mock_open.assert_called_once()
        mock_json_load.assert_called_once()


class TestConfigurationHandling:
    """Test configuration and constants."""

    def test_csv_config_constants(self):
        """Test CSV configuration constants."""
        from jobs.csv_s3_writer import CSV_CONFIG

        # Test all expected configuration keys
        expected_keys = [
            "include_header",
            "sep",
            "date_format",
            "coalesce_to_single_file",
        ]
        for key in expected_keys:
            assert key in CSV_CONFIG

        # Test specific values
        assert CSV_CONFIG["include_header"] is True
        assert CSV_CONFIG["sep"] == ","
        assert CSV_CONFIG["date_format"] == "yyyy - MM - dd"
        assert CSV_CONFIG["coalesce_to_single_file"] is True

    def test_logger_configuration(self):
        """Test logger configuration."""
        from jobs.utils.logger_config import get_logger

        # Test logger creation
        logger = get_logger("test_module")
        assert logger is not None
        assert logger.name == "test_module"

    def test_dataset_typology_mapping(self):
        """Test dataset typology mapping."""
        from jobs.utils.s3_dataset_typology import get_dataset_typology

        # Mock the load_datasets function to avoid network calls
        with patch("jobs.utils.s3_dataset_typology.load_datasets") as mock_load:
            mock_load.return_value = [
                {"dataset": "title - boundary", "typology": "geography"},
                {"dataset": "transport - access - node", "typology": "geography"},
            ]

            # Test known dataset types
            assert get_dataset_typology("title - boundary") == "geography"
            assert get_dataset_typology("transport - access - node") == "geography"
            assert get_dataset_typology("unknown - dataset") is None


class TestErrorHandling:
    """Test error handling and exception classes."""

    def test_csv_writer_error(self):
        """Test CSVWriterError exception."""
        from jobs.csv_s3_writer import CSVWriterError

        # Test basic error creation
        error = CSVWriterError("Test message")
        assert str(error) == "Test message"

        # Test error with formatting
        error = CSVWriterError("Error with {}: {}", "param", 123)
        assert "param" in str(error)

    def test_aurora_import_error(self):
        """Test AuroraImportError exception."""
        from jobs.csv_s3_writer import AuroraImportError

        error = AuroraImportError("Aurora connection failed")
        assert str(error) == "Aurora connection failed"

        # Test inheritance
        assert isinstance(error, Exception)


class TestStringProcessing:
    """Test string processing utilities."""

    def test_json_string_cleaning(self):
        """Test JSON string cleaning functionality."""
        # Test cases that parse_possible_json handles
        test_cases = [
            # Double - escaped quotes
            ('{"key"": ""value""}', {"key": "value"}),
            ('{"nested"": {""inner"": ""value""}}', {"nested": {"inner": "value"}}),
            # Outer quotes removal
            ('"{""key"": ""value""}"', {"key": "value"}),
            ('"[1, 2, 3]"', [1, 2, 3]),
            # Mixed cases
            ('"{""array"": [1, 2, 3]}"', {"array": [1, 2, 3]}),
        ]

        for input_str, expected in test_cases:
            result = parse_possible_json(input_str)
            assert result == expected

    def test_empty_and_whitespace_handling(self):
        """Test handling of empty and whitespace strings."""
        empty_cases = ["", "   ", "\t", "\n", "\r\n"]

        for case in empty_cases:
            result = parse_possible_json(case)
            assert result is None


class TestDataTypeHandling:
    """Test data type handling utilities."""

    def test_df_utils_functions(self):
        """Test DataFrame utility functions."""
        from jobs.utils.df_utils import get_loggerf, get_loggerf

        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.show.return_value = None

        # Test get_loggerf in different environments (no logger patching needed)
        get_loggerf(mock_df, 5, "development")  # Should call show
        get_loggerf(mock_df, 5, "production")  # Should not call show

        # Test get_loggerf
        result = get_loggerf(mock_df, "development")
        assert result == 100

        result = get_loggerf(mock_df, "production")
        assert result is None

    def test_path_resolution_edge_cases(self):
        """Test path resolution edge cases."""
        # Test empty relative path
        result = resolve_desktop_path("")
        assert "Desktop" in result

        # Test nested path
        result = resolve_repo_path("src/jobs/utils/test.py")
        assert "src/jobs/utils/test.py" in result

        # Test path with dots
        result = resolve_desktop_path("../test.txt")
        assert "test.txt" in result


@pytest.mark.unit
class TestIntegrationScenarios:
    """Test integration scenarios without PySpark."""

    def test_json_processing_workflow(self):
        """Test complete JSON processing workflow."""
        # Simulate processing a batch of JSON strings
        json_strings = [
            '{"id": 1, "name": "Item 1"}',
            '{"id": 2, "name": "Item 2"}',
            "invalid_json",
            '{"nested": {"key": "value"}}',
        ]

        results = []
        for json_str in json_strings:
            parsed = parse_possible_json(json_str)
            if parsed is not None:
                results.append(parsed)

        # Should have 3 valid results (excluding invalid_json)
        assert len(results) == 3
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2
        assert results[2]["nested"]["key"] == "value"

    @patch("boto3.client")
    def test_s3_operations_workflow(self, mock_boto3):
        """Test S3 operations workflow."""
        from jobs.utils.s3_format_utils import renaming

        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        # Test successful renaming
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "csv/dataset.csv/part - 00000 - abc.csv"},
                {"Key": "csv/dataset.csv/_SUCCESS"},
            ]
        }

        renaming("dataset", "test - bucket")

        # Verify operations
        mock_s3.list_objects_v2.assert_called_once()
        mock_s3.copy_object.assert_called_once()
        mock_s3.delete_object.assert_called_once()

    def test_error_recovery_workflow(self):
        """Test error recovery in processing workflow."""
        # Simulate processing with some failures
        mixed_data = [
            '{"valid": "json"}',
            "invalid json",
            '{"another": "valid"}',
            None,
            '{"third": "valid"}',
        ]

        valid_count = 0
        invalid_count = 0

        for item in mixed_data:
            try:
                result = parse_possible_json(item)
                if result is not None:
                    valid_count += 1
                else:
                    invalid_count += 1
            except Exception:
                invalid_count += 1

        assert valid_count == 3
        assert invalid_count == 2
