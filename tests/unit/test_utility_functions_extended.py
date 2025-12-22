"""Additional utility tests to increase coverage without PySpark dependencies."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Import utility functions that don't require PySpark
from jobs.utils.s3_format_utils import parse_possible_json
from jobs.utils.path_utils import validate_s3_path, extract_s3_components


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
            "{\"key\": value}",  # Unquoted value
            "{\"key\": \"value\",}",  # Trailing comma
            "undefined",
            "NaN",
            "Infinity",
            "{",  # Incomplete
            "}",  # Just closing brace
        ]
        
        for case in malformed_cases:
            assert parse_possible_json(case) is None

    def test_validate_s3_path_valid_paths(self):
        """Test S3 path validation with valid paths."""
        valid_paths = [
            "s3://bucket/path/",
            "s3://my-bucket/folder/subfolder/",
            "s3://bucket-name/file.txt",
            "s3://bucket123/path_with_underscores/",
            "s3://bucket/path/with-hyphens/",
        ]
        
        for path in valid_paths:
            assert validate_s3_path(path) is True

    def test_validate_s3_path_invalid_paths(self):
        """Test S3 path validation with invalid paths."""
        invalid_paths = [
            "",
            None,
            "http://bucket/path/",
            "https://bucket/path/",
            "ftp://bucket/path/",
            "bucket/path/",
            "/local/path/",
            "s3:/bucket/path/",  # Missing slash
            "s3:///path/",  # Missing bucket
        ]
        
        for path in invalid_paths:
            assert validate_s3_path(path) is False

    def test_extract_s3_components_valid(self):
        """Test S3 component extraction from valid paths."""
        test_cases = [
            ("s3://bucket/path/file.txt", ("bucket", "path/file.txt")),
            ("s3://my-bucket/", ("my-bucket", "")),
            ("s3://bucket/folder/subfolder/", ("bucket", "folder/subfolder/")),
        ]
        
        for s3_path, expected in test_cases:
            result = extract_s3_components(s3_path)
            assert result == expected

    def test_extract_s3_components_invalid(self):
        """Test S3 component extraction from invalid paths."""
        invalid_paths = [
            "http://bucket/path/",
            "bucket/path/",
            "",
            None,
        ]
        
        for path in invalid_paths:
            with pytest.raises(ValueError):
                extract_s3_components(path)


class TestConfigurationHandling:
    """Test configuration and constants."""

    def test_csv_config_constants(self):
        """Test CSV configuration constants."""
        from jobs.csv_s3_writer import CSV_CONFIG
        
        # Test all expected configuration keys
        expected_keys = ['include_header', 'sep', 'date_format', 'coalesce_to_single_file']
        for key in expected_keys:
            assert key in CSV_CONFIG
        
        # Test specific values
        assert CSV_CONFIG['include_header'] is True
        assert CSV_CONFIG['sep'] == ","
        assert CSV_CONFIG['date_format'] == "yyyy-MM-dd"
        assert CSV_CONFIG['coalesce_to_single_file'] is True

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
        
        # Test known dataset types
        test_cases = [
            ("title-boundary", "geography"),
            ("transport-access-node", "geography"),
            ("unknown-dataset", "unknown"),
        ]
        
        for dataset, expected_typology in test_cases:
            result = get_dataset_typology(dataset)
            assert result == expected_typology


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
            # Double-escaped quotes
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
        from jobs.utils.df_utils import show_df, count_df
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.show.return_value = None
        
        # Test show_df in different environments
        show_df(mock_df, 5, "development")  # Should call show
        show_df(mock_df, 5, "production")   # Should not call show
        
        # Test count_df
        result = count_df(mock_df, "development")
        assert result == 100
        
        result = count_df(mock_df, "production")
        assert result is None

    def test_path_validation_edge_cases(self):
        """Test path validation edge cases."""
        # Test very long paths
        long_bucket = "a" * 63  # Max bucket name length
        long_path = "s3://{}/path/".format(long_bucket)
        assert validate_s3_path(long_path) is True
        
        # Test minimum valid path
        min_path = "s3://a/"
        assert validate_s3_path(min_path) is True
        
        # Test path with special characters
        special_path = "s3://bucket/path/with%20spaces/"
        assert validate_s3_path(special_path) is True


@pytest.mark.unit
class TestIntegrationScenarios:
    """Test integration scenarios without PySpark."""

    def test_json_processing_workflow(self):
        """Test complete JSON processing workflow."""
        # Simulate processing a batch of JSON strings
        json_strings = [
            '{"id": 1, "name": "Item 1"}',
            '"{""id"": 2, ""name"": ""Item 2""}"',  # Double-escaped
            '{"id": 3, "nested": {"value": "test"}}',
            'invalid json',
            None,
            '[]',
            'null'
        ]
        
        results = []
        for json_str in json_strings:
            parsed = parse_possible_json(json_str)
            if parsed is not None:
                results.append(parsed)
        
        # Should have 5 valid results (excluding invalid json and None)
        assert len(results) == 5
        assert results[0] == {"id": 1, "name": "Item 1"}
        assert results[1] == {"id": 2, "name": "Item 2"}

    @patch('boto3.client')
    def test_s3_operations_workflow(self, mock_boto3):
        """Test S3 operations workflow."""
        from jobs.utils.s3_format_utils import renaming
        
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        
        # Test successful renaming
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'csv/dataset.csv/part-00000-abc.csv'},
                {'Key': 'csv/dataset.csv/_SUCCESS'}
            ]
        }
        
        renaming("dataset", "test-bucket")
        
        # Verify operations
        mock_s3.list_objects_v2.assert_called_once()
        mock_s3.copy_object.assert_called_once()
        mock_s3.delete_object.assert_called_once()

    def test_error_recovery_workflow(self):
        """Test error recovery in processing workflow."""
        # Simulate processing with some failures
        mixed_data = [
            '{"valid": "json"}',
            'invalid json',
            '{"another": "valid"}',
            None,
            '{"third": "valid"}'
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