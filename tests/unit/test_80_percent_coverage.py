import os
import sys
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


class TestCoverageBoost:
    """Minimal tests to reach 80% coverage target."""

    def test_main_collection_data_missing_lines(self):
        """Test missing lines in main_collection_data.py."""
        from jobs.main_collection_data import validate_s3_path

        # Test line 110 - path too short
        with pytest.raises(ValueError, match="Path too short"):
            validate_s3_path("s3://")

    @patch("jobs.main_collection_data.pkgutil.get_data")
    def test_load_metadata_pkgutil_failure(self, mock_get_data):
        """Test pkgutil failure path in load_metadata."""
        from jobs.main_collection_data import load_metadata

        mock_get_data.return_value = None

        with pytest.raises(FileNotFoundError):
            load_metadata("config/test.json")

    def test_csv_s3_writer_missing_lines(self):
        """Test missing lines in csv_s3_writer.py."""
        from jobs.csv_s3_writer import _cleanup_temp_path

        with patch("boto3.client") as mock_boto3:
            mock_s3 = Mock()
            mock_boto3.return_value = mock_s3
            mock_s3.list_objects_v2.return_value = {}  # No Contents key

            _cleanup_temp_path("s3://bucket/temp/")
            mock_s3.delete_objects.assert_not_called()

    def test_postgres_writer_utils_error_paths(self):
        """Test error paths in postgres_writer_utils.py."""
        from jobs.utils.postgres_writer_utils import get_performance_recommendations

        # Test with very small DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 50

        result = get_performance_recommendations(mock_df, "entity", "test")
        assert result["method"] == "jdbc_batch"
        assert result["batch_size"] == 1000

    def test_s3_format_utils_error_handling(self):
        """Test error handling in s3_format_utils.py."""
        from jobs.utils.s3_format_utils import parse_possible_json

        # Test lines 148-153 - malformed JSON
        result = parse_possible_json('{"invalid": json}')
        assert result is None

        # Test lines 198-200 - empty string
        result = parse_possible_json("")
        assert result is None
