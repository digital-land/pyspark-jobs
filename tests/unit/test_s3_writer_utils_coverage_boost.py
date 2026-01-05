import os
import sys
from unittest.mock import Mock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


class TestS3WriterUtilsCoverage:
    """Tests to improve s3_writer_utils.py coverage from 59.38% to help reach 80%."""

    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_write_to_s3_format_missing_lines(self, mock_logger):
        """Test missing lines in write_to_s3_format function."""
        from jobs.utils.s3_writer_utils import write_to_s3_format

        mock_logger.return_value = Mock()
        mock_df = Mock()
        mock_spark = Mock()

        # Test line 100 - early return for None DataFrame
        result = write_to_s3_format(
            None, "s3://bucket/", "dataset", "entity", mock_spark, "dev"
        )
        assert result is None

    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_write_to_s3_missing_lines(self, mock_logger):
        """Test missing lines in write_to_s3 function."""
        from jobs.utils.s3_writer_utils import write_to_s3

        mock_logger.return_value = Mock()

        # Test line 269 - early return for None DataFrame
        result = write_to_s3(None, "s3://bucket/", "dataset", "entity", "dev")
        assert result is None

    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_cleanup_dataset_data_missing_lines(self, mock_logger):
        """Test missing lines in cleanup_dataset_data function."""
        from jobs.utils.s3_writer_utils import cleanup_dataset_data

        mock_logger.return_value = Mock()

        with patch("boto3.client") as mock_boto3:
            mock_s3 = Mock()
            mock_boto3.return_value = mock_s3
            mock_s3.list_objects_v2.return_value = {"Contents": []}

            # Test line 498 - no objects to delete
            result = cleanup_dataset_data("s3://bucket/path/", "dataset")
            assert result["objects_deleted"] == 0
