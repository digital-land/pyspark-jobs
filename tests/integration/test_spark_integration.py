"""Integration tests for Spark operations."""

from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


@pytest.mark.integration
class TestSparkIntegration:
    """Integration tests for Spark session and DataFrame operations."""
import os
import sys
import pytest

    def test_spark_session_creation(self):
        """Test Spark session can be created."""
        with patch("pyspark.sql.SparkSession") as mock_spark:
            mock_builder = Mock()
            mock_spark.builder = mock_builder
            mock_builder.appName.return_value = mock_builder
            mock_builder.config.return_value = mock_builder
            mock_builder.getOrCreate.return_value = Mock()

            from jobs.main_collection_data import create_spark_session

            result = create_spark_session("IntegrationTest")
            assert result is not None

    def test_dataframe_operations_integration(self):
        """Test DataFrame operations work together."""
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name", "value"]
        mock_df.select.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.withColumn.return_value = mock_df

        # Test chained operations
        result = (
            mock_df.select("id", "name").filter("id > 0").withColumn("new_col", "value")
        )
        assert result is not None

    def test_s3_path_validation_integration(self):
        """Test S3 path validation with various inputs."""
        from jobs.main_collection_data import validate_s3_path

        valid_paths = [
            "s3://bucket/path/",
            "s3://my - bucket - 123/data/file.csv",
            "s3://test/folder/subfolder/",
        ]

        for path in valid_paths:
            validate_s3_path(path)  # Should not raise

    def test_csv_writer_integration(self):
        """Test CSV writer components work together."""
        from jobs.csv_s3_writer import AuroraImportError, CSVWriterError

        # Test exception creation
        csv_error = CSVWriterError("Test CSV error")
        aurora_error = AuroraImportError("Test Aurora error")

        assert str(csv_error) == "Test CSV error"
        assert str(aurora_error) == "Test Aurora error"

    @patch("boto3.client")
    def test_s3_operations_integration(self, mock_boto3):
        """Test S3 operations integration."""
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        from jobs.utils.s3_format_utils import renaming

        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "csv/test.csv/part - 00000.csv"}]
        }

        renaming("test", "bucket")
        mock_s3.copy_object.assert_called_once()
        mock_s3.delete_object.assert_called_once()
