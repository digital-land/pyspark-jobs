import os
import sys

import pytest

"""Minimal tests targeting specific missing lines for coverage boost."""

from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


@pytest.mark.unit
class TestMissingLines:
    """Target specific missing lines for coverage boost."""

    def test_postgres_writer_utils_lines_93_256(self):
        """Test postgres_writer_utils lines 93 - 256."""
        with patch.dict(
            "sys.modules",
            {
                "pg8000": MagicMock(),
                "pyspark.sql.functions": MagicMock(),
                "pyspark.sql.types": MagicMock(),
            },
        ):
            from jobs.utils.postgres_writer_utils import _ensure_required_columns

            # Test missing columns scenario (lines 93 - 110)
            mock_df = Mock()
            mock_df.columns = ["entity"]
            mock_df.withColumn.return_value = mock_df

            with patch("jobs.utils.postgres_writer_utils.lit") as mock_lit, patch(
                "jobs.utils.postgres_writer_utils.col"
            ) as mock_col, patch(
                "jobs.utils.postgres_writer_utils.to_json"
            ) as mock_to_json:

                mock_lit.return_value.cast.return_value = "col"
                mock_col.return_value.cast.return_value = "col"
                mock_to_json.return_value = "json"

                # Test with logger (lines 100 - 105)
                logger = Mock()
                result = _ensure_required_columns(
                    mock_df, ["entity", "name", "json"], {"name": "default"}, logger
                )
                logger.warning.assert_called()

                # Test without logger (lines 93 - 99)
                result = _ensure_required_columns(mock_df, ["entity", "name"])
                assert result == mock_df

    def test_s3_format_utils_lines_34_152(self):
        """Test s3_format_utils lines 34 - 152."""
        with patch.dict(
            "sys.modules",
            {
                "pyspark.sql.functions": MagicMock(),
                "pyspark.sql.types": MagicMock(),
                "boto3": MagicMock(),
            },
        ):
            from jobs.utils.s3_format_utils import (
                flatten_s3_json,
                renaming,
                s3_csv_format,
            )

            # Test s3_csv_format with no string columns (lines 34 - 38)
            mock_df = Mock()
            mock_df.schema = []
            result = s3_csv_format(mock_df)
            assert result == mock_df

            # Test flatten_s3_json with no nested columns (lines 89 - 91)
            mock_df.dtypes = [("col1", "string"), ("col2", "int")]
            mock_df.columns = ["col1", "col2"]
            mock_df.select.return_value = mock_df
            result = flatten_s3_json(mock_df)
            assert result == mock_df

            # Test renaming with no contents (lines 113 - 152)
            with patch("jobs.utils.s3_format_utils.boto3.client") as mock_boto3:
                mock_s3 = Mock()
                mock_boto3.return_value = mock_s3
                mock_s3.list_objects_v2.return_value = {}  # No Contents key
                renaming("dataset", "bucket")  # Should handle gracefully

    def test_s3_writer_utils_lines_490_711(self):
        """Test s3_writer_utils lines 490 - 711."""
        with patch.dict(
            "sys.modules",
            {
                "pyspark.sql.functions": MagicMock(),
                "boto3": MagicMock(),
                "requests": MagicMock(),
            },
        ):
            from jobs.utils.s3_writer_utils import (
                ensure_schema_fields,
                fetch_dataset_schema_fields,
                wkt_to_geojson,
            )

            # Test wkt_to_geojson MULTIPOLYGON with multiple polygons (lines 320 - 350)
            wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))"
            result = wkt_to_geojson(wkt)
            assert result["type"] == "Polygon"  # Flattened to Polygon type

            # Test fetch_dataset_schema_fields with no fields section (lines 490 - 520)
            with patch("jobs.utils.s3_writer_utils.requests") as mock_requests:
                mock_response = Mock()
                mock_response.text = "---\nother: value\n---"
                mock_requests.get.return_value = mock_response
                result = fetch_dataset_schema_fields("dataset")
                assert result == []

            # Test ensure_schema_fields with no missing fields (lines 550 - 570)
            mock_df = Mock()
            mock_df.columns = ["entity", "name"]
            with patch(
                "jobs.utils.s3_writer_utils.fetch_dataset_schema_fields",
                return_value=["entity", "name"],
            ):
                result = ensure_schema_fields(mock_df, "dataset")
                assert result == mock_df

    def test_additional_edge_cases(self):
        """Test additional edge cases for coverage."""
        with patch.dict(
            "sys.modules", {"pyspark.sql.functions": MagicMock(), "boto3": MagicMock()}
        ):
            from jobs.utils.s3_format_utils import parse_possible_json
            from jobs.utils.s3_writer_utils import wkt_to_geojson

            # Test MULTIPOLYGON depth parsing (specific algorithm lines)
            wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0), (0.2 0.2, 0.8 0.2, 0.8 0.8, 0.2 0.8, 0.2 0.8, 0.2 0.2)))"
            result = wkt_to_geojson(wkt)
            assert result is not None

            # Test parse_possible_json with simple cases
            result = parse_possible_json('{"key": "value"}')
            assert result == {"key": "value"}

            # Test parse_possible_json with double quote replacement
            result = parse_possible_json('{"key"": ""value""}')
            assert result == {"key": "value"}

    def test_error_paths(self):
        """Test error handling paths for coverage."""
        with patch.dict("sys.modules", {"boto3": MagicMock(), "requests": MagicMock()}):
            from jobs.utils.s3_format_utils import renaming
            from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields

            # Test fetch_dataset_schema_fields timeout/error
            with patch("jobs.utils.s3_writer_utils.requests") as mock_requests:
                mock_requests.get.side_effect = Exception("Timeout")
                result = fetch_dataset_schema_fields("dataset")
                assert result == []

            # Test renaming S3 error handling
            with patch("jobs.utils.s3_format_utils.boto3.client") as mock_boto3:
                mock_s3 = Mock()
                mock_boto3.return_value = mock_s3
                mock_s3.list_objects_v2.side_effect = Exception("S3 Error")

                with pytest.raises(Exception):
                    renaming("dataset", "bucket")

    def test_function_branches(self):
        """Test specific function branches for coverage."""
        with patch.dict(
            "sys.modules",
            {"pyspark.sql.functions": MagicMock(), "pyspark.sql.types": MagicMock()},
        ):
            from jobs.utils.postgres_writer_utils import _ensure_required_columns

            # Test different column types in _ensure_required_columns
            mock_df = Mock()
            mock_df.columns = []
            mock_df.withColumn.return_value = mock_df

            with patch("jobs.utils.postgres_writer_utils.lit") as mock_lit, patch(
                "jobs.utils.postgres_writer_utils.col"
            ) as mock_col:

                mock_lit.return_value.cast.return_value = "col"
                mock_col.return_value.cast.return_value = "col"

                # Test bigint columns
                result = _ensure_required_columns(
                    mock_df, ["entity", "organisation_entity"]
                )
                assert mock_df.withColumn.call_count >= 2

                # Test date columns
                mock_df.withColumn.reset_mock()
                result = _ensure_required_columns(
                    mock_df, ["entry_date", "start_date", "end_date"]
                )
                assert mock_df.withColumn.call_count >= 3

                # Test string columns
                mock_df.withColumn.reset_mock()
                result = _ensure_required_columns(
                    mock_df, ["json", "geojson", "geometry", "point"]
                )
                assert mock_df.withColumn.call_count >= 4
