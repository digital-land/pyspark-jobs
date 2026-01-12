import os
import sys

import pytest

"""Final targeted tests to push coverage from 76.27% to 80%+ by covering remaining high - impact lines."""

from unittest.mock import MagicMock, Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

# Mock all external dependencies
with patch.dict(
    "sys.modules",
    {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
        "pyspark.sql.types": MagicMock(),
        "pyspark.sql.window": MagicMock(),
        "pg8000": MagicMock(),
        "boto3": MagicMock(),
        "requests": MagicMock(),
    },
):
    from jobs.utils import postgres_writer_utils, s3_format_utils, s3_writer_utils


def create_mock_df(columns=None, count_return=100):
    """Create a mock DataFrame."""
    mock_df = Mock()
    mock_df.columns = columns or ["entity", "name"]
    mock_df.count.return_value = count_return
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.write = Mock()
    mock_df.write.jdbc = Mock()
    mock_df.schema = []
    mock_df.dtypes = [("entity", "bigint"), ("name", "string")]
    return mock_df


@pytest.mark.unit
class TestPostgresWriterUtilsTargeted:
    """Target specific lines in postgres_writer_utils.py."""

    def test_postgres_writer_utils_import(self):
        """Test postgres_writer_utils module imports correctly."""
        assert hasattr(postgres_writer_utils, "_ensure_required_columns")
        assert hasattr(postgres_writer_utils, "write_dataframe_to_postgres_jdbc")

    def test_ensure_required_columns_all_types(self):
        """Test all column type handling."""
        mock_df = create_mock_df(["existing_col"])
        required_cols = ["entity", "json", "entry_date", "custom_field"]

        with patch("jobs.utils.postgres_writer_utils.lit") as mock_lit, patch(
            "jobs.utils.postgres_writer_utils.col"
        ) as mock_col, patch(
            "jobs.utils.postgres_writer_utils.to_json"
        ) as mock_to_json:

            mock_lit.return_value.cast.return_value = "mocked_column"
            mock_col.return_value.cast.return_value = "mocked_column"
            mock_to_json.return_value = "json_string"

            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols, {}, Mock()
            )

            assert mock_df.withColumn.call_count >= 3
            assert result == mock_df


@pytest.mark.unit
class TestS3FormatUtilsTargeted:
    """Target specific lines in s3_format_utils.py."""

    def test_parse_possible_json_valid(self):
        """Test parse_possible_json with valid JSON."""
        result = s3_format_utils.parse_possible_json('{"key": "value"}')
        assert result == {"key": "value"}

    def test_parse_possible_json_invalid(self):
        """Test parse_possible_json with invalid JSON."""
        assert s3_format_utils.parse_possible_json(None) is None
        assert s3_format_utils.parse_possible_json("invalid json") is None

    def test_s3_format_utils_import(self):
        """Test s3_format_utils module imports correctly."""
        assert hasattr(s3_format_utils, "parse_possible_json")
        assert hasattr(s3_format_utils, "flatten_s3_json")
        assert hasattr(s3_format_utils, "s3_csv_format")


@pytest.mark.unit
class TestS3WriterUtilsTargeted:
    """Target specific lines in s3_writer_utils.py."""

    def test_wkt_to_geojson_point(self):
        """Test POINT conversion."""
        wkt = "POINT (1.234567 2.345678)"
        result = s3_writer_utils.wkt_to_geojson(wkt)

        expected = {"type": "Point", "coordinates": [1.234567, 2.345678]}
        assert result == expected

    def test_wkt_to_geojson_polygon(self):
        """Test POLYGON conversion."""
        wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
        result = s3_writer_utils.wkt_to_geojson(wkt)

        assert result["type"] == "Polygon"
        assert len(result["coordinates"]) == 1

    @patch("requests.get")
    def test_fetch_dataset_schema_fields_success(self, mock_get):
        """Test schema field fetching."""
        mock_response = Mock()
        mock_response.text = "---\nfields:\n- field: entity\n- field: name\n---"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = s3_writer_utils.fetch_dataset_schema_fields("test - dataset")

        assert result == ["entity", "name"]
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_fetch_dataset_schema_fields_failure(self, mock_get):
        """Test schema field fetching failure."""
        mock_get.side_effect = Exception("Network error")

        result = s3_writer_utils.fetch_dataset_schema_fields("test - dataset")

        assert result == []

    def test_ensure_schema_fields_missing(self):
        """Test ensuring schema fields when missing."""
        mock_df = create_mock_df(["existing_field"])

        with patch(
            "jobs.utils.s3_writer_utils.fetch_dataset_schema_fields"
        ) as mock_fetch, patch("jobs.utils.s3_writer_utils.lit") as mock_lit:

            mock_fetch.return_value = ["existing_field", "missing_field"]
            mock_lit.return_value = "empty_string"

            result = s3_writer_utils.ensure_schema_fields(mock_df, "test - dataset")

            mock_fetch.assert_called_once()
            mock_df.withColumn.assert_called()
            assert result == mock_df
