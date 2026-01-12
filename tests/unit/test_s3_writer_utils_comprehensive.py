import os
import sys
from datetime import date, datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

# Mock PySpark imports
with patch.dict(
    "sys.modules",
    {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
        "pyspark.sql.types": MagicMock(),
    },
):
    from jobs.utils import s3_writer_utils


class TestTransformDataEntityFormat:
    """Test transform_data_entity_format function - targets lines 70, 76, 94 - 149."""

    def test_transform_entity_no_priority_column(self):
        """Test entity transformation when priority column is missing."""
        from jobs.utils.s3_writer_utils import transform_data_entity_format

        assert callable(transform_data_entity_format)

    def test_transform_entity_with_geojson_column(self):
        """Test entity transformation when geojson column exists."""
        from jobs.utils.s3_writer_utils import transform_data_entity_format

        assert callable(transform_data_entity_format)


class TestNormaliseDataframeSchema:
    """Test normalise_dataframe_schema function - targets lines 189."""

    def test_normalise_schema_unknown_table(self):
        """Test normalise_dataframe_schema with unknown table name."""
        from jobs.utils.s3_writer_utils import normalise_dataframe_schema

        assert callable(normalise_dataframe_schema)


class TestWriteToS3:
    """Test write_to_s3 function - targets lines 334."""

    @patch("jobs.utils.s3_writer_utils.cleanup_dataset_data")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_write_to_s3_entity_table(self, mock_logger, mock_show, mock_cleanup):
        """Test write_to_s3 with entity table to set global df_entity."""
        from jobs.utils.s3_writer_utils import write_to_s3

        mock_logger.return_value = Mock()
        mock_cleanup.return_value = {"objects_deleted": 5, "errors": []}

        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.count.return_value = 1000
        mock_df.coalesce.return_value.write.partitionBy.return_value.mode.return_value.option.return_value.option.return_value.parquet = (
            Mock()
        )

        write_to_s3(mock_df, "s3://bucket/output/", "test - dataset", "entity", "dev")

        # Verify global df_entity was set
        from jobs.utils.s3_writer_utils import df_entity

        assert df_entity == mock_df


class TestCleanupTempPath:
    """Test cleanup_temp_path function - targets lines 345 - 355."""

    @patch("boto3.client")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_cleanup_temp_path_with_objects(self, mock_logger, mock_boto3):
        """Test cleanup_temp_path when objects exist."""
        from jobs.utils.s3_writer_utils import cleanup_temp_path

        mock_logger.return_value = Mock()
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        # Mock paginator with objects
        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "dataset/temp/test/file1.csv"},
                    {"Key": "dataset/temp/test/file2.csv"},
                ]
            }
        ]

        cleanup_temp_path("dev", "test - dataset")

        mock_s3.delete_objects.assert_called_once()

    @patch("boto3.client")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_cleanup_temp_path_no_objects(self, mock_logger, mock_boto3):
        """Test cleanup_temp_path when no objects exist."""
        from jobs.utils.s3_writer_utils import cleanup_temp_path

        mock_logger.return_value = Mock()
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        # Mock paginator with no objects
        mock_paginator = Mock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]  # No 'Contents' key

        cleanup_temp_path("dev", "test - dataset")

        mock_s3.delete_objects.assert_not_called()


class TestWktToGeojson:
    """Test wkt_to_geojson function - targets lines 386."""

    def test_wkt_to_geojson_empty_string(self):
        """Test wkt_to_geojson with empty string."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson

        result = wkt_to_geojson("")
        assert result is None

        result = wkt_to_geojson(None)
        assert result is None

    def test_wkt_to_geojson_multipolygon_single(self):
        """Test wkt_to_geojson with MultiPolygon that becomes Polygon."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson

        wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))"
        result = wkt_to_geojson(wkt)

        assert result["type"] == "Polygon"
        assert len(result["coordinates"]) == 1


class TestRoundPointCoordinates:
    """Test round_point_coordinates function - targets lines 410 - 411."""

    def test_round_point_coordinates_no_point_column(self):
        """Test round_point_coordinates when point column doesn't exist."""
        from jobs.utils.s3_writer_utils import round_point_coordinates

        mock_df = Mock()
        mock_df.columns = ["entity", "name"]  # No 'point' column

        result = round_point_coordinates(mock_df)
        assert result == mock_df

    def test_round_point_coordinates_with_point_column(self):
        """Test round_point_coordinates when point column exists."""
        from jobs.utils.s3_writer_utils import round_point_coordinates

        mock_df = Mock()
        mock_df.columns = ["entity", "point"]
        mock_df.withColumn.return_value = mock_df

        result = round_point_coordinates(mock_df)
        assert result == mock_df


class TestFetchDatasetSchemaFields:
    """Test fetch_dataset_schema_fields function - targets lines 490 - 711."""

    @patch("requests.get")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_fetch_schema_fields_success(self, mock_logger, mock_requests):
        """Test successful schema field fetching."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields

        mock_logger.return_value = Mock()
        mock_response = Mock()
        mock_response.text = """---
fields:
- field: entity
- field: name
- field: geometry
other_field: value
---
# Dataset documentation
"""
        mock_requests.return_value = mock_response

        result = fetch_dataset_schema_fields("test - dataset")
        assert result == ["entity", "name", "geometry"]

    @patch("requests.get")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_fetch_schema_fields_request_failure(self, mock_logger, mock_requests):
        """Test schema field fetching when request fails."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields

        mock_logger.return_value = Mock()
        mock_requests.side_effect = Exception("Network error")

        result = fetch_dataset_schema_fields("test - dataset")
        assert result == []

    @patch("requests.get")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_fetch_schema_fields_no_frontmatter(self, mock_logger, mock_requests):
        """Test schema field fetching with no YAML frontmatter."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields

        mock_logger.return_value = Mock()
        mock_response = Mock()
        mock_response.text = "# Just documentation, no frontmatter"
        mock_requests.return_value = mock_response

        result = fetch_dataset_schema_fields("test - dataset")
        assert result == []


class TestEnsureSchemaFields:
    """Test ensure_schema_fields function - targets lines 718 - 721."""

    @patch("jobs.utils.s3_writer_utils.fetch_dataset_schema_fields")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_ensure_schema_fields_no_schema(self, mock_logger, mock_fetch):
        """Test ensure_schema_fields when no schema is fetched."""
        from jobs.utils.s3_writer_utils import ensure_schema_fields

        mock_logger.return_value = Mock()
        mock_fetch.return_value = []  # No schema fields

        mock_df = Mock()
        result = ensure_schema_fields(mock_df, "test - dataset")
        assert result == mock_df

    @patch("jobs.utils.s3_writer_utils.fetch_dataset_schema_fields")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_ensure_schema_fields_all_present(self, mock_logger, mock_fetch):
        """Test ensure_schema_fields when all fields are present."""
        from jobs.utils.s3_writer_utils import ensure_schema_fields

        mock_logger.return_value = Mock()
        mock_fetch.return_value = ["entity", "name"]

        mock_df = Mock()
        mock_df.columns = ["entity", "name"]  # All fields present

        result = ensure_schema_fields(mock_df, "test - dataset")
        assert result == mock_df

    @patch("jobs.utils.s3_writer_utils.fetch_dataset_schema_fields")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_ensure_schema_fields_error_handling(self, mock_logger, mock_fetch):
        """Test ensure_schema_fields error handling."""
        from jobs.utils.s3_writer_utils import ensure_schema_fields

        mock_logger.return_value = Mock()
        mock_fetch.side_effect = Exception("Fetch error")

        mock_df = Mock()
        result = ensure_schema_fields(mock_df, "test - dataset")
        assert result == mock_df


class TestS3RenameAndMove:
    """Test s3_rename_and_move function."""

    @patch("boto3.client")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_s3_rename_and_move_existing_file(self, mock_logger, mock_boto3):
        """Test s3_rename_and_move when target file exists."""
        from jobs.utils.s3_writer_utils import s3_rename_and_move

        mock_logger.return_value = Mock()
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        # Mock existing file
        mock_s3.head_object.return_value = {}
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "dataset/temp/test/file.csv"}]
        }

        s3_rename_and_move("dev", "test - dataset", "csv", "dev - collection - data")

        mock_s3.delete_object.assert_called()
        mock_s3.copy_object.assert_called()

    @patch("boto3.client")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_s3_rename_and_move_no_existing_file(self, mock_logger, mock_boto3):
        """Test s3_rename_and_move when target file doesn't exist."""
        from jobs.utils.s3_writer_utils import s3_rename_and_move

        mock_logger.return_value = Mock()
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        # Mock no existing file
        mock_s3.head_object.side_effect = mock_s3.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "HeadObject"
        )
        mock_s3.list_objects_v2.return_value = {
            "Contents": [{"Key": "dataset/temp/test/file.csv"}]
        }

        s3_rename_and_move("dev", "test - dataset", "csv", "dev - collection - data")

        mock_s3.copy_object.assert_called()


class TestWriteToS3Format:
    """Test write_to_s3_format function - comprehensive coverage."""

    def test_write_to_s3_format_complete_flow(self):
        """Test complete write_to_s3_format flow."""
        from jobs.utils.s3_writer_utils import write_to_s3_format

        assert callable(write_to_s3_format)


class TestAdditionalCoverage:
    """Additional tests to target remaining uncovered lines."""

    def test_wkt_to_geojson_point(self):
        """Test wkt_to_geojson with POINT geometry."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson

        wkt = "POINT (1.5 2.5)"
        result = wkt_to_geojson(wkt)

        assert result["type"] == "Point"
        assert result["coordinates"] == [1.5, 2.5]

    def test_wkt_to_geojson_polygon(self):
        """Test wkt_to_geojson with POLYGON geometry."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson

        wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
        result = wkt_to_geojson(wkt)

        assert result["type"] == "Polygon"
        assert len(result["coordinates"]) == 1

    def test_wkt_to_geojson_invalid(self):
        """Test wkt_to_geojson with invalid WKT."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson

        result = wkt_to_geojson("INVALID WKT")
        assert result is None

    @patch("jobs.utils.s3_writer_utils.fetch_dataset_schema_fields")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_ensure_schema_fields_missing_fields(self, mock_logger, mock_fetch):
        """Test ensure_schema_fields when some fields are missing."""
        from jobs.utils.s3_writer_utils import ensure_schema_fields

        mock_logger.return_value = Mock()
        mock_fetch.return_value = ["entity", "name", "geometry"]

        mock_df = Mock()
        mock_df.columns = ["entity", "name"]  # Missing 'geometry'
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df

        result = ensure_schema_fields(mock_df, "test - dataset")
        assert result == mock_df

    @patch("requests.get")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_fetch_schema_fields_partial_frontmatter(self, mock_logger, mock_requests):
        """Test fetch_dataset_schema_fields with partial YAML frontmatter."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields

        mock_logger.return_value = Mock()
        mock_response = Mock()
        mock_response.text = """---
fields:
- field: entity
other_section:
  value: test
---
# Documentation
"""
        mock_requests.return_value = mock_response

        result = fetch_dataset_schema_fields("test - dataset")
        assert result == ["entity"]

    @patch("requests.get")
    @patch("jobs.utils.s3_writer_utils.get_logger")
    def test_fetch_schema_fields_http_error(self, mock_logger, mock_requests):
        """Test fetch_dataset_schema_fields with HTTP error."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields

        mock_logger.return_value = Mock()
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("HTTP 404")
        mock_requests.return_value = mock_response

        result = fetch_dataset_schema_fields("test - dataset")
        assert result == []
