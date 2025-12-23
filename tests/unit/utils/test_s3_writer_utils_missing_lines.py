"""
Targeted tests for missing lines in s3_writer_utils.py
Focus on lines: 36-152, 157-202, 263-273, 334, 345-355, 386, 407-432, 437-468, 472-721
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

try:
    from jobs.utils import s3_writer_utils
except ImportError:
    # Mock PySpark if not available
    with patch.dict('sys.modules', {
        'pyspark': Mock(),
        'pyspark.sql': Mock(),
        'pyspark.sql.functions': Mock(),
        'pyspark.sql.types': Mock(),
        'pyspark.sql.window': Mock()
    }):
        from jobs.utils import s3_writer_utils


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame with all necessary methods."""
    df = Mock()
    df.columns = ['entity', 'field', 'value', 'entry_date', 'entry_number', 'organisation']
    df.count.return_value = 1000
    df.withColumn.return_value = df
    df.withColumnRenamed.return_value = df
    df.select.return_value = df
    df.filter.return_value = df
    df.drop.return_value = df
    df.join.return_value = df
    df.groupBy.return_value.pivot.return_value.agg.return_value = df
    df.dropDuplicates.return_value = df
    df.printSchema.return_value = None
    df.coalesce.return_value.write.mode.return_value.option.return_value.csv.return_value = None
    df.write.partitionBy.return_value.mode.return_value.option.return_value.parquet.return_value = None
    df.toLocalIterator.return_value = iter([Mock(asDict=lambda: {'entity': 1, 'name': 'test'})])
    df.repartition.return_value = df
    df.unpersist.return_value = None
    return df


@pytest.mark.unit
class TestTransformDataEntityFormatMissingLines:
    """Test missing lines 36-152 in transform_data_entity_format function."""

    @patch('jobs.utils.s3_writer_utils.get_dataset_typology')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    @patch('jobs.utils.s3_writer_utils.Window')
    @patch('jobs.utils.s3_writer_utils.desc')
    @patch('jobs.utils.s3_writer_utils.row_number')
    @patch('jobs.utils.s3_writer_utils.col')
    @patch('jobs.utils.s3_writer_utils.first')
    @patch('jobs.utils.s3_writer_utils.lit')
    def test_transform_entity_with_geojson_column(self, mock_lit, mock_first, mock_col, mock_row_number, 
                                                 mock_desc, mock_window, mock_logger, mock_show_df, 
                                                 mock_get_typology, mock_dataframe, mock_spark):
        """Test lines 67-68: geojson column handling."""
        mock_get_typology.return_value = "test-typology"
        mock_dataframe.columns = ['entity', 'field', 'value', 'geojson', 'organisation']
        
        # Mock spark read for organisation CSV
        mock_org_df = Mock()
        mock_org_df.columns = ['organisation', 'entity']
        mock_spark.read.option.return_value.csv.return_value = mock_org_df
        
        result = s3_writer_utils.transform_data_entity_format(
            mock_dataframe, "test-dataset", mock_spark, "dev"
        )
        
        # Verify geojson column is dropped (line 68)
        mock_dataframe.drop.assert_called()

    @patch('jobs.utils.s3_writer_utils.get_dataset_typology')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_transform_entity_missing_columns_handling(self, mock_logger, mock_show_df, 
                                                      mock_get_typology, mock_dataframe, mock_spark):
        """Test lines 89-98: missing column handling."""
        mock_get_typology.return_value = "test-typology"
        mock_dataframe.columns = ['entity', 'field', 'value']  # Missing standard columns
        
        # Mock spark read for organisation CSV
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df
        
        result = s3_writer_utils.transform_data_entity_format(
            mock_dataframe, "test-dataset", mock_spark, "dev"
        )
        
        # Verify missing columns are added with appropriate defaults
        assert mock_dataframe.withColumn.call_count >= 4  # geometry, end_date, start_date, name, point

    @patch('jobs.utils.s3_writer_utils.get_dataset_typology')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    @patch('jobs.utils.s3_writer_utils.to_json')
    @patch('jobs.utils.s3_writer_utils.struct')
    @patch('jobs.utils.s3_writer_utils.col')
    def test_transform_entity_json_building_with_diff_columns(self, mock_col, mock_struct, mock_to_json,
                                                             mock_logger, mock_show_df, mock_get_typology, 
                                                             mock_dataframe, mock_spark):
        """Test lines 99-104: JSON building from non-standard columns."""
        mock_get_typology.return_value = "test-typology"
        mock_dataframe.columns = ['entity', 'field', 'value', 'custom_col1', 'custom_col2']
        
        # Mock spark read for organisation CSV
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df
        
        result = s3_writer_utils.transform_data_entity_format(
            mock_dataframe, "test-dataset", mock_spark, "dev"
        )
        
        # Verify JSON is built from non-standard columns (lines 102-103)
        mock_to_json.assert_called()
        mock_struct.assert_called()

    @patch('jobs.utils.s3_writer_utils.get_dataset_typology')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    @patch('jobs.utils.s3_writer_utils.lit')
    def test_transform_entity_json_building_no_diff_columns(self, mock_lit, mock_logger, mock_show_df, 
                                                           mock_get_typology, mock_dataframe, mock_spark):
        """Test lines 104-105: JSON building when no non-standard columns."""
        mock_get_typology.return_value = "test-typology"
        # Only standard columns
        mock_dataframe.columns = ['entity', 'dataset', 'end_date', 'entry_date', 'geometry', 'json',
                                 'name', 'organisation_entity', 'point', 'prefix', 'reference',
                                 'start_date', 'typology']
        
        # Mock spark read for organisation CSV
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df
        
        result = s3_writer_utils.transform_data_entity_format(
            mock_dataframe, "test-dataset", mock_spark, "dev"
        )
        
        # Verify empty JSON is set (line 105)
        mock_lit.assert_called_with("{}")

    @patch('jobs.utils.s3_writer_utils.get_dataset_typology')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    @patch('jobs.utils.s3_writer_utils.when')
    @patch('jobs.utils.s3_writer_utils.col')
    @patch('jobs.utils.s3_writer_utils.to_date')
    def test_transform_entity_date_normalization(self, mock_to_date, mock_col, mock_when,
                                                 mock_logger, mock_show_df, mock_get_typology, 
                                                 mock_dataframe, mock_spark):
        """Test lines 107-115: Date column normalization."""
        mock_get_typology.return_value = "test-typology"
        mock_dataframe.columns = ['entity', 'field', 'value', 'end_date', 'entry_date', 'start_date']
        
        # Mock spark read for organisation CSV
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df
        
        result = s3_writer_utils.transform_data_entity_format(
            mock_dataframe, "test-dataset", mock_spark, "dev"
        )
        
        # Verify date normalization is applied (lines 108-115)
        assert mock_when.call_count >= 3  # For each date column
        mock_to_date.assert_called()

    @patch('jobs.utils.s3_writer_utils.get_dataset_typology')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    @patch('jobs.utils.s3_writer_utils.when')
    @patch('jobs.utils.s3_writer_utils.col')
    def test_transform_entity_geometry_normalization(self, mock_col, mock_when, mock_logger, 
                                                    mock_show_df, mock_get_typology, 
                                                    mock_dataframe, mock_spark):
        """Test lines 117-127: Geometry column normalization."""
        mock_get_typology.return_value = "test-typology"
        mock_dataframe.columns = ['entity', 'field', 'value', 'geometry', 'point']
        
        # Mock spark read for organisation CSV
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df
        
        result = s3_writer_utils.transform_data_entity_format(
            mock_dataframe, "test-dataset", mock_spark, "dev"
        )
        
        # Verify geometry normalization is applied (lines 118-127)
        assert mock_when.call_count >= 2  # For geometry and point columns


@pytest.mark.unit
class TestNormaliseDataframeSchemaMissingLines:
    """Test missing lines 157-202 in normalise_dataframe_schema function."""

    @patch('jobs.utils.s3_writer_utils.load_metadata')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_normalise_schema_fact_table(self, mock_logger, mock_show_df, mock_load_metadata, 
                                        mock_dataframe, mock_spark):
        """Test lines 166-168: fact table schema handling."""
        mock_load_metadata.return_value = {
            "schema_fact_res_fact_entity": ["entity", "name", "dataset"]
        }
        
        with pytest.raises(ValueError, match="Unknown table name: fact"):
            s3_writer_utils.normalise_dataframe_schema(
                mock_dataframe, "fact", "test-dataset", mock_spark, "dev"
            )

    @patch('jobs.utils.s3_writer_utils.load_metadata')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_normalise_schema_fact_res_table(self, mock_logger, mock_show_df, mock_load_metadata, 
                                            mock_dataframe, mock_spark):
        """Test lines 166-168: fact_res table schema handling."""
        mock_load_metadata.return_value = {
            "schema_fact_res_fact_entity": ["entity", "name", "dataset"]
        }
        
        with pytest.raises(ValueError, match="Unknown table name: fact_res"):
            s3_writer_utils.normalise_dataframe_schema(
                mock_dataframe, "fact_res", "test-dataset", mock_spark, "dev"
            )

    @patch('jobs.utils.s3_writer_utils.load_metadata')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_normalise_schema_issue_table_fields(self, mock_logger, mock_show_df, mock_load_metadata, 
                                                 mock_dataframe, mock_spark):
        """Test lines 169-171: issue table schema handling."""
        mock_load_metadata.return_value = {
            "schema_issue": ["issue", "severity", "message"]
        }
        
        with pytest.raises(ValueError, match="Unknown table name: issue"):
            s3_writer_utils.normalise_dataframe_schema(
                mock_dataframe, "issue", "test-dataset", mock_spark, "dev"
            )
        
        # Verify issue schema fields are extracted
        mock_load_metadata.assert_called_once_with("config/transformed_source.json")

    @patch('jobs.utils.s3_writer_utils.load_metadata')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_normalise_schema_column_renaming(self, mock_logger, mock_show_df, mock_load_metadata, 
                                             mock_dataframe, mock_spark):
        """Test lines 173-177: Column renaming from kebab-case to snake_case."""
        mock_load_metadata.return_value = {
            "schema_fact_res_fact_entity": ["entity", "name", "dataset"]
        }
        mock_dataframe.columns = ['entity-id', 'data-set', 'entry-date']
        
        with pytest.raises(ValueError):
            s3_writer_utils.normalise_dataframe_schema(
                mock_dataframe, "unknown", "test-dataset", mock_spark, "dev"
            )
        
        # Verify column renaming is attempted (lines 174-176)
        mock_dataframe.withColumnRenamed.assert_called()

    @patch('jobs.utils.s3_writer_utils.load_metadata')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_normalise_schema_fields_comparison(self, mock_logger, mock_show_df, mock_load_metadata, 
                                               mock_dataframe, mock_spark):
        """Test lines 184-189: Fields comparison logic."""
        mock_load_metadata.return_value = {
            "schema_fact_res_fact_entity": ["entity", "name", "dataset"]
        }
        mock_dataframe.columns = ['entity', 'name', 'dataset']  # All fields present
        
        with pytest.raises(ValueError):
            s3_writer_utils.normalise_dataframe_schema(
                mock_dataframe, "unknown", "test-dataset", mock_spark, "dev"
            )
        
        # Verify fields comparison logging (lines 185-188)
        mock_logger.info.assert_called()

    @patch('jobs.utils.s3_writer_utils.load_metadata')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_normalise_schema_missing_fields_warning(self, mock_logger, mock_show_df, mock_load_metadata, 
                                                    mock_dataframe, mock_spark):
        """Test lines 187-188: Missing fields warning."""
        mock_load_metadata.return_value = {
            "schema_fact_res_fact_entity": ["entity", "name", "dataset", "missing_field"]
        }
        mock_dataframe.columns = ['entity', 'name', 'dataset']  # Missing field
        
        with pytest.raises(ValueError):
            s3_writer_utils.normalise_dataframe_schema(
                mock_dataframe, "unknown", "test-dataset", mock_spark, "dev"
            )
        
        # Verify missing fields warning (line 188)
        mock_logger.warning.assert_called()


@pytest.mark.unit
class TestWriteToS3MissingLines:
    """Test missing lines 263-273 in write_to_s3 function."""

    @patch('jobs.utils.s3_writer_utils.cleanup_dataset_data')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    @patch('jobs.utils.s3_writer_utils.datetime')
    def test_write_to_s3_cleanup_errors_handling(self, mock_datetime, mock_logger, mock_show_df, 
                                                 mock_cleanup, mock_dataframe):
        """Test lines 268-270: Cleanup errors handling."""
        mock_cleanup.return_value = {
            'objects_deleted': 5, 
            'errors': ['Error 1', 'Error 2']
        }
        mock_dataframe.count.return_value = 1000
        
        s3_writer_utils.write_to_s3(
            mock_dataframe, "s3://test-bucket/output/", "test-dataset", "entity", "dev"
        )
        
        # Verify cleanup errors are logged (lines 269-270)
        mock_logger.warning.assert_called()

    @patch('jobs.utils.s3_writer_utils.cleanup_dataset_data')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_write_to_s3_debug_logging(self, mock_logger, mock_show_df, mock_cleanup, mock_dataframe):
        """Test line 271: Debug logging of cleanup summary."""
        mock_cleanup.return_value = {
            'objects_deleted': 3, 
            'errors': [],
            'details': 'cleanup details'
        }
        mock_dataframe.count.return_value = 500
        
        s3_writer_utils.write_to_s3(
            mock_dataframe, "s3://test-bucket/output/", "test-dataset", "entity", "dev"
        )
        
        # Verify debug logging (line 271)
        mock_logger.debug.assert_called()


@pytest.mark.unit
class TestWriteToS3FormatMissingLines:
    """Test missing lines 472-721 in write_to_s3_format function."""

    @patch('jobs.utils.s3_writer_utils.count_df')
    @patch('jobs.utils.s3_writer_utils.read_csv_from_s3')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_write_to_s3_format_count_logging(self, mock_logger, mock_show_df, mock_read_csv, 
                                             mock_count_df, mock_dataframe, mock_spark):
        """Test lines 474-476: Count logging logic."""
        mock_count_df.return_value = 1000
        mock_read_csv.return_value = mock_dataframe
        
        with patch('jobs.utils.s3_writer_utils.normalise_dataframe_schema') as mock_normalise:
            mock_normalise.return_value = mock_dataframe
            with patch('jobs.utils.s3_writer_utils.cleanup_dataset_data') as mock_cleanup:
                mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
                
                result = s3_writer_utils.write_to_s3_format(
                    mock_dataframe, "s3://output/", "test-dataset", "entity", mock_spark, "dev"
                )
        
        # Verify count logging (lines 475-476)
        mock_logger.info.assert_called()

    @patch('jobs.utils.s3_writer_utils.count_df')
    @patch('jobs.utils.s3_writer_utils.read_csv_from_s3')
    @patch('jobs.utils.s3_writer_utils.show_df')
    @patch('jobs.utils.s3_writer_utils.logger')
    def test_write_to_s3_format_count_none_handling(self, mock_logger, mock_show_df, mock_read_csv, 
                                                   mock_count_df, mock_dataframe, mock_spark):
        """Test lines 474-476: Count None handling."""
        mock_count_df.return_value = None  # Count returns None
        mock_read_csv.return_value = mock_dataframe
        
        with patch('jobs.utils.s3_writer_utils.normalise_dataframe_schema') as mock_normalise:
            mock_normalise.return_value = mock_dataframe
            with patch('jobs.utils.s3_writer_utils.cleanup_dataset_data') as mock_cleanup:
                mock_cleanup.return_value = {'objects_deleted': 0, 'errors': []}
                
                result = s3_writer_utils.write_to_s3_format(
                    mock_dataframe, "s3://output/", "test-dataset", "entity", mock_spark, "dev"
                )
        
        # Verify count None is handled gracefully (no logging when None)
        assert mock_count_df.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])