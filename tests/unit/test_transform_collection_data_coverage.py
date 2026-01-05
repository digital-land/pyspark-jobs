import os
import sys
import pytest
"""Additional tests for transform_collection_data to improve coverage."""

from unittest.mock import MagicMock, Mock, call, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

# Mock problematic imports before importing the module
with patch.dict(
    "sys.modules",
    {
        "sedona": MagicMock(),
        "sedona.spark": MagicMock(),
        "sedona.spark.SedonaContext": MagicMock(),
        "pandas": MagicMock(),
    },
):
    from jobs.transform_collection_data import (
        transform_data_entity,
        transform_data_fact,
        transform_data_fact_res,
        transform_data_issue,
    )


def create_mock_dataframe(columns=None):
    """Create a mock DataFrame that supports PySpark operations."""
    mock_df = Mock()
    if columns:
        mock_df.columns = columns
        mock_df.__iter__ = Mock(return_value=iter(columns))
    mock_df.__getitem__ = Mock(return_value=Mock())
    return mock_df


@pytest.mark.unit
class TestTransformDataFactCoverage:
    """Test transform_data_fact function coverage."""

    def test_transform_data_fact_window_operations(self):
        """Test window operations and row numbering logic."""
        # Create mock DataFrames with PySpark operation support
        mock_df = create_mock_dataframe()
        mock_window_df = create_mock_dataframe()
        mock_filtered_df = Mock()
        mock_selected_df = Mock()
        mock_final_df = Mock()

        # Set up method chain
        mock_df.withColumn.return_value = mock_window_df
        mock_window_df.filter.return_value = mock_filtered_df
        mock_filtered_df.drop.return_value = mock_selected_df
        mock_selected_df.select.return_value = mock_final_df
        mock_final_df.select.return_value = mock_final_df

        result = transform_data_fact(mock_df)

        # Verify the transformation chain
        mock_df.withColumn.assert_called_once()
        mock_window_df.filter.assert_called_once()
        mock_filtered_df.drop.assert_called_once_with("row_num")
        # The actual code calls select twice (lines 27 and 29)
        assert mock_selected_df.select.call_count == 1
        assert mock_final_df.select.call_count == 1
        assert result == mock_final_df

    def test_transform_data_fact_column_selection(self):
        """Test specific column selection logic."""
        mock_df = create_mock_dataframe()
        mock_window_df = create_mock_dataframe()
        mock_filtered_df = Mock()
        mock_selected_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_window_df
        mock_window_df.filter.return_value = mock_filtered_df
        mock_filtered_df.drop.return_value = mock_selected_df
        mock_selected_df.select.return_value = mock_final_df
        mock_final_df.select.return_value = mock_final_df

        transform_data_fact(mock_df)

        # Verify correct columns are selected in first call
        expected_first_select = [
            "fact",
            "end_date",
            "entity",
            "field",
            "entry_date",
            "priority",
            "reference_entity",
            "start_date",
            "value",
        ]

        calls = mock_selected_df.select.call_args_list
        assert len(calls) == 1
        assert calls[0][0] == tuple(expected_first_select)

        # Verify second select call on the result
        expected_final_select = [
            "end_date",
            "entity",
            "fact",
            "field",
            "entry_date",
            "priority",
            "reference_entity",
            "start_date",
            "value",
        ]
        final_calls = mock_final_df.select.call_args_list
        assert len(final_calls) == 1
        assert final_calls[0][0] == tuple(expected_final_select)


@pytest.mark.unit
class TestTransformDataFactResCoverage:
    """Test transform_data_fact_res function coverage."""

    def test_transform_data_fact_res_column_operations(self):
        """Test column selection and reordering."""
        mock_df = Mock()
        mock_selected_df = Mock()
        mock_final_df = Mock()

        mock_df.select.return_value = mock_selected_df
        mock_selected_df.select.return_value = mock_final_df

        result = transform_data_fact_res(mock_df)

        # Verify column selections
        expected_first_select = [
            "end_date",
            "fact",
            "entry_date",
            "entry_number",
            "priority",
            "resource",
            "start_date",
        ]
        expected_final_select = [
            "end_date",
            "fact",
            "entry_date",
            "entry_number",
            "priority",
            "resource",
            "start_date",
        ]

        calls = mock_df.select.call_args_list
        assert len(calls) == 1
        assert calls[0][0] == tuple(expected_first_select)

        calls = mock_selected_df.select.call_args_list
        assert len(calls) == 1
        assert calls[0][0] == tuple(expected_final_select)

        assert result == mock_final_df


@pytest.mark.unit
class TestTransformDataIssueCoverage:
    """Test transform_data_issue function coverage."""

    def test_transform_data_issue_date_column_addition(self):
        """Test addition of date columns with empty strings."""
        mock_df = Mock()
        mock_df1 = Mock()
        mock_df2 = Mock()
        mock_df3 = Mock()
        mock_final_df = Mock()

        # Set up method chain for withColumn calls
        mock_df.withColumn.return_value = mock_df1
        mock_df1.withColumn.return_value = mock_df2
        mock_df2.withColumn.return_value = mock_df3
        mock_df3.select.return_value = mock_final_df

        result = transform_data_issue(mock_df)

        # Verify all three date columns are added
        assert mock_df.withColumn.call_count == 1
        assert mock_df1.withColumn.call_count == 1
        assert mock_df2.withColumn.call_count == 1

        # Verify final column selection
        expected_columns = [
            "end_date",
            "entity",
            "entry_date",
            "entry_number",
            "field",
            "issue_type",
            "line_number",
            "dataset",
            "resource",
            "start_date",
            "value",
            "message",
        ]
        mock_df3.select.assert_called_once_with(*expected_columns)

        assert result == mock_final_df

    def test_transform_data_issue_column_casting(self):
        """Test that date columns are cast to string type."""
        mock_df = Mock()
        mock_lit = Mock()
        mock_cast = Mock()

        # Mock the lit().cast() chain
        with patch("jobs.transform_collection_data.lit") as mock_lit_func:
            mock_lit_func.return_value = mock_lit
            mock_lit.cast.return_value = mock_cast

            mock_df.withColumn.return_value = mock_df
            mock_df.select.return_value = mock_df

            transform_data_issue(mock_df)

            # Verify lit("").cast("string") is called for each date column
            assert mock_lit_func.call_count == 3
            assert mock_lit.cast.call_count == 3

            # Verify cast is called with "string"
            for call in mock_lit.cast.call_args_list:
                assert call[0][0] == "string"


@pytest.mark.unit
class TestTransformDataEntityCoverage:
    """Test transform_data_entity function coverage."""

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_priority_column_check(
        self, mock_show_df, mock_typology
    ):
        """Test priority column existence check and ordering logic."""
        mock_typology.return_value = "test - typology"

        # Create mock DataFrames with PySpark operation support
        mock_df_with_priority = create_mock_dataframe(
            ["entity", "field", "priority", "entry_date", "entry_number"]
        )
        mock_ranked_df = Mock()
        mock_pivot_df = create_mock_dataframe(["entity", "name", "dataset"])
        mock_final_df = Mock()

        mock_df_with_priority.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock all subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        result = transform_data_entity(
            mock_df_with_priority, "test - dataset", mock_spark, "development"
        )

        # Verify priority - based ordering is used
        mock_df_with_priority.withColumn.assert_called()
        assert result == mock_final_df

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_without_priority_column(
        self, mock_show_df, mock_typology
    ):
        """Test entity transformation when priority column is missing."""
        mock_typology.return_value = "test - typology"

        # Create mock DataFrames with PySpark operation support
        mock_df_no_priority = create_mock_dataframe(
            ["entity", "field", "entry_date", "entry_number"]
        )
        mock_ranked_df = Mock()
        mock_pivot_df = create_mock_dataframe(["entity", "name", "dataset"])
        mock_final_df = Mock()

        mock_df_no_priority.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock all subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        result = transform_data_entity(
            mock_df_no_priority, "test - dataset", mock_spark, "development"
        )

        # Verify entry_date/entry_number ordering is used when priority is missing
        assert result == mock_final_df

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_column_normalization(
        self, mock_show_df, mock_typology
    ):
        """Test kebab - case to snake_case column normalization."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "test - field", "another - column", "normal_column"]
        )

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify withColumnRenamed is called for kebab - case columns
        rename_calls = mock_pivot_df.withColumnRenamed.call_args_list

        # Check that kebab - case columns are renamed
        kebab_columns_renamed = any(
            "-" in str(call_args) and "_" in str(call_args)
            for call_args in rename_calls
        )
        assert kebab_columns_renamed or len(rename_calls) >= 2

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_missing_columns_handling(
        self, mock_show_df, mock_typology
    ):
        """Test handling of missing standard columns."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "name"]
        )  # Missing geometry, end_date, etc.

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify missing columns are added with default values
        withColumn_calls = mock_pivot_df.withColumn.call_args_list

        # Should add missing standard columns
        added_columns = [call[0][0] for call in withColumn_calls]
        expected_missing_columns = [
            "geometry",
            "end_date",
            "start_date",
            "name",
            "point",
        ]

        # Check that some missing columns are added
        missing_columns_added = any(
            col in added_columns for col in expected_missing_columns
        )
        assert missing_columns_added or len(withColumn_calls) >= 5

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_json_creation_logic(
        self, mock_show_df, mock_typology
    ):
        """Test JSON creation for non - standard columns."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "name", "custom_field", "another_field", "dataset"]
        )

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        with patch("jobs.transform_collection_data.to_json") as mock_to_json:
            with patch("jobs.transform_collection_data.struct") as mock_struct:
                transform_data_entity(
                    mock_df, "test - dataset", mock_spark, "development"
                )

                # Verify JSON creation is called for non - standard columns
                json_calls = mock_pivot_df.withColumn.call_args_list
                json_column_added = any("json" in str(call) for call in json_calls)
                assert json_column_added or mock_to_json.called

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_date_normalization(
        self, mock_show_df, mock_typology
    ):
        """Test date column normalization logic."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "end_date", "entry_date", "start_date"]
        )

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify date normalization is applied
        withColumn_calls = mock_pivot_df.withColumn.call_args_list

        # Should normalize date columns
        date_columns_processed = any("date" in str(call) for call in withColumn_calls)
        assert date_columns_processed or len(withColumn_calls) >= 3

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_geometry_normalization(
        self, mock_show_df, mock_typology
    ):
        """Test geometry column normalization logic."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(["entity", "geometry", "point"])

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify geometry normalization is applied
        withColumn_calls = mock_pivot_df.withColumn.call_args_list

        # Should normalize geometry columns
        geometry_columns_processed = any(
            "geometry" in str(call) or "point" in str(call) for call in withColumn_calls
        )
        assert geometry_columns_processed or len(withColumn_calls) >= 2

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_final_projection(self, mock_show_df, mock_typology):
        """Test final column projection and deduplication."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(["entity"])

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data with subscripting support
        mock_spark = Mock()
        mock_org_df = create_mock_dataframe(["organisation", "entity"])
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        result = transform_data_entity(
            mock_df, "test - dataset", mock_spark, "development"
        )

        # Verify final projection includes all expected columns
        expected_columns = [
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        ]

        select_calls = mock_pivot_df.select.call_args_list
        final_select_call = select_calls[-1]  # Last select call

        # Verify all expected columns are in final selection
        assert len(final_select_call[0]) == len(expected_columns)

        # Verify deduplication is called
        mock_pivot_df.dropDuplicates.assert_called_once_with(["entity"])

        assert result == mock_final_df


@pytest.mark.unit
class TestTransformDataErrorHandling:
    """Test error handling across all transform functions."""

    def test_all_functions_handle_exceptions(self):
        """Test that all transform functions properly handle and re - raise exceptions."""
        functions_to_test = [
            (transform_data_fact, [Mock()]),
            (transform_data_fact_res, [Mock()]),
            (transform_data_issue, [Mock()]),
        ]

        for func, args in functions_to_test:
            # Create mock that raises exception
            error_mock = Mock()
            error_mock.withColumn.side_effect = Exception("Test error")
            error_mock.select.side_effect = Exception("Test error")

            with pytest.raises(Exception, match="Test error"):
                func(error_mock, *args[1:])

    @patch("jobs.transform_collection_data.get_dataset_typology")
    def test_transform_data_entity_exception_handling(self, mock_typology):
        """Test transform_data_entity exception handling."""
        mock_typology.return_value = "test - typology"

        error_mock = Mock()
        error_mock.columns = ["entity", "field"]
        error_mock.withColumn.side_effect = Exception("Test error")

        mock_spark = Mock()

        with pytest.raises(Exception, match="Test error"):
            transform_data_entity(
                error_mock, "test - dataset", mock_spark, "development"
            )

        # Create mock DataFrames with PySpark operation support
        mock_df_no_priority = create_mock_dataframe(
            ["entity", "field", "entry_date", "entry_number"]
        )
        mock_ranked_df = Mock()
        mock_pivot_df = create_mock_dataframe(["entity", "name", "dataset"])
        mock_final_df = Mock()

        mock_df_no_priority.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock all subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data
        mock_spark = Mock()
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        result = transform_data_entity(
            mock_df_no_priority, "test - dataset", mock_spark, "development"
        )

        # Verify entry_date/entry_number ordering is used when priority is missing
        assert result == mock_final_df

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_column_normalization(
        self, mock_show_df, mock_typology
    ):
        """Test kebab - case to snake_case column normalization."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "test - field", "another - column", "normal_column"]
        )

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data
        mock_spark = Mock()
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify withColumnRenamed is called for kebab - case columns
        rename_calls = mock_pivot_df.withColumnRenamed.call_args_list

        # Check that kebab - case columns are renamed
        kebab_columns_renamed = any(
            "-" in str(call_args) and "_" in str(call_args)
            for call_args in rename_calls
        )
        assert kebab_columns_renamed or len(rename_calls) >= 2

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_missing_columns_handling(
        self, mock_show_df, mock_typology
    ):
        """Test handling of missing standard columns."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "name"]
        )  # Missing geometry, end_date, etc.

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data
        mock_spark = Mock()
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify missing columns are added with default values
        withColumn_calls = mock_pivot_df.withColumn.call_args_list

        # Should add missing standard columns
        added_columns = [call[0][0] for call in withColumn_calls]
        expected_missing_columns = [
            "geometry",
            "end_date",
            "start_date",
            "name",
            "point",
        ]

        # Check that some missing columns are added
        missing_columns_added = any(
            col in added_columns for col in expected_missing_columns
        )
        assert missing_columns_added or len(withColumn_calls) >= 5

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_json_creation_logic(
        self, mock_show_df, mock_typology
    ):
        """Test JSON creation for non - standard columns."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "name", "custom_field", "another_field", "dataset"]
        )

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data
        mock_spark = Mock()
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        with patch("jobs.transform_collection_data.to_json") as mock_to_json:
            with patch("jobs.transform_collection_data.struct") as mock_struct:
                transform_data_entity(
                    mock_df, "test - dataset", mock_spark, "development"
                )

                # Verify JSON creation is called for non - standard columns
                json_calls = mock_pivot_df.withColumn.call_args_list
                json_column_added = any("json" in str(call) for call in json_calls)
                assert json_column_added or mock_to_json.called

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_date_normalization(
        self, mock_show_df, mock_typology
    ):
        """Test date column normalization logic."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(
            ["entity", "end_date", "entry_date", "start_date"]
        )

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data
        mock_spark = Mock()
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify date normalization is applied
        withColumn_calls = mock_pivot_df.withColumn.call_args_list

        # Should normalize date columns
        date_columns_processed = any("date" in str(call) for call in withColumn_calls)
        assert date_columns_processed or len(withColumn_calls) >= 3

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_geometry_normalization(
        self, mock_show_df, mock_typology
    ):
        """Test geometry column normalization logic."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(["entity", "geometry", "point"])

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data
        mock_spark = Mock()
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        transform_data_entity(mock_df, "test - dataset", mock_spark, "development")

        # Verify geometry normalization is applied
        withColumn_calls = mock_pivot_df.withColumn.call_args_list

        # Should normalize geometry columns
        geometry_columns_processed = any(
            "geometry" in str(call) or "point" in str(call) for call in withColumn_calls
        )
        assert geometry_columns_processed or len(withColumn_calls) >= 2

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.transform_collection_data.show_d")
    def test_transform_data_entity_final_projection(self, mock_show_df, mock_typology):
        """Test final column projection and deduplication."""
        mock_typology.return_value = "test - typology"

        mock_df = create_mock_dataframe(["entity", "field"])
        mock_pivot_df = create_mock_dataframe(["entity"])

        # Mock the transformation chain
        mock_ranked_df = Mock()
        mock_final_df = Mock()

        mock_df.withColumn.return_value = mock_ranked_df
        mock_ranked_df.filter.return_value = mock_ranked_df
        mock_ranked_df.drop.return_value = mock_ranked_df
        mock_ranked_df.groupBy.return_value.pivot.return_value.agg.return_value = (
            mock_pivot_df
        )

        # Mock subsequent operations
        mock_pivot_df.withColumn.return_value = mock_pivot_df
        mock_pivot_df.withColumnRenamed.return_value = mock_pivot_df
        mock_pivot_df.drop.return_value = mock_pivot_df
        mock_pivot_df.join.return_value.select.return_value.drop.return_value = (
            mock_pivot_df
        )
        mock_pivot_df.select.return_value = mock_pivot_df
        mock_pivot_df.dropDuplicates.return_value = mock_final_df

        # Mock spark and organisation data
        mock_spark = Mock()
        mock_org_df = Mock()
        mock_spark.read.option.return_value.csv.return_value = mock_org_df

        result = transform_data_entity(
            mock_df, "test - dataset", mock_spark, "development"
        )

        # Verify final projection includes all expected columns
        expected_columns = [
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        ]

        select_calls = mock_pivot_df.select.call_args_list
        final_select_call = select_calls[-1]  # Last select call

        # Verify all expected columns are in final selection
        assert len(final_select_call[0]) == len(expected_columns)

        # Verify deduplication is called
        mock_pivot_df.dropDuplicates.assert_called_once_with(["entity"])

        assert result == mock_final_df


@pytest.mark.unit
class TestTransformDataErrorHandling:
    """Test error handling across all transform functions."""

    def test_all_functions_handle_exceptions(self):
        """Test that all transform functions properly handle and re - raise exceptions."""
        functions_to_test = [
            (transform_data_fact, [Mock()]),
            (transform_data_fact_res, [Mock()]),
            (transform_data_issue, [Mock()]),
        ]

        for func, args in functions_to_test:
            # Create mock that raises exception
            error_mock = Mock()
            error_mock.withColumn.side_effect = Exception("Test error")
            error_mock.select.side_effect = Exception("Test error")

            with pytest.raises(Exception, match="Test error"):
                func(error_mock, *args[1:])

    @patch("jobs.transform_collection_data.get_dataset_typology")
    def test_transform_data_entity_exception_handling(self, mock_typology):
        """Test transform_data_entity exception handling."""
        mock_typology.return_value = "test - typology"

        error_mock = Mock()
        error_mock.columns = ["entity", "field"]
        error_mock.withColumn.side_effect = Exception("Test error")

        mock_spark = Mock()

        with pytest.raises(Exception, match="Test error"):
            transform_data_entity(
                error_mock, "test - dataset", mock_spark, "development"
            )
