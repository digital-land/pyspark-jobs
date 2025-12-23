"""
Unit tests for geometry_utils module.
Tests geometry operations without requiring Sedona dependencies.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

# Mock Sedona dependencies before import
with patch.dict('sys.modules', {
    'sedona': Mock(),
    'sedona.spark': Mock(),
    'sedona.spark.SedonaContext': Mock()
}):
    from jobs.utils import geometry_utils


@pytest.fixture
def mock_dataframe():
    """Create mock DataFrame."""
    df = Mock()
    df.columns = ['geometry', 'other_col']
    df.sparkSession = Mock()
    return df


@pytest.fixture
def mock_spark_session():
    """Create mock SparkSession."""
    session = Mock()
    session.sql.return_value = Mock()
    return session


@pytest.mark.unit
class TestCalculateCentroid:
    """Test calculate_centroid function."""

    @patch('jobs.utils.geometry_utils.SedonaContext')
    def test_calculate_centroid_basic(self, mock_sedona_context, mock_dataframe):
        """Test basic centroid calculation."""
        mock_dataframe.drop.return_value = mock_dataframe
        mock_dataframe.createOrReplaceTempView.return_value = None
        
        result = geometry_utils.calculate_centroid(mock_dataframe)
        
        # Verify Sedona context creation
        mock_sedona_context.create.assert_called_once_with(mock_dataframe.sparkSession)
        
        # Verify temp view creation
        mock_dataframe.createOrReplaceTempView.assert_called_once_with("temp_geometry")
        
        # Verify SQL execution
        mock_dataframe.sparkSession.sql.assert_called_once()
        assert result is not None

    @patch('jobs.utils.geometry_utils.SedonaContext')
    def test_calculate_centroid_no_point_column(self, mock_sedona_context, mock_dataframe):
        """Test centroid calculation when no point column exists."""
        mock_dataframe.columns = ['geometry', 'other_col']  # No point column
        mock_dataframe.createOrReplaceTempView.return_value = None
        
        result = geometry_utils.calculate_centroid(mock_dataframe)
        
        # Should not call drop since no point column exists
        mock_dataframe.drop.assert_not_called()
        
        # Should still create temp view and execute SQL
        mock_dataframe.createOrReplaceTempView.assert_called_once()
        mock_dataframe.sparkSession.sql.assert_called_once()

    @patch('jobs.utils.geometry_utils.SedonaContext')
    def test_calculate_centroid_with_existing_point_column(self, mock_sedona_context, mock_dataframe):
        """Test centroid calculation when point column already exists."""
        mock_dataframe.columns = ['geometry', 'point', 'other_col']
        mock_dataframe.drop.return_value = mock_dataframe
        
        result = geometry_utils.calculate_centroid(mock_dataframe)
        
        # Should drop existing point column
        mock_dataframe.drop.assert_called_once_with('point')
        
        # Should create temp view and execute SQL
        mock_dataframe.createOrReplaceTempView.assert_called_once()
        mock_dataframe.sparkSession.sql.assert_called_once()

    @patch('jobs.utils.geometry_utils.SedonaContext')
    def test_calculate_centroid_sql_query_content(self, mock_sedona_context, mock_dataframe):
        """Test that SQL query contains expected spatial functions."""
        mock_dataframe.drop.return_value = mock_dataframe
        
        geometry_utils.calculate_centroid(mock_dataframe)
        
        # Get the SQL query that was executed
        sql_call = mock_dataframe.sparkSession.sql.call_args[0][0]
        
        # Verify SQL contains expected spatial functions
        assert 'ST_AsText' in sql_call
        assert 'ST_SetSRID' in sql_call
        assert 'ST_Point' in sql_call
        assert 'ST_X' in sql_call
        assert 'ST_Y' in sql_call
        assert 'ST_Centroid' in sql_call
        assert 'ST_GeomFromWKT' in sql_call
        assert 'ROUND' in sql_call
        assert '4326' in sql_call  # SRID
        assert 'temp_geometry' in sql_call


@pytest.mark.unit
class TestSedonaUnitTest:
    """Test sedona_unit_test function."""

    @patch('jobs.utils.geometry_utils.SedonaContext')
    @patch('jobs.utils.geometry_utils.SparkSession')
    def test_sedona_unit_test_basic(self, mock_spark_session_class, mock_sedona_context):
        """Test basic sedona unit test execution."""
        # Setup mocks
        mock_spark = Mock()
        mock_spark_session_class.builder.getOrCreate.return_value = mock_spark
        
        mock_sedona = Mock()
        mock_sedona_context.create.return_value = mock_sedona
        
        # Mock SQL results
        mock_point_result = Mock()
        mock_point_result.collect.return_value = [Mock()]
        mock_sedona.sql.return_value = mock_point_result
        
        # Mock analysis results
        mock_analysis_result = Mock()
        mock_row = {
            'area': 100.0,
            'perimeter': 40.0,
            'distance_between_points': 2.828,
            'centroid_x': 5.0,
            'centroid_y': 5.0
        }
        mock_analysis_result.collect.return_value = [mock_row]
        
        # Configure sedona.sql to return different results for different queries
        def sql_side_effect(query):
            if 'ST_Point(0,0)' in query:
                return mock_point_result
            else:
                return mock_analysis_result
        
        mock_sedona.sql.side_effect = sql_side_effect
        
        # Execute function
        geometry_utils.sedona_unit_test()
        
        # Verify SparkSession creation
        mock_spark_session_class.builder.getOrCreate.assert_called_once()
        
        # Verify Sedona context creation
        mock_sedona_context.create.assert_called_once_with(mock_spark)
        
        # Verify SQL calls
        assert mock_sedona.sql.call_count == 2

    @patch('jobs.utils.geometry_utils.SedonaContext')
    @patch('jobs.utils.geometry_utils.SparkSession')
    def test_sedona_unit_test_sql_queries(self, mock_spark_session_class, mock_sedona_context):
        """Test that sedona unit test executes expected SQL queries."""
        # Setup mocks
        mock_spark = Mock()
        mock_spark_session_class.builder.getOrCreate.return_value = mock_spark
        
        mock_sedona = Mock()
        mock_sedona_context.create.return_value = mock_sedona
        
        # Mock results
        mock_result = Mock()
        mock_result.collect.return_value = [Mock()]
        mock_sedona.sql.return_value = mock_result
        
        # Mock analysis results with proper structure
        mock_analysis_result = Mock()
        mock_row = {
            'area': 100.0,
            'perimeter': 40.0,
            'distance_between_points': 2.828,
            'centroid_x': 5.0,
            'centroid_y': 5.0
        }
        mock_analysis_result.collect.return_value = [mock_row]
        
        def sql_side_effect(query):
            if 'ST_Point(0,0)' in query:
                return mock_result
            else:
                return mock_analysis_result
        
        mock_sedona.sql.side_effect = sql_side_effect
        
        # Execute function
        geometry_utils.sedona_unit_test()
        
        # Verify SQL queries contain expected spatial functions
        sql_calls = [call[0][0] for call in mock_sedona.sql.call_args_list]
        
        # First call should be the fail-fast check
        assert any('ST_Point(0,0)' in call for call in sql_calls)
        
        # Second call should be the detailed analysis
        analysis_query = next(call for call in sql_calls if 'ST_Area' in call)
        assert 'ST_Area' in analysis_query
        assert 'ST_Perimeter' in analysis_query
        assert 'ST_Distance' in analysis_query
        assert 'ST_Centroid' in analysis_query
        assert 'ST_GeomFromWKT' in analysis_query
        assert 'POLYGON' in analysis_query
        assert 'POINT' in analysis_query

    @patch('jobs.utils.geometry_utils.SedonaContext')
    @patch('jobs.utils.geometry_utils.SparkSession')
    @patch('builtins.print')
    def test_sedona_unit_test_output_formatting(self, mock_print, mock_spark_session_class, mock_sedona_context):
        """Test that sedona unit test produces formatted output."""
        # Setup mocks
        mock_spark = Mock()
        mock_spark_session_class.builder.getOrCreate.return_value = mock_spark
        
        mock_sedona = Mock()
        mock_sedona_context.create.return_value = mock_sedona
        
        # Mock results
        mock_result = Mock()
        mock_result.collect.return_value = [Mock()]
        
        mock_analysis_result = Mock()
        mock_row = {
            'area': 100.0,
            'perimeter': 40.0,
            'distance_between_points': 2.828,
            'centroid_x': 5.0,
            'centroid_y': 5.0
        }
        mock_analysis_result.collect.return_value = [mock_row]
        
        def sql_side_effect(query):
            if 'ST_Point(0,0)' in query:
                return mock_result
            else:
                return mock_analysis_result
        
        mock_sedona.sql.side_effect = sql_side_effect
        
        # Execute function
        geometry_utils.sedona_unit_test()
        
        # Verify print statements were called with expected content
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        
        # Check for expected output strings
        assert any('Spatial Analysis Results:' in call for call in print_calls)
        assert any('sedons test starts' in call for call in print_calls)
        assert any('sedona test ends' in call for call in print_calls)
        assert any('Area of polygon: 100.0' in call for call in print_calls)
        assert any('Perimeter of polygon: 40.0' in call for call in print_calls)
        assert any('Distance between points' in call for call in print_calls)
        assert any('Polygon centroid: (5.0, 5.0)' in call for call in print_calls)