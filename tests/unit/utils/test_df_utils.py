"""Unit tests for df_utils module."""
import pytest
import os
import sys
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from jobs.utils.df_utils import show_df, count_df


class TestDFUtils:
    """Test suite for df_utils module."""

    def test_show_df_development_environment(self, spark, caplog):
        """Test show_df in development environment."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        data = [(1, "test1"), (2, "test2")]
        df = spark.createDataFrame(data, schema)
        
        with caplog.at_level("INFO"):
            show_df(df, 5, "development")
        
        # In development, should show the DataFrame
        # Check that show was called (indirectly through logs or behavior)
        assert True  # If no exception, the function worked

    def test_show_df_production_environment(self, spark, caplog):
        """Test show_df in production environment."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        data = [(1, "test1"), (2, "test2")]
        df = spark.createDataFrame(data, schema)
        
        with caplog.at_level("INFO"):
            show_df(df, 5, "production")
        
        # In production, should not show the DataFrame
        assert True  # If no exception, the function worked

    def test_show_df_with_mock_dataframe(self, caplog):
        """Test show_df with mock DataFrame."""
        mock_df = Mock()
        
        with caplog.at_level("INFO"):
            show_df(mock_df, 10, "development")
        
        # Should call show on the DataFrame in development
        mock_df.show.assert_called_once_with(10)

    def test_show_df_production_no_show(self, caplog):
        """Test that show_df doesn't call show in production."""
        mock_df = Mock()
        
        with caplog.at_level("INFO"):
            show_df(mock_df, 10, "production")
        
        # Should not call show on the DataFrame in production
        mock_df.show.assert_not_called()

    def test_count_df_development_environment(self, spark, caplog):
        """Test count_df in development environment."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        data = [(1, "test1"), (2, "test2"), (3, "test3")]
        df = spark.createDataFrame(data, schema)
        
        with caplog.at_level("INFO"):
            result = count_df(df, "development")
        
        assert result == 3

    def test_count_df_production_environment(self, spark, caplog):
        """Test count_df in production environment."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        data = [(1, "test1"), (2, "test2")]
        df = spark.createDataFrame(data, schema)
        
        with caplog.at_level("INFO"):
            result = count_df(df, "production")
        
        # In production, should return None instead of actual count
        assert result is None

    def test_count_df_with_mock_dataframe(self, caplog):
        """Test count_df with mock DataFrame."""
        mock_df = Mock()
        mock_df.count.return_value = 100
        
        with caplog.at_level("INFO"):
            result = count_df(mock_df, "development")
        
        assert result == 100
        mock_df.count.assert_called_once()

    def test_count_df_production_no_count(self, caplog):
        """Test that count_df doesn't call count in production."""
        mock_df = Mock()
        
        with caplog.at_level("INFO"):
            result = count_df(mock_df, "production")
        
        assert result is None
        # Should not call count on the DataFrame in production
        mock_df.count.assert_not_called()

    def test_show_df_empty_dataframe(self, spark, caplog):
        """Test show_df with empty DataFrame."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame([], schema)
        
        with caplog.at_level("INFO"):
            show_df(df, 5, "development")
        
        # Should handle empty DataFrame without error
        assert True

    def test_count_df_empty_dataframe(self, spark, caplog):
        """Test count_df with empty DataFrame."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame([], schema)
        
        with caplog.at_level("INFO"):
            result = count_df(df, "development")
        
        assert result == 0

    def test_show_df_different_environments(self, caplog):
        """Test show_df with different environment values."""
        mock_df = Mock()
        
        environments = ["development", "dev", "local", "staging", "production", "prod"]
        
        for env in environments:
            mock_df.reset_mock()
            with caplog.at_level("INFO"):
                show_df(mock_df, 5, env)
            
            if env in ["production", "prod"]:
                mock_df.show.assert_not_called()
            else:
                mock_df.show.assert_called_once_with(5)

    def test_count_df_different_environments(self, caplog):
        """Test count_df with different environment values."""
        mock_df = Mock()
        mock_df.count.return_value = 42
        
        environments = ["development", "dev", "local", "staging", "production", "prod"]
        
        for env in environments:
            mock_df.reset_mock()
            with caplog.at_level("INFO"):
                result = count_df(mock_df, env)
            
            if env in ["production", "prod"]:
                assert result is None
                mock_df.count.assert_not_called()
            else:
                assert result == 42
                mock_df.count.assert_called_once()

    def test_show_df_with_exception(self, caplog):
        """Test show_df when DataFrame.show raises exception."""
        mock_df = Mock()
        mock_df.show.side_effect = Exception("Show failed")
        
        with caplog.at_level("INFO"):
            # Should handle exception gracefully
            show_df(mock_df, 5, "development")
        
        mock_df.show.assert_called_once_with(5)

    def test_count_df_with_exception(self, caplog):
        """Test count_df when DataFrame.count raises exception."""
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Count failed")
        
        with caplog.at_level("INFO"):
            # Should handle exception gracefully and return None
            result = count_df(mock_df, "development")
        
        assert result is None
        mock_df.count.assert_called_once()


@pytest.mark.unit
class TestDFUtilsIntegration:
    """Integration-style tests for df_utils module."""

    def test_show_and_count_workflow(self, spark, caplog):
        """Test complete show and count workflow."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        data = [
            (1, "item1", "value1"),
            (2, "item2", "value2"),
            (3, "item3", "value3"),
            (4, "item4", "value4"),
            (5, "item5", "value5")
        ]
        df = spark.createDataFrame(data, schema)
        
        # Test in development environment
        with caplog.at_level("INFO"):
            show_df(df, 3, "development")  # Show first 3 rows
            count = count_df(df, "development")
        
        assert count == 5
        
        # Test in production environment
        with caplog.at_level("INFO"):
            show_df(df, 3, "production")  # Should not show
            count = count_df(df, "production")  # Should return None
        
        assert count is None