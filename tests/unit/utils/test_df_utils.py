"""Unit tests for df_utils module."""

import os
import sys
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

import pytest

from jobs.utils.df_utils import count_df, show_df


class TestDFUtils:
    """Test suite for df_utils module."""

    def test_show_df_development_environment(self, caplog):
        """Test show_df in development environment."""
        mock_df = Mock()

        with caplog.at_level("INFO"):
            show_df(mock_df, 5, "development")

        # In development, should call show
        mock_df.show.assert_called_once_with(5)

    def test_show_df_production_environment(self, caplog):
        """Test show_df in production environment."""
        mock_df = Mock()

        with caplog.at_level("INFO"):
            show_df(mock_df, 5, "production")

        # In production, should not call show
        mock_df.show.assert_not_called()

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

    def test_show_df_development_environment(self, caplog):
        """Test show_df in development environment."""
        mock_df = Mock()
        mock_df.count.return_value = 3

        with caplog.at_level("INFO"):
            result = count_df(mock_df, "development")

        assert result == 3
        mock_df.count.assert_called_once()

    def test_show_df_production_environment(self, caplog):
        """Test show_df in production environment."""
        mock_df = Mock()

        with caplog.at_level("INFO"):
            result = count_df(mock_df, "production")

        # In production, should return None and not call count
        assert result is None
        mock_df.count.assert_not_called()

    def test_show_df_with_mock_dataframe(self, caplog):
        """Test show_df with mock DataFrame."""
        mock_df = Mock()
        mock_df.count.return_value = 100

        with caplog.at_level("INFO"):
            result = count_df(mock_df, "development")

        assert result == 100
        mock_df.count.assert_called_once()

    def test_show_df_production_no_count(self, caplog):
        """Test that show_df doesn't call count in production."""
        mock_df = Mock()

        with caplog.at_level("INFO"):
            result = count_df(mock_df, "production")

        assert result is None
        # Should not call count on the DataFrame in production
        mock_df.count.assert_not_called()

    def test_show_df_empty_dataframe(self, caplog):
        """Test show_df with empty DataFrame."""
        mock_df = Mock()

        with caplog.at_level("INFO"):
            show_df(mock_df, 5, "development")

        # Should handle empty DataFrame without error
        mock_df.show.assert_called_once_with(5)

    def test_show_df_empty_dataframe(self, caplog):
        """Test show_df with empty DataFrame."""
        mock_df = Mock()
        mock_df.count.return_value = 0

        with caplog.at_level("INFO"):
            result = count_df(mock_df, "development")

        assert result == 0
        mock_df.count.assert_called_once()

    def test_show_df_different_environments(self, caplog):
        """Test show_df with different environment values."""
        mock_df = Mock()

        environments = ["development", "dev", "local", "staging", "production", "prod"]

        for env in environments:
            mock_df.reset_mock()
            with caplog.at_level("INFO"):
                show_df(mock_df, 5, env)

            # Current implementation only shows for development and staging
            if env in ["development", "staging"]:
                mock_df.show.assert_called_once_with(5)
            else:
                mock_df.show.assert_not_called()

    def test_show_df_different_environments(self, caplog):
        """Test show_df with different environment values."""
        mock_df = Mock()
        mock_df.count.return_value = 42

        environments = ["development", "dev", "local", "staging", "production", "prod"]

        for env in environments:
            mock_df.reset_mock()
            with caplog.at_level("INFO"):
                result = show_df(mock_df, env)

            # Current implementation only counts for development and staging
            if env in ["development", "staging"]:
                assert result == 42 or result is None
                mock_df.count.assert_called_once()
            else:
                assert result is None
                mock_df.count.assert_not_called()

    def test_show_df_with_exception(self, caplog):
        """Test show_df when DataFrame.show raises exception."""
        mock_df = Mock()
        mock_df.show.side_effect = Exception("Show failed")

        with pytest.raises(Exception, match="Show failed"):
            show_df(mock_df, 5, "development")

        mock_df.show.assert_called_once_with(5)

    def test_show_df_with_exception(self, caplog):
        """Test show_df when DataFrame.count raises exception."""
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Count failed")

        with pytest.raises(Exception, match="Count failed"):
            count_df(mock_df, "development")

        mock_df.count.assert_called_once()


@pytest.mark.unit
class TestDFUtilsIntegration:
    """Integration - style tests for df_utils module."""

    def test_show_and_count_workflow(self, caplog):
        """Test complete show and count workflow."""
        mock_df = Mock()
        mock_df.count.return_value = 5

        # Test in development environment
        with caplog.at_level("INFO"):
            show_df(mock_df, 3, "development")
            count = count_df(mock_df, "development")

        assert count == 5
        mock_df.show.assert_called_with(3)

        # Reset mock for production test
        mock_df.reset_mock()

        # Test in production environment
        with caplog.at_level("INFO"):
            show_df(mock_df, 3, "production")
            count = count_df(mock_df, "production")

        assert count is None
        mock_df.show.assert_not_called()
        mock_df.count.assert_not_called()
