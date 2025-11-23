"""Unit tests for df_utils module."""
import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))

from jobs.utils.df_utils import show_df

def test_show_df_development():
    """Test show_df in development environment."""
    mock_df = Mock()
    show_df(mock_df, 5, "development")
    mock_df.show.assert_called_once_with(5)

def test_show_df_staging():
    """Test show_df in staging environment."""
    mock_df = Mock()
    show_df(mock_df, 5, "staging")
    mock_df.show.assert_called_once_with(5)

def test_show_df_production():
    """Test show_df in production environment."""
    mock_df = Mock()
    show_df(mock_df, 5, "production")
    mock_df.show.assert_not_called()

def test_show_df_local():
    """Test show_df in local environment."""
    mock_df = Mock()
    show_df(mock_df, 3, "local")
    # Local environment should not show data (not in development/staging)
    mock_df.show.assert_not_called()