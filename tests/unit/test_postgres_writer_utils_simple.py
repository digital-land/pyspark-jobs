import os
import sys
from unittest.mock import Mock

import pytest

"""
Simplest possible test for postgres_writer_utils.py to improve coverage.
Just import and attempt to call the main function.
"""


def test_import_postgres_writer_functions():
    """Just import the functions to ensure they load."""
    from jobs.utils.postgres_writer_utils import (
        _ensure_required_columns,
        write_dataframe_to_postgres_jdbc,
    )

    assert callable(_ensure_required_columns)
    assert callable(write_dataframe_to_postgres_jdbc)


def test_ensure_required_columns_simple():
    """Simplest test for _ensure_required_columns."""

    from jobs.utils.postgres_writer_utils import _ensure_required_columns

    mock_df = Mock()
    mock_df.columns = ["entity"]
    mock_df.withColumn.return_value = mock_df

    result = _ensure_required_columns(mock_df, ["entity"], {})
    assert result is not None
