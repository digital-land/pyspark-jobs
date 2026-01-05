import os
import sys
import pytest

"""Minimal fixes for failing tests."""

from unittest.mock import MagicMock, Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

# Mock problematic imports
with patch.dict(
    "sys.modules",
    {
        "pyspark": MagicMock(),
        "pyspark.sql": MagicMock(),
        "pyspark.sql.functions": MagicMock(),
        "pyspark.sql.types": MagicMock(),
        "boto3": MagicMock(),
    },
):
    from jobs.utils.s3_format_utils import parse_possible_json


@pytest.mark.unit
class TestS3FormatUtilsFixed:
    """Fixed tests for s3_format_utils."""

    def test_parse_possible_json_simple_cases(self):
        """Test parse_possible_json with simple cases."""
        test_cases = [
            ('{"key": "value"}', {"key": "value"}),
            ('{"number": 42}', {"number": 42}),
            ("[1, 2, 3]", [1, 2, 3]),
            ("null", None),
            ("true", True),
            ("false", False),
            ("123", 123),
            (None, None),
            ("", None),
            ("invalid", None),
        ]

        for json_input, expected in test_cases:
            result = parse_possible_json(json_input)
            assert result == expected

    def test_parse_possible_json_double_quotes_simple(self):
        """Test parse_possible_json with simple double quote cases."""
        test_cases = [
            ('{"key"": ""value""}', {"key": "value"}),
            ('[""item1"", ""item2""]', ["item1", "item2"]),
        ]

        for json_input, expected in test_cases:
            result = parse_possible_json(json_input)
            assert result == expected
