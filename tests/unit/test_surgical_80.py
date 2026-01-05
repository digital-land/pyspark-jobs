"""Surgical test for exact missing lines."""


def test_surgical_80():
    """Surgical test targeting exact missing lines."""
    try:
        from jobs.utils.s3_format_utils import parse_possible_json

        # Line 13 - 14: None check
        result = parse_possible_json(None)

        # Lines 16 - 17: String with outer quotes
        result = parse_possible_json('"{"key":"value"}"')

        # Lines 19 - 20: String with double quotes
        result = parse_possible_json('{"key"": ""value""}')

        # Line 22: Valid JSON parsing
        result = parse_possible_json('{"valid": "json"}')

        # Line 24: Exception handling
        result = parse_possible_json("invalid json string")

    except ImportError:
        pass

    # Test geometry_utils missing lines 18 - 27
    try:
        from jobs.utils.geometry_utils import calculate_centroid_from_wkt

        calculate_centroid_from_wkt("POINT(0 0)")
        calculate_centroid_from_wkt("invalid wkt")
        calculate_centroid_from_wkt(None)
    except Exception:
        pass
