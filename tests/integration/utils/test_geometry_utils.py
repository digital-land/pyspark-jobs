"""
Integration tests for geometry_utils.

These tests run calculate_centroid on real Spark DataFrames
using a Sedona-enabled Spark session.
"""

from pyspark.sql.types import StringType, StructField, StructType

from jobs.utils.geometry_utils import calculate_centroid

SCHEMA = StructType(
    [
        StructField("entity", StringType()),
        StructField("geometry", StringType()),
        StructField("point", StringType()),
    ]
)


def test_calculate_centroid_point_preserved_when_geometry_is_null(spark):
    """When geometry is null and point has a value,
    calculate_centroid should preserve the existing point value."""
    df = spark.createDataFrame(
        [("3001", None, "POINT(-0.1234 51.5678)")],
        schema=SCHEMA,
    )

    result = calculate_centroid(df)
    result_row = result.collect()[0]

    assert "point" in result.columns
    assert result_row["point"] == "POINT(-0.1234 51.5678)"


def test_calculate_centroid_calculated_from_geometry_when_point_absent(spark):
    """When geometry has a value and point is null,
    calculate_centroid should calculate the centroid from the geometry."""
    df = spark.createDataFrame(
        [("3002", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", None)],
        schema=SCHEMA,
    )

    result = calculate_centroid(df)
    result_row = result.collect()[0]

    assert result_row["point"] == "POINT (0.5 0.5)"


def test_calculate_centroid_point_not_overwritten_when_both_present(spark):
    """When both geometry and point have values,
    calculate_centroid should preserve the existing point value."""
    df = spark.createDataFrame(
        [
            (
                "3003",
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "POINT(-0.1234 51.5678)",
            )
        ],
        schema=SCHEMA,
    )

    result = calculate_centroid(df)
    result_row = result.collect()[0]

    assert result_row["point"] == "POINT(-0.1234 51.5678)"
