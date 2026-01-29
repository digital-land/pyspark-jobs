"""Unit tests for geometry utilities."""

import pytest
from pyspark.sql import SparkSession


@pytest.mark.skip(
    reason="Sedona requires specific Spark configuration and is better tested in integration tests"
)
def test_sedona_spatial_operations():
    """Test Sedona spatial operations including area, perimeter, distance, and centroid calculations."""
    # Create (or reuse) the SparkSession first
    spark = SparkSession.builder.getOrCreate()

    # Wrap it with Sedona; this wires up SQL functions/UDFs
    sedona = SedonaContext.create(spark)

    # Minimal fail‑fast check (optional but helpful in Airflow logs)
    sedona.sql("SELECT ST_Point(0,0)").collect()

    # Create a more detailed spatial analysis
    df = sedona.sql(
        """
        SELECT 
            ST_Area(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))')) as area,
            ST_Perimeter(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))')) as perimeter,
            ST_Distance(
                ST_GeomFromWKT('POINT(5 5)'),
                ST_GeomFromWKT('POINT(7 7)')
            ) as distance_between_points,
            ST_X(ST_Centroid(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))'))) as centroid_x,
            ST_Y(ST_Centroid(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))'))) as centroid_y
        """
    )

    # Show the results with more descriptive formatting
    print("\nSpatial Analysis Results:")
    print(
        "----------------------------sedons test starts-------------------------------------------"
    )
    row = df.collect()[0]
    print(f"Area of polygon: {row['area']} square units")
    print(f"Perimeter of polygon: {row['perimeter']} units")
    print(
        f"Distance between points (5,5) and (7,7): {row['distance_between_points']} units"
    )
    print(f"Polygon centroid: ({row['centroid_x']}, {row['centroid_y']})")

    print(
        "-------------------------sedona test ends----------------------------------------------"
    )

    # Assert expected values
    assert row["area"] == 100.0, "Expected area of 10x10 square to be 100"
    assert row["perimeter"] == 40.0, "Expected perimeter of 10x10 square to be 40"
    assert row["centroid_x"] == 5.0, "Expected centroid X coordinate to be 5"
    assert row["centroid_y"] == 5.0, "Expected centroid Y coordinate to be 5"
