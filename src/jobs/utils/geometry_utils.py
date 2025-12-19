from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr
from sedona.spark import SedonaContext


def calculate_centroid(df: DataFrame) -> DataFrame:
    """
    Calculate centroid from geometry column and add as point column.

    Args:
        df: DataFrame with 'geometry' column containing WKT multipolygon data

    Returns:
        DataFrame with added 'point' column containing centroid as WKT point
    """
    # Initialize Sedona context to register spatial functions
    SedonaContext.create(df.sparkSession)

    # Drop existing point column if present
    if "point" in df.columns:
        df = df.drop("point")

    # Create temp view and use SQL to calculate centroid
    df.createOrReplaceTempView("temp_geometry")

    return df.sparkSession.sql(
        """
        SELECT *, 
            CASE 
                WHEN geometry IS NOT NULL THEN
                    ST_AsText(
                        ST_SetSRID(
                            ST_Point(
                                ROUND(ST_X(ST_Centroid(ST_GeomFromWKT(geometry))), 6),
                                ROUND(ST_Y(ST_Centroid(ST_GeomFromWKT(geometry))), 6)
                            ),
                            4326
                        )
                    )
                ELSE NULL
            END as point
        FROM temp_geometry
        """
    )


def sedona_unit_test():
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
