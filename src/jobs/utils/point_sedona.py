from pyspark.sql import SparkSession
from sedona.spark import SedonaContext


def sedona_test():
    sedona = SedonaContext.create(SparkSession.builder)

    # Create a more detailed spatial analysis
    df = sedona.sql("""
        SELECT 
            ST_Area(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))')) as area,
            ST_Perimeter(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))')) as perimeter,
            ST_Distance(
                ST_GeomFromWKT('POINT(5 5)'),
                ST_GeomFromWKT('POINT(7 7)')
            ) as distance_between_points,
            ST_X(ST_Centroid(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))'))) as centroid_x,
            ST_Y(ST_Centroid(ST_GeomFromWKT('POLYGON((0 0,10 0,10 10,0 10,0 0))'))) as centroid_y
        """)

    # Show the results with more descriptive formatting
    print("\nSpatial Analysis Results:")
    print("----------------------------sedons test starts-------------------------------------------")
    row = df.collect()[0]
    print(f"Area of polygon: {row['area']} square units")
    print(f"Perimeter of polygon: {row['perimeter']} units")
    print(f"Distance between points (5,5) and (7,7): {row['distance_between_points']} units")
    print(f"Polygon centroid: ({row['centroid_x']}, {row['centroid_y']})")

    print("-------------------------sedona test ends----------------------------------------------")