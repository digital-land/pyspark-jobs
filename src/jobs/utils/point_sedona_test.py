# run_sedona.py
import os
import sys
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# Set Java Home
#os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@17'

# Create Spark session with Sedona configuration
#spark = SparkSession.builder \
#    .appName("SedonaExample") \
#    .config("spark.jars", "/Users/399182/MHCLG-githib/unit_test/sedona_jars/sedona-spark-shaded-3.5_2.12-1.8.0.jar,/Users/399182/MHCLG-githib/unit_test/sedona_jars/geotools-wrapper-1.8.0-33.1.jar") \
#    .config("spark.driver.extraClassPath", "/Users/399182/MHCLG-githib/unit_test/sedona_jars/sedona-spark-shaded-3.5_2.12-1.8.0.jar:/Users/399182/MHCLG-githib/unit_test/sedona_jars/geotools-wrapper-1.8.0-33.1.jar") \
    # .config("spark.executor.extraClassPath", "/Users/399182/MHCLG-githib/unit_test/sedona_jars/sedona-spark-shaded-3.5_2.12-1.8.0.jar:/Users/399182/MHCLG-githib/unit_test/sedona_jars/geotools-wrapper-1.8.0-33.1.jar") \
    # .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    # .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    # .config("spark.sql.extensions", "org.apache.sedona.sql.SedonaSqlExtensions") \
    # .master("local[*]") \
    # .getOrCreate()

def sedona_test(spark):
    sedona = SedonaContext.create(spark)

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

    