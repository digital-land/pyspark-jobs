from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import when
from sedona.spark import SedonaContext
from sedona.spark.sql.st_constructors import ST_GeomFromWKT, ST_Point
from sedona.spark.sql.st_functions import ST_X, ST_Y, ST_AsText, ST_Centroid, ST_SetSRID


def calculate_centroid(df: DataFrame) -> DataFrame:
    """
    Calculate centroid from geometry column and add as point column.

    If a point column already exists with a non-null value, that value is preserved.
    Otherwise, if geometry is not null, the centroid is calculated from the geometry.

    Args:
        df: DataFrame with 'geometry' column containing WKT multipolygon data

    Returns:
        DataFrame with 'point' column containing WKT point
    """
    SedonaContext.create(df.sparkSession)

    has_point = "point" in df.columns

    centroid = ST_Centroid(ST_GeomFromWKT(col("geometry")))
    centroid_expr = ST_AsText(
        ST_SetSRID(
            ST_Point(
                spark_round(ST_X(centroid), 6),
                spark_round(ST_Y(centroid), 6),
            ),
            lit(4326),
        )
    )

    calculated_point = when(col("geometry").isNotNull(), centroid_expr)

    if has_point:
        point_col = when(col("point").isNotNull(), col("point")).otherwise(
            calculated_point
        )
    else:
        point_col = calculated_point

    return df.withColumn("point", point_col)
