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
