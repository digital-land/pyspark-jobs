from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType


def flatten_json_column(df: DataFrame) -> DataFrame:
    """
    Flatten JSON data from 'json' column into separate columns.

    Args:
        df: DataFrame containing a 'json' column with JSON string data

    Returns:
        DataFrame with JSON data flattened into columns, original 'json' column dropped
    """
    if "json" not in df.columns:
        return df

    # Infer schema from a sample JSON string
    sample_json = df.select(col("json")).filter(col("json").isNotNull()).first()
    if not sample_json or not sample_json[0]:
        return df

    json_schema = schema_of_json(sample_json[0])

    # Parse JSON and flatten
    df_parsed = df.withColumn("parsed", from_json(col("json"), json_schema))

    # Extract all fields from parsed struct
    df_flattened = df_parsed.select(*[c for c in df.columns if c != "json"], "parsed.*")

    return df_flattened


def flatten_geojson_column(df: DataFrame) -> DataFrame:
    """
    Flatten GeoJSON data from 'geojson' column into separate columns.

    Args:
        df: DataFrame containing a 'geojson' column with GeoJSON string data

    Returns:
        DataFrame with GeoJSON data flattened into columns, original 'geojson' column dropped
    """
    if "geojson" not in df.columns:
        return df

    # Define GeoJSON schema
    geojson_schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("coordinates", ArrayType(DoubleType()), True),
            StructField("properties", StringType(), True),
        ]
    )

    # Parse GeoJSON and flatten
    df_flattened = df.withColumn(
        "parsed", from_json(col("geojson"), geojson_schema)
    ).select(*[c for c in df.columns if c != "geojson"], "parsed.*")

    return df_flattened
