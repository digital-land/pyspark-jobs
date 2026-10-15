from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, from_json, map_keys
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)


def flatten_json_column(df: DataFrame) -> DataFrame:
    """
    Flatten JSON data from 'json' column into separate columns.

    Every key found in any row's JSON becomes a column, so a field only some
    entities have is kept for those entities and left null for the rest.
    Values stay the strings they were stored as.

    Args:
        df: DataFrame containing a 'json' column with JSON string data

    Returns:
        DataFrame with JSON data flattened into columns, original 'json' column dropped
    """
    if "json" not in df.columns:
        return df

    df_parsed = df.withColumn(
        "_json_map", from_json(col("json"), MapType(StringType(), StringType()))
    )

    # Collect the keys across all rows
    keys = sorted(
        row["key"]
        for row in df_parsed.select(explode(map_keys(col("_json_map"))).alias("key"))
        .distinct()
        .collect()
        if row["key"] not in df.columns
    )

    return df_parsed.select(
        *[c for c in df.columns if c != "json"],
        *[col("_json_map").getItem(key).alias(key) for key in keys],
    )


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
