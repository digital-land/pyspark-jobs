from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
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
    
    # Infer schema from the json column
    schema = df.select(from_json(col("json"), "string").alias("parsed")).schema.fields[0].dataType
    
    # If schema inference didn't work, sample and infer
    if isinstance(schema, StructType):
        json_schema = schema
    else:
        json_schema = df.select(from_json(col("json"), df.select(col("json")).first()[0]).alias("parsed")).schema.fields[0].dataType
    
    # Parse JSON and flatten
    df_parsed = df.withColumn("parsed", from_json(col("json"), json_schema))
    
    # Extract all fields from parsed struct
    df_flattened = df_parsed.select(
        *[c for c in df.columns if c != "json"],
        "parsed.*"
    )
    
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
    geojson_schema = StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True),
        StructField("properties", StringType(), True)
    ])
    
    # Parse GeoJSON and flatten
    df_flattened = df.withColumn("parsed", from_json(col("geojson"), geojson_schema)).select(
        *[c for c in df.columns if c != "geojson"],
        "parsed.*"
    )
    
    return df_flattened
