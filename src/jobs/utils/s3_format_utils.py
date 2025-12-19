import json
import logging

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, regexp_replace, when
from pyspark.sql.types import MapType, StringType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Helper function to clean and parse double-escaped JSON strings
def parse_possible_json(val):
    if val is None:
        return None
    try:
        # Remove outer quotes if present
        if isinstance(val, str) and val.startswith('"') and val.endswith('"'):
            val = val[1:-1]
        # Replace doubled quotes with single quotes
        if isinstance(val, str) and '""' in val:
            val = val.replace('""', '"')
        # Try to parse as JSON
        return json.loads(val)
    except Exception:
        return None


detected_json_cols = []


def s3_csv_format(df):
    # Automatically detect string columns that contain JSON by sampling non-null values
    candidate_string_cols = [
        f.name for f in df.schema if isinstance(f.dataType, StringType)
    ]
    detected_json_cols = []
    logger.info(f"Candidate string columns: {candidate_string_cols}")
    for c in candidate_string_cols:
        sample = df.select(c).dropna().limit(1).collect()
        if sample:
            if parse_possible_json(sample[0][0]):
                detected_json_cols.append(c)
                logger.info(f"Detected JSON column: {c}")

    # Identify and log columns with JSON data
    for json_col in detected_json_cols:
        sample = df.select(json_col).dropna().limit(1).collect()
        if sample:
            parsed = parse_possible_json(sample[0][0])
            if parsed:
                logger.info(f"Column '{json_col}' contains JSON data. Example:")
                logger.debug(json.dumps(parsed, indent=2))

    for json_col in detected_json_cols:
        sample = df.select(json_col).dropna().limit(1).collect()
        if sample:
            logger.info(f"Processing JSON column: {json_col}")
            # Remove outer quotes and double quotes using Spark SQL functions
            cleaned_col = when(
                (col(json_col).startswith('"') & col(json_col).endswith('"')),
                expr(f"substring({json_col}, 2, length({json_col}) - 2)"),
            ).otherwise(col(json_col))
            logger.debug(f"Cleaned column expression for '{json_col}': {cleaned_col}")
            # Replace double quotes with single quotes
            cleaned_col = regexp_replace(cleaned_col, '""', '"')
            logger.debug(f"Final cleaned column for '{json_col}': {cleaned_col}")
            # Parse JSON using from_json
            df = df.withColumn(
                f"{json_col}_map",
                from_json(col(json_col), MapType(StringType(), StringType())),
            )
            # Get all keys in this JSON column
            keys = (
                df.select(f"{json_col}_map")
                .rdd.flatMap(
                    lambda row: (
                        row[f"{json_col}_map"].keys() if row[f"{json_col}_map"] else []
                    )
                )
                .distinct()
                .collect()
            )
            logger.info(f"Keys found in column '{json_col}': {keys}")
            # Add each key as a new column
            for key in keys:
                logger.debug(f"Adding column: {json_col}_{key}")
                df = df.withColumn(
                    f"{json_col}_{key}", col(f"{json_col}_map").getItem(key)
                )
            # Drop the original and map columns
            df = df.drop(json_col, f"{json_col}_map")
    logger.info("Completed S3 CSV format transformation.")
    return df


def flatten_s3_json(nested_df):
    # Function to recursively flatten the DataFrame
    flat_cols = [
        c[0]
        for c in nested_df.dtypes
        if not c[1].startswith("struct") and not c[1].startswith("array")
    ]
    nested_cols = [c[0] for c in nested_df.dtypes if c[1].startswith("struct")]

    while nested_cols:
        col_to_flatten = nested_cols.pop(0)
        expanded = [
            col(f"{col_to_flatten}.{k}").alias(f"{col_to_flatten}_{k}")
            for k in nested_df.select(col_to_flatten + ".*").columns
        ]
        nested_df = nested_df.select("*", *expanded).drop(col_to_flatten)
        nested_cols = [c[0] for c in nested_df.dtypes if c[1].startswith("struct")]

    return nested_df.select(
        *flat_cols, *[c for c in nested_df.columns if c not in flat_cols]
    )


def renaming(dataset_name, bucket):
    s3 = boto3.client("s3")

    bucket = "development-pd-batch-emr-studio-ws-bucket"
    prefix = f"csv/{dataset_name}.csv/"
    new_key = f"csv/{dataset_name}.csv"

    # List objects in the folder
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv") and "part-" in key:
            # Copy to new key
            s3.copy_object(
                Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key
            )
            # Delete old file
            s3.delete_object(Bucket=bucket, Key=key)
            break


def flatten_s3_geojson(df):
    df = df.withColumn(
        "longitude", regexp_extract("point", r"POINT \(([^ ]+)", 1).cast("double")
    )
    df = df.withColumn(
        "latitude",
        regexp_extract("point", r"POINT \([^ ]+ ([^\)]+)\)", 1).cast("double"),
    )

    # Create the geometry object structure
    df = df.withColumn(
        "geometry",
        struct(
            lit("Point").alias("type"),
            array("longitude", "latitude").alias("coordinates"),
        ),
    )

    # Create properties structure with all columns except geometry-related ones
    exclude_cols = ["point", "longitude", "latitude", "geometry"]
    property_cols = [col for col in df.columns if col not in exclude_cols]

    # Create properties map from remaining columns
    properties_expr = create_map(
        *[lit(col_name) for col in property_cols for col_name in [col, col]]
    )
    df = df.withColumn("properties", properties_expr)

    # Create final GeoJSON feature structure
    df = df.select(lit("Feature").alias("type"), col("geometry"), col("properties"))

    # Collect all features into a FeatureCollection
    features_df = df.select(
        lit("FeatureCollection").alias("type"),
        collect_list(struct("type", "geometry", "properties")).alias("features"),
    )

    # Convert to GeoJSON string
    geojson = features_df.select(to_json(struct("type", "features"))).first()[0]

    # Write the GeoJSON to a file
    with open(output_path, "w") as f:
        f.write(geojson)

    print(f"GeoJSON file has been created at: {output_path}")
