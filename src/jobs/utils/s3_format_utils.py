from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import MapType, StringType
import json

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

parse_possible_json_udf = udf(parse_possible_json, MapType(StringType(), StringType()))
detected_json_cols = []
def s3_csv_format(df):
    # Automatically detect string columns that contain JSON by sampling non-null values
    candidate_string_cols = [f.name for f in df.schema if isinstance(f.dataType, StringType)]
    detected_json_cols = []
    for c in candidate_string_cols:
        sample = df.select(c).dropna().limit(1).collect()
        if sample:
            if parse_possible_json(sample[0][0]):
                detected_json_cols.append(c)

    # Identify and print columns with JSON data
    for json_col in detected_json_cols:
        sample = df.select(json_col).dropna().limit(1).collect()
        if sample:
            parsed = parse_possible_json(sample[0][0])
            if parsed:
                print(f"Column '{json_col}' contains JSON data. Example:")
                print(json.dumps(parsed, indent=2))

    for json_col in detected_json_cols:
        sample = df.select(json_col).dropna().limit(1).collect()
        if sample:
            # Use the generalised UDF to parse possible JSON
            df = df.withColumn(f"{json_col}_map", parse_possible_json_udf(col(json_col)))
            # Get all keys in this JSON column
            keys = df.select(f"{json_col}_map").rdd \
                .flatMap(lambda row: row[f"{json_col}_map"].keys() if row[f"{json_col}_map"] else []) \
                .distinct().collect()
            # Add each key as a new column
            for key in keys:
                df = df.withColumn(f"{json_col}_{key}", col(f"{json_col}_map").getItem(key))
            # Drop the original and map columns
            df = df.drop(json_col, f"{json_col}_map")

        return df