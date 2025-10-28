from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, expr, regexp_replace
from pyspark.sql.types import MapType, StringType
import json
import logging

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
    candidate_string_cols = [f.name for f in df.schema if isinstance(f.dataType, StringType)]
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
                expr(f"substring({json_col}, 2, length({json_col}) - 2)")
            ).otherwise(col(json_col))
            logger.debug(f"Cleaned column expression for '{json_col}': {cleaned_col}")
            # Replace double quotes with single quotes
            cleaned_col = regexp_replace(cleaned_col, '""', '"')
            logger.debug(f"Final cleaned column for '{json_col}': {cleaned_col}")
            # Parse JSON using from_json
            df = df.withColumn(f"{json_col}_map", from_json(col(json_col), MapType(StringType(), StringType())))
            # Get all keys in this JSON column
            keys = df.select(f"{json_col}_map").rdd \
                .flatMap(lambda row: row[f"{json_col}_map"].keys() if row[f"{json_col}_map"] else []) \
                .distinct().collect()
            logger.info(f"Keys found in column '{json_col}': {keys}")
            # Add each key as a new column
            for key in keys:
                logger.debug(f"Adding column: {json_col}_{key}")
                df = df.withColumn(f"{json_col}_{key}", col(f"{json_col}_map").getItem(key))
            # Drop the original and map columns
            df = df.drop(json_col, f"{json_col}_map")
    logger.info("Completed S3 CSV format transformation.")
    return df