#!/usr/bin/env python3
"""
Parquet to SQLite Converter

This script reads parquet files from S3 and converts them to SQLite databases
for local analysis, backup, and portable data distribution.

Usage:
    python parquet_to_sqlite.py --input s3://bucket/path/to/parquet --output /local/path/output.sqlite
    python parquet_to_sqlite.py --input s3://bucket/path/to/parquet --output s3://bucket/sqlite-output/
"""

import argparse
import sys
import os
from pathlib import Path

# Add the jobs package to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from jobs.utils.logger_config import get_logger, log_execution_time
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_json, date_format
from pyspark.sql.types import StringType
import time

logger = get_logger(__name__)


def create_spark_session(app_name="ParquetToSQLite"):
    """
    Create and configure Spark session for parquet to SQLite conversion.

    Returns:
        SparkSession: Configured Spark session
    """
    logger.info(f"create_spark_session: Creating Spark session: {app_name}")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    logger.info("create_spark_session: Spark session created successfully")
    return spark


def get_sqlite_schema_mapping():
    """
    Map common data types to SQLite equivalents.

    Returns:
        dict: Mapping of data types to SQLite types
    """
    return {
        "string": "TEXT",
        "text": "TEXT",
        "varchar": "TEXT",
        "char": "TEXT",
        "date": "TEXT",  # SQLite stores dates as TEXT
        "timestamp": "TEXT",  # SQLite stores timestamps as TEXT
        "json": "TEXT",  # SQLite stores JSON as TEXT
        "jsonb": "TEXT",
        "geometry": "TEXT",  # Store as WKT text
        "point": "TEXT",
        "multipolygon": "TEXT",
        "bigint": "INTEGER",
        "int": "INTEGER",
        "integer": "INTEGER",
        "long": "INTEGER",
        "float": "REAL",
        "double": "REAL",
        "decimal": "REAL",
        "boolean": "INTEGER",  # SQLite uses INTEGER for boolean
        "bool": "INTEGER",
    }


def prepare_dataframe_for_sqlite(df):
    """
    Prepare DataFrame for SQLite output by handling data types and special columns.

    Args:
        df: PySpark DataFrame to prepare

    Returns:
        DataFrame: Prepared DataFrame for SQLite compatibility
    """
    logger.info(
        "prepare_dataframe_for_sqlite: Preparing DataFrame for SQLite compatibility"
    )

    processed_df = df

    # Get all columns and their types
    columns_info = [
        (field.name, str(field.dataType).lower()) for field in df.schema.fields
    ]
    logger.info(f"prepare_dataframe_for_sqlite: Processing {len(columns_info)} columns")

    for col_name, col_type in columns_info:
        logger.debug(
            f"prepare_dataframe_for_sqlite: Processing column {col_name} of type {col_type}"
        )

        # Handle JSON/struct columns - convert to JSON strings
        if (
            "struct" in col_type
            or "map" in col_type
            or col_name.lower() in ["geojson", "json"]
        ):
            logger.info(
                f"prepare_dataframe_for_sqlite: Converting {col_name} to JSON string"
            )
            processed_df = processed_df.withColumn(
                col_name,
                when(col(col_name).isNull(), None).otherwise(to_json(col(col_name))),
            )

        # Handle date/timestamp columns - convert to ISO format strings
        elif (
            "date" in col_type
            or "timestamp" in col_type
            or col_name.lower().endswith("_date")
        ):
            logger.info(
                f"prepare_dataframe_for_sqlite: Converting {col_name} to ISO date string"
            )
            if "timestamp" in col_type:
                # For timestamps, include time
                processed_df = processed_df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), None).otherwise(
                        date_format(col(col_name), "yyyy-MM-dd HH:mm:ss")
                    ),
                )
            else:
                # For dates, just the date part
                processed_df = processed_df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), None).otherwise(
                        date_format(col(col_name), "yyyy-MM-dd")
                    ),
                )

        # Handle geometry columns - ensure they're stored as text
        elif (
            col_name.lower() in ["geometry", "point", "geom", "wkt"]
            or "geometry" in col_type
        ):
            logger.info(
                f"prepare_dataframe_for_sqlite: Converting {col_name} to WKT text for SQLite"
            )
            processed_df = processed_df.withColumn(
                col_name,
                when(col(col_name).isNull(), None).otherwise(
                    col(col_name).cast(StringType())
                ),
            )

    logger.info("prepare_dataframe_for_sqlite: DataFrame preparation completed")
    return processed_df


def write_to_sqlite_file(
    df, output_path, table_name="data", method="optimized", max_records_per_file=500000
):
    """
    Write DataFrame to SQLite file(s).

    Args:
        df: PySpark DataFrame to write
        output_path: Path where SQLite file should be created
        table_name: Name of table in SQLite
        method: Write method - "single_file", "partitioned", "optimized"
        max_records_per_file: Maximum records per SQLite file
    """
    logger.info(
        f"write_to_sqlite_file: Writing DataFrame to SQLite using {method} method"
    )
    logger.info(f"write_to_sqlite_file: Output path: {output_path}")
    logger.info(f"write_to_sqlite_file: Table name: {table_name}")

    # Prepare DataFrame for SQLite
    processed_df = prepare_dataframe_for_sqlite(df)
    row_count = processed_df.count()
    logger.info(f"write_to_sqlite_file: Processing {row_count} rows")

    # Determine approach based on method and row count
    if method == "optimized":
        if row_count <= max_records_per_file:
            method = "single_file"
            logger.info(
                "write_to_sqlite_file: Using single file approach for optimized method"
            )
        else:
            method = "partitioned"
            logger.info(
                f"write_to_sqlite_file: Using partitioned approach for optimized method ({row_count} rows)"
            )

    if method == "single_file":
        return _write_single_sqlite_file(processed_df, output_path, table_name)
    elif method == "partitioned":
        return _write_partitioned_sqlite_files(
            processed_df, output_path, table_name, max_records_per_file
        )
    else:
        raise ValueError(f"Unknown method: {method}")


def _write_single_sqlite_file(df, output_path, table_name):
    """Write DataFrame to a single SQLite file."""
    import sqlite3
    import tempfile

    logger.info("_write_single_sqlite_file: Writing to single SQLite file")

    # Determine final output path
    if output_path.endswith(".db") or output_path.endswith(".sqlite"):
        sqlite_path = output_path
    else:
        sqlite_path = f"{output_path.rstrip('/')}/{table_name}.sqlite"

    # Handle S3 output
    if sqlite_path.startswith("s3://") or sqlite_path.startswith("s3a://"):
        logger.info(
            "_write_single_sqlite_file: S3 output detected, using local staging"
        )
        local_sqlite_path = f"/tmp/{table_name}_{int(time.time())}.sqlite"
        final_path = sqlite_path
    else:
        local_sqlite_path = sqlite_path
        final_path = sqlite_path
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_sqlite_path), exist_ok=True)

    # Create temporary CSV for data transfer
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = f"{temp_dir}/data.csv"

        logger.info("_write_single_sqlite_file: Exporting DataFrame to CSV")
        df.coalesce(1).write.mode("overwrite").option("header", "true").option(
            "quote", '"'
        ).option("escape", '"').csv(csv_path)

        # Find the actual CSV file
        csv_files = [f for f in os.listdir(csv_path) if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError("No CSV file generated by Spark")

        actual_csv_path = os.path.join(csv_path, csv_files[0])

        # Create SQLite database and import data
        logger.info(
            f"_write_single_sqlite_file: Creating SQLite database: {local_sqlite_path}"
        )

        conn = sqlite3.connect(local_sqlite_path)
        cursor = conn.cursor()

        # Create table schema based on DataFrame schema
        columns_def = []
        schema_mapping = get_sqlite_schema_mapping()

        for field in df.schema.fields:
            col_name = field.name
            spark_type = str(field.dataType).lower()

            # Map Spark types to SQLite types
            sqlite_type = "TEXT"  # Default
            for spark_pattern, sqlite_target in schema_mapping.items():
                if spark_pattern in spark_type:
                    sqlite_type = sqlite_target
                    break

            columns_def.append(f'"{col_name}" {sqlite_type}')

        create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns_def)})'
        logger.info(
            f"_write_single_sqlite_file: Creating table with schema: {len(columns_def)} columns"
        )
        cursor.execute(create_table_sql)

        # Import CSV data
        logger.info("_write_single_sqlite_file: Importing CSV data to SQLite")
        import csv

        with open(actual_csv_path, "r", encoding="utf-8") as csvfile:
            csv_reader = csv.DictReader(csvfile)
            columns = [field.name for field in df.schema.fields]
            placeholders = ", ".join(["?" for _ in columns])
            column_names = ", ".join([f'"{c}"' for c in columns])
            insert_sql = (
                f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
            )

            rows_imported = 0
            batch_data = []
            batch_size = 1000

            for row in csv_reader:
                row_data = [row.get(col, None) for col in columns]
                batch_data.append(row_data)

                if len(batch_data) >= batch_size:
                    cursor.executemany(insert_sql, batch_data)
                    rows_imported += len(batch_data)
                    batch_data = []

            # Insert remaining rows
            if batch_data:
                cursor.executemany(insert_sql, batch_data)
                rows_imported += len(batch_data)

        conn.commit()
        conn.close()

        logger.info(
            f"_write_single_sqlite_file: Successfully imported {rows_imported} rows to SQLite"
        )

    # Upload to S3 if needed
    if final_path != local_sqlite_path:
        _upload_to_s3(local_sqlite_path, final_path)
        os.remove(local_sqlite_path)
        logger.info(
            f"_write_single_sqlite_file: SQLite file uploaded to S3: {final_path}"
        )

    logger.info(f"_write_single_sqlite_file: SQLite file created: {final_path}")
    return final_path


def _write_partitioned_sqlite_files(df, output_path, table_name, max_records_per_file):
    """Write DataFrame to multiple SQLite files."""
    logger.info("_write_partitioned_sqlite_files: Writing to multiple SQLite files")

    row_count = df.count()
    num_files = (row_count // max_records_per_file) + 1
    logger.info(f"_write_partitioned_sqlite_files: Creating {num_files} SQLite files")

    # Repartition DataFrame
    partitioned_df = df.repartition(num_files)

    output_dir = output_path.rstrip("/")
    created_files = []

    def write_partition_to_sqlite(iterator, partition_id):
        """Write a single partition to SQLite."""
        import sqlite3
        import tempfile

        # Create SQLite file for this partition
        sqlite_file = f"{output_dir}/{table_name}_part_{partition_id:03d}.sqlite"

        if sqlite_file.startswith("s3://") or sqlite_file.startswith("s3a://"):
            local_file = (
                f"/tmp/{table_name}_part_{partition_id:03d}_{int(time.time())}.sqlite"
            )
        else:
            local_file = sqlite_file
            os.makedirs(os.path.dirname(local_file), exist_ok=True)

        # Convert iterator to list for processing
        rows = list(iterator)
        if not rows:
            logger.info(
                f"_write_partitioned_sqlite_files: Partition {partition_id} is empty, skipping"
            )
            return []

        logger.info(
            f"_write_partitioned_sqlite_files: Writing partition {partition_id} with {len(rows)} rows"
        )

        # Create SQLite database
        conn = sqlite3.connect(local_file)
        cursor = conn.cursor()

        # Create table schema (assuming we have the schema from the first row)
        if rows:
            # Get column names from the first row
            columns = rows[0].__fields__
            columns_def = [
                f'"{col}" TEXT' for col in columns
            ]  # Use TEXT for simplicity in partitioned mode

            create_table_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns_def)})'
            cursor.execute(create_table_sql)

            # Insert data
            placeholders = ", ".join(["?" for _ in columns])
            column_names = ", ".join([f'"{c}"' for c in columns])
            insert_sql = (
                f'INSERT INTO "{table_name}" ({column_names}) VALUES ({placeholders})'
            )

            batch_data = []
            for row in rows:
                row_data = [getattr(row, col, None) for col in columns]
                batch_data.append(row_data)

            cursor.executemany(insert_sql, batch_data)

        conn.commit()
        conn.close()

        # Upload to S3 if needed
        if sqlite_file != local_file:
            _upload_to_s3(local_file, sqlite_file)
            os.remove(local_file)

        return [f"Partition {partition_id}: {len(rows)} rows written to {sqlite_file}"]

    # Execute partitioned write
    results = partitioned_df.mapPartitionsWithIndex(write_partition_to_sqlite).collect()

    for result in results:
        for message in result:
            logger.info(f"_write_partitioned_sqlite_files: {message}")

    logger.info(
        f"_write_partitioned_sqlite_files: Successfully created SQLite files in {output_dir}"
    )
    return output_dir


def _upload_to_s3(local_path, s3_path):
    """Upload local file to S3."""
    try:
        import boto3

        # Parse S3 path
        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]
        elif s3_path.startswith("s3a://"):
            s3_path = s3_path[6:]

        bucket, key = s3_path.split("/", 1)

        s3_client = boto3.client("s3")
        logger.info(f"_upload_to_s3: Uploading {local_path} to s3://{bucket}/{key}")
        s3_client.upload_file(local_path, bucket, key)
        logger.info("_upload_to_s3: Upload completed successfully")

    except ImportError:
        logger.error("_upload_to_s3: boto3 not available - cannot upload to S3")
        raise
    except Exception as e:
        logger.error(f"_upload_to_s3: Upload failed: {e}")
        raise


@log_execution_time
def convert_parquet_to_sqlite(
    input_path,
    output_path,
    table_name="data",
    method="optimized",
    max_records_per_file=500000,
):
    """
    Convert parquet files to SQLite database(s).

    Args:
        input_path: Path to parquet files (local or S3)
        output_path: Path for SQLite output (local or S3)
        table_name: Name of table in SQLite database
        method: Conversion method - "single_file", "partitioned", "optimized"
        max_records_per_file: Maximum records per SQLite file for partitioned mode

    Returns:
        str: Path to created SQLite file(s)
    """
    logger.info("=" * 60)
    logger.info("PARQUET TO SQLITE CONVERTER")
    logger.info("=" * 60)
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")
    logger.info(f"Table name: {table_name}")
    logger.info(f"Method: {method}")

    # Create Spark session
    spark = create_spark_session("ParquetToSQLiteConverter")

    try:
        # Read parquet files
        logger.info("convert_parquet_to_sqlite: Reading parquet files")
        df = spark.read.parquet(input_path)

        # Show DataFrame info
        row_count = df.count()
        logger.info(
            f"convert_parquet_to_sqlite: Loaded {row_count} rows from parquet files"
        )
        logger.info(f"convert_parquet_to_sqlite: Schema: {len(df.columns)} columns")

        # Log column names for debugging
        logger.info(f"convert_parquet_to_sqlite: Columns: {df.columns}")

        # Convert to SQLite
        result_path = write_to_sqlite_file(
            df, output_path, table_name, method, max_records_per_file
        )

        logger.info("=" * 60)
        logger.info("CONVERSION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"SQLite output: {result_path}")
        logger.info(f"Records converted: {row_count}")

        return result_path

    except Exception as e:
        logger.error(f"convert_parquet_to_sqlite: Conversion failed: {e}")
        raise
    finally:
        spark.stop()


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Convert Parquet files to SQLite databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert S3 parquet to local SQLite
  python parquet_to_sqlite.py --input s3://my-bucket/data/parquet/ --output ./data.sqlite
  
  # Convert to multiple SQLite files
  python parquet_to_sqlite.py --input s3://my-bucket/data/parquet/ --output ./sqlite-output/ --method partitioned
  
  # Upload SQLite to S3
  python parquet_to_sqlite.py --input s3://my-bucket/data/parquet/ --output s3://my-bucket/sqlite-output/
        """,
    )

    parser.add_argument(
        "--input", "-i", required=True, help="Input path to parquet files (local or S3)"
    )

    parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output path for SQLite file(s) (local or S3)",
    )

    parser.add_argument(
        "--table-name",
        "-t",
        default="data",
        help="Name of table in SQLite database (default: data)",
    )

    parser.add_argument(
        "--method",
        "-m",
        choices=["single_file", "partitioned", "optimized"],
        default="optimized",
        help="Conversion method (default: optimized)",
    )

    parser.add_argument(
        "--max-records",
        type=int,
        default=500000,
        help="Maximum records per SQLite file for partitioned method (default: 500000)",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        import logging

        logging.getLogger().setLevel(logging.DEBUG)

    try:
        result = convert_parquet_to_sqlite(
            input_path=args.input,
            output_path=args.output,
            table_name=args.table_name,
            method=args.method,
            max_records_per_file=args.max_records,
        )

        print(f"\n✅ Conversion completed successfully!")
        print(f"📁 SQLite output: {result}")

    except Exception as e:
        print(f"\n❌ Conversion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
