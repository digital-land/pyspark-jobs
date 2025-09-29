#!/usr/bin/env python3
"""
CSV S3 Writer and Aurora Import Module

This module provides functionality to:
1. Write PySpark DataFrames to CSV files in S3
2. Read CSV data from S3
3. Import CSV data into Aurora PostgreSQL using the S3 import feature
4. Provide failover mechanism between S3 import and JDBC methods

Features:
- Optimized CSV writing with configurable partitioning
- Aurora PostgreSQL S3 import with proper error handling
- Configuration-based failover between import methods
- Support for geometry and complex data types
- Comprehensive logging and error handling

Usage:
    from jobs.csv_s3_writer import write_dataframe_to_csv_s3, import_csv_to_aurora
    
    # Write DataFrame to CSV in S3
    csv_path = write_dataframe_to_csv_s3(df, "s3://bucket/path/", "entity", "my-dataset")
    
    # Import CSV from S3 to Aurora using S3 import
    import_csv_to_aurora(csv_path, "entity", use_s3_import=True)
"""

import argparse
import sys
import os
import boto3
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime

# Add the jobs package to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jobs.utils.logger_config import get_logger, log_execution_time
from jobs.utils.aws_secrets_manager import get_secret_emr_compatible
from jobs.utils.s3_utils import cleanup_dataset_data, validate_s3_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, to_json, date_format, lit, coalesce
from pyspark.sql.types import StringType, DateType

logger = get_logger(__name__)


class CSVWriterError(Exception):
    """Custom exception for CSV writer related errors."""
    pass


class AuroraImportError(Exception):
    """Custom exception for Aurora import related errors."""
    pass


# ==================== CONFIGURATION ====================

# Simple CSV writing configuration
CSV_CONFIG = {
    "include_header": True,
    "escape_char": '"',
    "quote_char": '"',
    "sep": ",",
    "null_value": "",
    "date_format": "yyyy-MM-dd",
    "timestamp_format": "yyyy-MM-dd HH:mm:ss",
    "coalesce_to_single_file": True,  # For Aurora S3 import compatibility
}


# ==================== SPARK SESSION ====================

def create_spark_session_for_csv(app_name="CSVWriterAndAuroraImport"):
    """
    Create and configure Spark session for CSV operations.
    
    Returns:
        SparkSession: Configured Spark session
    """
    logger.info(f"create_spark_session_for_csv: Creating Spark session: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    logger.info("create_spark_session_for_csv: Spark session created successfully")
    return spark


# ==================== CSV PREPARATION ====================

def prepare_dataframe_for_csv(df):
    """
    Prepare DataFrame for CSV export with proper data type handling.
    
    This function ensures that all data types are compatible with CSV format
    and Aurora PostgreSQL S3 import requirements.
    
    Args:
        df: PySpark DataFrame to prepare
        
    Returns:
        DataFrame: Prepared DataFrame for CSV export
    """
    logger.info("prepare_dataframe_for_csv: Preparing DataFrame for CSV export")
    
    processed_df = df
    
    # Get all columns and their types
    columns_info = [(field.name, str(field.dataType).lower()) for field in df.schema.fields]
    logger.info(f"prepare_dataframe_for_csv: Processing {len(columns_info)} columns")
    
    for col_name, col_type in columns_info:
        logger.debug(f"prepare_dataframe_for_csv: Processing column {col_name} of type {col_type}")
        
        # Handle JSON/struct columns - convert to JSON strings
        if "struct" in col_type or "map" in col_type or col_name.lower() in ["geojson", "json"]:
            logger.info(f"prepare_dataframe_for_csv: Converting {col_name} to JSON string")
            processed_df = processed_df.withColumn(
                col_name,
                when(col(col_name).isNull(), None)
                .otherwise(to_json(col(col_name)))
            )
        
        # Handle date/timestamp columns - convert to standardized format
        elif "date" in col_type or "timestamp" in col_type or col_name.lower().endswith('_date'):
            logger.info(f"prepare_dataframe_for_csv: Converting {col_name} to standardized date format")
            if "timestamp" in col_type:
                # For timestamps, use full datetime format
                processed_df = processed_df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), None)
                    .when(col(col_name) == "", None)
                    .otherwise(date_format(col(col_name), CSV_CONFIG["timestamp_format"]))
                )
            else:
                # For dates, use date-only format
                processed_df = processed_df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), None)
                    .when(col(col_name) == "", None)
                    .otherwise(date_format(col(col_name), CSV_CONFIG["date_format"]))
                )
        
        # Handle geometry columns - ensure proper text format for Aurora
        elif col_name.lower() in ["geometry", "point", "geom", "wkt"] or "geometry" in col_type:
            logger.info(f"prepare_dataframe_for_csv: Converting {col_name} to WKT text for Aurora compatibility")
            processed_df = processed_df.withColumn(
                col_name,
                when(col(col_name).isNull(), None)
                .when(col(col_name) == "", None)
                .when(col(col_name).startswith("POINT"), col(col_name))
                .when(col(col_name).startswith("POLYGON"), col(col_name))
                .when(col(col_name).startswith("MULTIPOLYGON"), col(col_name))
                .otherwise(None)  # Invalid geometry → NULL
            )
        
        # Handle boolean columns - convert to text for CSV compatibility
        elif "boolean" in col_type or col_name.lower().endswith('_flag'):
            logger.info(f"prepare_dataframe_for_csv: Converting {col_name} to text boolean")
            processed_df = processed_df.withColumn(
                col_name,
                when(col(col_name).isNull(), None)
                .when(col(col_name) == True, "true")
                .when(col(col_name) == False, "false")
                .otherwise(col(col_name).cast(StringType()))
            )
    
    logger.info("prepare_dataframe_for_csv: DataFrame preparation completed")
    return processed_df


# ==================== CSV WRITING ====================

@log_execution_time
def write_dataframe_to_csv_s3(
    df, 
    output_path: str, 
    table_name: str, 
    dataset_name: str,
    cleanup_existing: bool = True,
    csv_config: Optional[Dict] = None,
    temp_folder: bool = True
) -> str:
    """
    Write PySpark DataFrame to temporary CSV file(s) in S3.
    
    This function writes the DataFrame to CSV format optimized for Aurora S3 import.
    The CSV files are intended to be temporary and should be cleaned up after import.
    
    Args:
        df: PySpark DataFrame to write
        output_path: S3 path where CSV files should be created (e.g., "s3://bucket/path/")
        table_name: Name of the table (used for file naming)
        dataset_name: Name of the dataset (used for partitioning/cleanup)
        cleanup_existing: Whether to cleanup existing files before writing
        csv_config: Optional CSV configuration overrides
        temp_folder: Whether to use temporary folder structure (default: True)
        
    Returns:
        str: S3 path to the created CSV file(s)
        
    Raises:
        CSVWriterError: If CSV writing fails
    """
    logger.info("=" * 60)
    logger.info("CSV S3 WRITER")
    logger.info("=" * 60)
    logger.info(f"Output path: {output_path}")
    logger.info(f"Table name: {table_name}")
    logger.info(f"Dataset name: {dataset_name}")
    
    # Merge configuration
    config = {**CSV_CONFIG, **(csv_config or {})}
    
    try:
        # Validate S3 path
        if not validate_s3_path(output_path):
            raise CSVWriterError(f"Invalid S3 path format: {output_path}")
        
        # Prepare DataFrame for CSV export
        processed_df = prepare_dataframe_for_csv(df)
        row_count = processed_df.count()
        logger.info(f"write_dataframe_to_csv_s3: Processing {row_count} rows")
        
        # Cleanup existing data if requested
        if cleanup_existing:
            cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
            logger.info(f"write_dataframe_to_csv_s3: Cleaned up {cleanup_summary['objects_deleted']} objects")
            if cleanup_summary['errors']:
                logger.warning(f"write_dataframe_to_csv_s3: Cleanup warnings: {cleanup_summary['errors']}")
        
        # Create temporary folder path with timestamp for uniqueness
        timestamp = int(time.time())
        if temp_folder:
            temp_output_path = f"{output_path.rstrip('/')}/temp-csv-{timestamp}/"
            logger.info(f"write_dataframe_to_csv_s3: Using temporary folder: {temp_output_path}")
        else:
            temp_output_path = output_path
        
        # Always use single file for Aurora S3 import compatibility
        logger.info("write_dataframe_to_csv_s3: Using single file output strategy for Aurora compatibility")
        return _write_single_csv_file(processed_df, temp_output_path, table_name, dataset_name, config, timestamp)
            
    except Exception as e:
        logger.error(f"write_dataframe_to_csv_s3: Failed to write CSV to S3: {e}")
        raise CSVWriterError(f"CSV writing failed: {e}")


def _write_single_csv_file(df, output_path: str, table_name: str, dataset_name: str, config: Dict, timestamp: int) -> str:
    """Write DataFrame to a single temporary CSV file."""
    logger.info("_write_single_csv_file: Writing DataFrame to temporary CSV file")
    
    # Create temporary staging path and final CSV path  
    temp_path = f"{output_path.rstrip('/')}/staging_{timestamp}/"
    final_csv_path = f"{output_path.rstrip('/')}/{table_name}_{dataset_name}_{timestamp}.csv"
    
    try:
        # Write DataFrame to temporary location
        writer = df.coalesce(1).write \
            .format("csv") \
            .option("header", str(config["include_header"]).lower()) \
            .option("sep", config["sep"]) \
            .option("quote", config["quote_char"]) \
            .option("escape", config["escape_char"]) \
            .option("nullValue", config["null_value"]) \
            .option("dateFormat", config["date_format"]) \
            .option("timestampFormat", config["timestamp_format"]) \
            .mode("overwrite")
        
        # Add compression if specified
        if config["compression"]:
            writer = writer.option("compression", config["compression"])
            
        writer.save(temp_path)
        
        logger.info(f"_write_single_csv_file: CSV written to temporary location: {temp_path}")
        
        # Move the CSV file to final location with proper name
        final_path = _move_csv_to_final_location(temp_path, final_csv_path)
        
        logger.info(f"_write_single_csv_file: Successfully created CSV file: {final_path}")
        return final_path
        
    except Exception as e:
        logger.error(f"_write_single_csv_file: Failed to write single CSV file: {e}")
        # Attempt cleanup
        _cleanup_temp_path(temp_path)
        raise


def _write_multiple_csv_files(df, output_path: str, table_name: str, dataset_name: str, config: Dict) -> str:
    """Write DataFrame to multiple CSV files."""
    logger.info("_write_multiple_csv_files: Writing DataFrame to multiple CSV files")
    
    row_count = df.count()
    num_files = (row_count // config["max_records_per_file"]) + 1
    logger.info(f"_write_multiple_csv_files: Creating {num_files} CSV files")
    
    # Create output directory path
    csv_dir_path = f"{output_path.rstrip('/')}/csv/{table_name}_{dataset_name}/"
    
    try:
        # Repartition DataFrame for optimal file distribution
        partitioned_df = df.repartition(num_files)
        
        # Write CSV files
        writer = partitioned_df.write \
            .format("csv") \
            .option("header", str(config["include_header"]).lower()) \
            .option("sep", config["sep"]) \
            .option("quote", config["quote_char"]) \
            .option("escape", config["escape_char"]) \
            .option("nullValue", config["null_value"]) \
            .option("dateFormat", config["date_format"]) \
            .option("timestampFormat", config["timestamp_format"]) \
            .mode("overwrite")
        
        # Add compression if specified
        if config["compression"]:
            writer = writer.option("compression", config["compression"])
            
        writer.save(csv_dir_path)
        
        logger.info(f"_write_multiple_csv_files: Successfully created CSV files in: {csv_dir_path}")
        return csv_dir_path
        
    except Exception as e:
        logger.error(f"_write_multiple_csv_files: Failed to write multiple CSV files: {e}")
        raise


def _move_csv_to_final_location(temp_path: str, final_csv_path: str) -> str:
    """Move CSV file from temporary location to final location with proper naming."""
    try:
        import boto3
        from urllib.parse import urlparse
        
        # Parse S3 paths
        temp_uri = urlparse(temp_path)
        final_uri = urlparse(final_csv_path)
        
        bucket = temp_uri.netloc
        temp_prefix = temp_uri.path.lstrip('/')
        final_key = final_uri.path.lstrip('/')
        
        s3_client = boto3.client('s3')
        
        # List objects in temp location to find the CSV file
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=temp_prefix)
        
        csv_key = None
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                csv_key = obj['Key']
                break
        
        if not csv_key:
            raise CSVWriterError("No CSV file found in temporary location")
        
        # Copy to final location
        copy_source = {'Bucket': bucket, 'Key': csv_key}
        s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=final_key)
        
        # Delete temporary files
        _cleanup_temp_path(temp_path)
        
        logger.info(f"_move_csv_to_final_location: Moved CSV to final location: {final_csv_path}")
        return final_csv_path
        
    except Exception as e:
        logger.error(f"_move_csv_to_final_location: Failed to move CSV file: {e}")
        raise CSVWriterError(f"Failed to move CSV file: {e}")


def _cleanup_temp_path(temp_path: str):
    """Clean up temporary files in S3."""
    try:
        import boto3
        from urllib.parse import urlparse
        
        uri = urlparse(temp_path)
        bucket = uri.netloc
        prefix = uri.path.lstrip('/')
        
        s3_client = boto3.client('s3')
        
        # List and delete all objects with the temporary prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': objects_to_delete}
                )
        
        logger.info(f"_cleanup_temp_path: Cleaned up temporary files at: {temp_path}")
        
    except Exception as e:
        logger.warning(f"_cleanup_temp_path: Failed to cleanup temporary files: {e}")


def cleanup_temp_csv_files(csv_path: str):
    """
    Clean up temporary CSV files after successful import.
    
    Args:
        csv_path: S3 path to the CSV file(s) to clean up
    """
    try:
        import boto3
        from urllib.parse import urlparse
        
        logger.info(f"cleanup_temp_csv_files: Cleaning up temporary CSV files at: {csv_path}")
        
        uri = urlparse(csv_path)
        bucket = uri.netloc
        
        s3_client = boto3.client('s3')
        
        if csv_path.endswith('.csv'):
            # Single file cleanup
            key = uri.path.lstrip('/')
            s3_client.delete_object(Bucket=bucket, Key=key)
            logger.info(f"cleanup_temp_csv_files: Deleted CSV file: {csv_path}")
        else:
            # Directory cleanup - delete all files in the temp directory
            prefix = uri.path.lstrip('/')
            if not prefix.endswith('/'):
                prefix += '/'
                
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    # Delete in batches of 1000 (S3 limit)
                    for i in range(0, len(objects_to_delete), 1000):
                        batch = objects_to_delete[i:i+1000]
                        s3_client.delete_objects(
                            Bucket=bucket,
                            Delete={'Objects': batch}
                        )
                    
                    logger.info(f"cleanup_temp_csv_files: Deleted {len(objects_to_delete)} temporary files")
                else:
                    logger.info("cleanup_temp_csv_files: No temporary files found to clean up")
            else:
                logger.info("cleanup_temp_csv_files: No temporary files found to clean up")
        
        logger.info("cleanup_temp_csv_files: Temporary CSV cleanup completed successfully")
        
    except Exception as e:
        # Log warning but don't fail the import process due to cleanup issues
        logger.warning(f"cleanup_temp_csv_files: Failed to cleanup temporary CSV files: {e}")
        logger.warning("cleanup_temp_csv_files: Import was successful, but manual cleanup may be needed")


# ==================== CSV READING ====================

@log_execution_time
def read_csv_from_s3(spark, csv_path: str, infer_schema: bool = True) -> 'DataFrame':
    """
    Read CSV data from S3 into a PySpark DataFrame.
    
    Args:
        spark: SparkSession instance
        csv_path: S3 path to CSV file(s)
        infer_schema: Whether to infer schema automatically
        
    Returns:
        DataFrame: Loaded PySpark DataFrame
        
    Raises:
        CSVWriterError: If reading fails
    """
    logger.info(f"read_csv_from_s3: Reading CSV data from {csv_path}")
    
    try:
        # Validate S3 path
        if not validate_s3_path(csv_path):
            raise CSVWriterError(f"Invalid S3 path format: {csv_path}")
        
        # Read CSV with optimized settings
        reader = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", str(infer_schema).lower()) \
            .option("multiline", "true") \
            .option("escape", '"') \
            .option("quote", '"')
        
        df = reader.load(csv_path)
        
        row_count = df.count()
        logger.info(f"read_csv_from_s3: Successfully loaded {row_count} rows from {csv_path}")
        
        return df
        
    except Exception as e:
        logger.error(f"read_csv_from_s3: Failed to read CSV from S3: {e}")
        raise CSVWriterError(f"Failed to read CSV: {e}")


# ==================== AURORA S3 IMPORT ====================

def get_aurora_connection_params() -> Dict[str, str]:
    """
    Get Aurora PostgreSQL connection parameters from AWS Secrets Manager.
    
    Returns:
        Dict containing connection parameters
        
    Raises:
        AuroraImportError: If secrets cannot be retrieved
    """
    try:
        logger.info("get_aurora_connection_params: Retrieving Aurora connection parameters")
        
        # Use the existing secret retrieval method
        aws_secrets_json = get_secret_emr_compatible("dev/pyspark/postgres")
        secrets = json.loads(aws_secrets_json)
        
        # Extract required fields
        conn_params = {
            "host": secrets.get("host"),
            "port": secrets.get("port", "5432"),
            "database": secrets.get("dbName"),
            "username": secrets.get("username"),
            "password": secrets.get("password"),
        }
        
        # Validate required fields
        missing_fields = [k for k, v in conn_params.items() if not v]
        if missing_fields:
            raise AuroraImportError(f"Missing connection parameters: {missing_fields}")
        
        logger.info("get_aurora_connection_params: Successfully retrieved connection parameters")
        return conn_params
        
    except Exception as e:
        logger.error(f"get_aurora_connection_params: Failed to get connection params: {e}")
        raise AuroraImportError(f"Failed to get Aurora connection parameters: {e}")


@log_execution_time
def import_csv_to_aurora(
    csv_s3_path: str,
    table_name: str,
    dataset_name: str,
    use_s3_import: bool = True,
    truncate_table: bool = True
) -> Dict[str, Any]:
    """
    Import CSV data from S3 into Aurora PostgreSQL table.
    
    Args:
        csv_s3_path: S3 path to CSV file(s)
        table_name: Target table name in Aurora
        dataset_name: Dataset name for filtering/cleanup
        use_s3_import: Whether to use S3 import (True) or JDBC import (False)
        truncate_table: Whether to truncate table before import
        
    Returns:
        Dict containing import results and metadata
        
    Raises:
        AuroraImportError: If import fails
    """
    logger.info("=" * 60)
    logger.info("AURORA CSV IMPORT")
    logger.info("=" * 60)
    logger.info(f"CSV S3 path: {csv_s3_path}")
    logger.info(f"Target table: {table_name}")
    logger.info(f"Dataset: {dataset_name}")
    logger.info(f"Method: {'Aurora S3 Import' if use_s3_import else 'JDBC Import'}")
    
    results = {
        "csv_path": csv_s3_path,
        "table_name": table_name,
        "dataset_name": dataset_name,
        "import_method_used": None,
        "import_successful": False,
        "rows_imported": 0,
        "import_duration": 0,
        "errors": [],
        "warnings": []
    }
    
    start_time = time.time()
    
    try:
        if use_s3_import:
            logger.info("import_csv_to_aurora: Using Aurora S3 import method")
            s3_result = _import_via_aurora_s3(csv_s3_path, table_name, dataset_name, truncate_table)
            results.update(s3_result)
            results["import_method_used"] = "aurora_s3"
            results["import_successful"] = True
            logger.info("import_csv_to_aurora: Aurora S3 import completed successfully")
        else:
            logger.info("import_csv_to_aurora: Using JDBC import method")
            jdbc_result = _import_via_jdbc(csv_s3_path, table_name, dataset_name, truncate_table)
            results.update(jdbc_result)
            results["import_method_used"] = "jdbc"
            results["import_successful"] = True
            logger.info("import_csv_to_aurora: JDBC import completed successfully")
        
        results["import_duration"] = time.time() - start_time
        
        # Clean up temporary CSV files after successful import
        logger.info("import_csv_to_aurora: Cleaning up temporary CSV files")
        cleanup_temp_csv_files(csv_s3_path)
        
        # Log final results
        logger.info("=" * 60)
        logger.info("IMPORT COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Method used: {results['import_method_used']}")
        logger.info(f"Rows imported: {results['rows_imported']}")
        logger.info(f"Duration: {results['import_duration']:.2f} seconds")
        logger.info("Temporary CSV files cleaned up to reduce storage costs")
        
        return results
        
    except Exception as e:
        results["import_duration"] = time.time() - start_time
        results["errors"].append(f"Import process failed: {e}")
        logger.error(f"import_csv_to_aurora: Import process failed: {e}")
        
        # Clean up temporary CSV files even if import fails to avoid storage costs
        logger.info("import_csv_to_aurora: Cleaning up temporary CSV files after import failure")
        cleanup_temp_csv_files(csv_s3_path)
        
        raise


def _import_via_aurora_s3(csv_s3_path: str, table_name: str, dataset_name: str, truncate_table: bool) -> Dict[str, Any]:
    """Import CSV data using Aurora PostgreSQL S3 import feature."""
    logger.info("_import_via_aurora_s3: Starting Aurora S3 import")
    
    try:
        import psycopg2
        from urllib.parse import urlparse
    except ImportError:
        raise AuroraImportError("psycopg2 is required for Aurora S3 import but not available")
    
    # Get connection parameters
    conn_params = get_aurora_connection_params()
    
    # Parse S3 path
    s3_uri = urlparse(csv_s3_path)
    bucket_name = s3_uri.netloc
    object_key = s3_uri.path.lstrip('/')
    region = "eu-west-2"  # Default region, could be made configurable
    
    conn = None
    cursor = None
    
    try:
        # Connect to Aurora PostgreSQL
        logger.info("_import_via_aurora_s3: Connecting to Aurora PostgreSQL")
        conn = psycopg2.connect(
            host=conn_params["host"],
            port=conn_params["port"],
            database=conn_params["database"],
            user=conn_params["username"],
            password=conn_params["password"],
            connect_timeout=30
        )
        cursor = conn.cursor()
        
        # Create aws_s3 extension if not exists
        logger.info("_import_via_aurora_s3: Ensuring aws_s3 extension is available")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")
        conn.commit()
        
        # Truncate table if requested
        if truncate_table:
            logger.info(f"_import_via_aurora_s3: Truncating table {table_name} for dataset {dataset_name}")
            cursor.execute(f"DELETE FROM {table_name} WHERE dataset = %s;", (dataset_name,))
            deleted_rows = cursor.rowcount
            conn.commit()
            logger.info(f"_import_via_aurora_s3: Deleted {deleted_rows} existing rows for dataset {dataset_name}")
        
        # Construct S3 import SQL
        import_sql = f"""
        SELECT aws_s3.table_import_from_s3(
            %s,  -- table_name
            %s,  -- column_list (empty for all columns)
            %s,  -- options (CSV format with header)
            aws_commons.create_s3_uri(%s, %s, %s)  -- S3 URI
        );
        """
        
        # Execute S3 import
        logger.info(f"_import_via_aurora_s3: Importing CSV from s3://{bucket_name}/{object_key}")
        cursor.execute(import_sql, (
            table_name,
            "",  # Empty column list means import all columns
            "(format csv, header true)",
            bucket_name,
            object_key,
            region
        ))
        
        # Get result
        result = cursor.fetchone()
        rows_imported = result[0] if result else 0
        
        conn.commit()
        
        logger.info(f"_import_via_aurora_s3: Successfully imported {rows_imported} rows")
        
        return {
            "rows_imported": rows_imported,
            "method_details": {
                "s3_bucket": bucket_name,
                "s3_key": object_key,
                "region": region
            }
        }
        
    except psycopg2.Error as e:
        logger.error(f"_import_via_aurora_s3: PostgreSQL error: {e}")
        if conn:
            conn.rollback()
        raise AuroraImportError(f"Aurora S3 import failed: {e}")
        
    except Exception as e:
        logger.error(f"_import_via_aurora_s3: Unexpected error: {e}")
        if conn:
            conn.rollback()
        raise AuroraImportError(f"Aurora S3 import failed: {e}")
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def _import_via_jdbc(csv_s3_path: str, table_name: str, dataset_name: str, truncate_table: bool) -> Dict[str, Any]:
    """Import CSV data using JDBC method (fallback)."""
    logger.info("_import_via_jdbc: Starting JDBC import")
    
    try:
        # This is a simplified version - in practice, you'd want to:
        # 1. Create a Spark session
        # 2. Read the CSV from S3
        # 3. Use the existing write_to_postgres function
        
        from jobs.dbaccess.postgres_connectivity import write_to_postgres, get_aws_secret
        
        # Create Spark session for reading CSV
        spark = create_spark_session_for_csv("JDBCImport")
        
        try:
            # Read CSV from S3
            df = read_csv_from_s3(spark, csv_s3_path)
            row_count = df.count()
            
            # Use existing JDBC writer
            write_to_postgres(df, dataset_name, get_aws_secret())
            
            logger.info(f"_import_via_jdbc: Successfully imported {row_count} rows via JDBC")
            
            return {
                "rows_imported": row_count,
                "method_details": {
                    "jdbc_method": "existing_write_to_postgres",
                    "csv_path": csv_s3_path
                }
            }
            
        finally:
            spark.stop()
            
    except Exception as e:
        logger.error(f"_import_via_jdbc: JDBC import failed: {e}")
        raise AuroraImportError(f"JDBC import failed: {e}")




# ==================== MAIN FUNCTION ====================

def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="CSV S3 Writer and Aurora Import Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Write DataFrame to CSV and import to Aurora
  python csv_s3_writer.py --input s3://bucket/parquet/ --output s3://bucket/csv/ --table entity --dataset my-dataset
  
  # Import existing CSV to Aurora using S3 import
  python csv_s3_writer.py --import-csv s3://bucket/csv/entity.csv --table entity --dataset my-dataset
  
  # Import using JDBC fallback
  python csv_s3_writer.py --import-csv s3://bucket/csv/entity.csv --table entity --dataset my-dataset --use-jdbc
        """
    )
    
    parser.add_argument(
        "--input", "-i",
        help="Input path to parquet files to convert to CSV"
    )
    
    parser.add_argument(
        "--output", "-o", 
        help="Output S3 path for CSV files"
    )
    
    parser.add_argument(
        "--import-csv",
        help="S3 path to existing CSV file to import"
    )
    
    parser.add_argument(
        "--table", "-t",
        required=True,
        help="Target table name in Aurora"
    )
    
    parser.add_argument(
        "--dataset", "-d",
        required=True,
        help="Dataset name"
    )
    
    parser.add_argument(
        "--use-jdbc",
        action="store_true",
        help="Use JDBC import instead of S3 import"
    )
    
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip cleanup of existing data"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if args.input and args.output:
            # Convert parquet to CSV and import
            spark = create_spark_session_for_csv()
            
            try:
                # Read parquet files
                logger.info(f"Reading parquet files from {args.input}")
                df = spark.read.parquet(args.input)
                
                # Write to CSV
                csv_path = write_dataframe_to_csv_s3(
                    df, 
                    args.output, 
                    args.table, 
                    args.dataset,
                    cleanup_existing=not args.no_cleanup
                )
                
                # Import to Aurora
                import_result = import_csv_to_aurora(
                    csv_path,
                    args.table,
                    args.dataset,
                    use_s3_import=not args.use_jdbc
                )
                
                print(f"\n✅ Conversion and import completed successfully!")
                print(f"📁 CSV path: {csv_path}")
                print(f"📊 Rows imported: {import_result['rows_imported']}")
                print(f"⚡ Method used: {import_result['import_method_used']}")
                
            finally:
                spark.stop()
                
        elif args.import_csv:
            # Import existing CSV
            import_result = import_csv_to_aurora(
                args.import_csv,
                args.table,
                args.dataset,
                use_s3_import=not args.use_jdbc
            )
            
            print(f"\n✅ Import completed successfully!")
            print(f"📊 Rows imported: {import_result['rows_imported']}")
            print(f"⚡ Method used: {import_result['import_method_used']}")
            
        else:
            parser.error("Either --input and --output, or --import-csv must be specified")
            
    except Exception as e:
        print(f"\n❌ Operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
