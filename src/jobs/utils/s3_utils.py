"""
S3 utilities for managing S3 operations in PySpark jobs.

This module provides utilities for common S3 operations including data cleanup,
bucket validation, and path management. It's designed to work with partitioned
datasets and handle large - scale data operations efficiently.

Usage:
    from utils.s3_utils import cleanup_dataset_data, validate_s3_path

    # Clean up existing dataset data before writing new data
    cleanup_dataset_data("s3://bucket/path/", "my - dataset")

    # Validate S3 path format
    is_valid = validate_s3_path("s3://bucket/path/")
"""

import logging
from typing import Tuple  # noqa: F401

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logger = logging.getLogger(__name__)


class S3UtilsError(Exception):
    """Custom exception for S3 utilities related errors."""

    pass


def read_csv_from_s3(spark, s3_path, header=True, infer_schema=True, **options):
    """
    Read CSV file(s) from S3 into a PySpark DataFrame.

    Args:
        spark: SparkSession instance
        s3_path: S3 path to CSV file(s) (e.g., 's3://bucket/path/file.csv')
        header: Whether CSV has header row (default: True)
        infer_schema: Whether to infer schema automatically (default: True)
        **options: Additional Spark CSV reader options

    Returns:
        PySpark DataFrame
    """
    return spark.read.csv(s3_path, header=header, inferSchema=infer_schema, **options)


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Parse an S3 path into bucket and prefix components.

    Args:
        s3_path (str): S3 path (with or without s3:// prefix)

    Returns:
        Tuple[str, str]: (bucket_name, prefix)

    Raises:
        S3UtilsError: If the S3 path format is invalid
    """
    # Remove s3:// prefix if present
    if s3_path.startswith("s3://"):
        clean_path = s3_path[5:]
    else:
        clean_path = s3_path

    # Validate path format
    if "/" not in clean_path:
        raise S3UtilsError(
            f"Invalid S3 path format: {s3_path}. Expected format: s3://bucket/prefix"
        )

    # Split into bucket and prefix
    parts = clean_path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1]

    return bucket, prefix


def validate_s3_path(s3_path: str) -> bool:
    """
    Validate S3 path format.

    Args:
        s3_path (str): S3 path to validate

    Returns:
        bool: True if valid, False otherwise
    """
    try:
        parse_s3_path(s3_path)
        return True
    except S3UtilsError:
        return False


def cleanup_dataset_data(output_path: str, dataset_name: str) -> dict:
    """
    Check and delete existing S3 data for the specified dataset before writing new data.

    This function is designed to work with partitioned datasets that use the structure:
    s3://bucket/prefix/dataset={dataset_name}/year={year}/month={month}/day={day}/

    Args:
        output_path (str): S3 path where data will be written
        dataset_name (str): Name of the dataset to clean up

    Returns:
        dict: Summary of cleanup operation with keys:
            - objects_found: Total number of objects found
            - objects_deleted: Number of objects successfully deleted
            - errors: List of any errors encountered

    Raises:
        S3UtilsError: If there's a critical error during cleanup
    """
    cleanup_summary = {"objects_found": 0, "objects_deleted": 0, "errors": []}

    try:
        # Parse S3 path to get bucket and prefix
        bucket, prefix = parse_s3_path(output_path)

        # Construct the full prefix for this dataset's partitioned data
        dataset_prefix = f"{prefix}dataset={dataset_name}/"

        s3_client = boto3.client("s3")

        logger.info(
            f"cleanup_dataset_data: Checking for existing data in s3://{bucket}/{dataset_prefix}"
        )

        # List all objects with the dataset prefix
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=dataset_prefix)

        for page in pages:
            if "Contents" in page:
                cleanup_summary["objects_found"] += len(page["Contents"])

                # Prepare list of objects to delete
                objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]

                if objects_to_delete:
                    # Delete objects in batch (max 1000 per request)
                    for i in range(0, len(objects_to_delete), 1000):
                        batch = objects_to_delete[i : i + 1000]
                        try:
                            response = s3_client.delete_objects(
                                Bucket=bucket, Delete={"Objects": batch}
                            )

                            # Count successful deletions
                            if "Deleted" in response:
                                cleanup_summary["objects_deleted"] += len(
                                    response["Deleted"]
                                )

                            # Collect any deletion errors
                            if "Errors" in response:
                                for error in response["Errors"]:
                                    error_msg = f"Failed to delete {error['Key']}: {error['Message']}"
                                    cleanup_summary["errors"].append(error_msg)
                                    logger.warning(f"cleanup_dataset_data: {error_msg}")

                        except ClientError as e:
                            error_msg = f"S3 delete batch error: {e}"
                            cleanup_summary["errors"].append(error_msg)
                            logger.warning(f"cleanup_dataset_data: {error_msg}")

        # Log summary
        if cleanup_summary["objects_found"] > 0:
            logger.info(
                f"cleanup_dataset_data: Found {cleanup_summary['objects_found']} existing objects, "
                f"successfully deleted {cleanup_summary['objects_deleted']} objects for dataset '{dataset_name}'"
            )
        else:
            logger.info(
                f"cleanup_dataset_data: No existing data found for dataset '{dataset_name}' "
                f"at s3://{bucket}/{dataset_prefix}"
            )

        return cleanup_summary

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "NoSuchBucket":
            # Extract bucket name for error message
            bucket_name = _extract_bucket_name_safe(output_path)
            error_msg = f"Bucket '{bucket_name}' does not exist"
            logger.warning(
                f"cleanup_dataset_data: {error_msg}, proceeding with write operation"
            )
            cleanup_summary["errors"].append(error_msg)
            return cleanup_summary
        elif error_code == "AccessDenied":
            # Extract bucket name for error message
            bucket_name = _extract_bucket_name_safe(output_path)
            error_msg = f"Access denied to bucket '{bucket_name}'"
            logger.warning(
                f"cleanup_dataset_data: {error_msg}, proceeding with write operation"
            )
            cleanup_summary["errors"].append(error_msg)
            return cleanup_summary
        else:
            error_msg = f"S3 client error during cleanup: {e}"
            logger.warning(f"cleanup_dataset_data: {error_msg}")
            cleanup_summary["errors"].append(error_msg)
            return cleanup_summary

    except NoCredentialsError as e:
        error_msg = f"AWS credentials not found: {e}"
        logger.error(f"cleanup_dataset_data: {error_msg}")
        raise S3UtilsError(error_msg)

    except Exception as e:
        error_msg = f"Unexpected error during S3 cleanup: {e}"
        logger.warning(f"cleanup_dataset_data: {error_msg}")
        cleanup_summary["errors"].append(error_msg)
        return cleanup_summary


def _extract_bucket_name_safe(output_path: str) -> str:
    """
    Safely extract bucket name from S3 path for error reporting.

    Args:
        output_path (str): S3 path

    Returns:
        str: Bucket name or a safe fallback
    """
    try:
        bucket, _ = parse_s3_path(output_path)
        return bucket
    except S3UtilsError:
        # Fallback: try to extract something useful for error reporting
        clean_path = output_path[5:] if output_path.startswith("s3://") else output_path
        return clean_path.split("/")[0] if "/" in clean_path else clean_path


def validate_s3_bucket_access(bucket_name: str) -> bool:
    """
    Validate that the S3 bucket exists and is accessible.

    Args:
        bucket_name (str): Name of the S3 bucket to validate

    Returns:
        bool: True if bucket is accessible, False otherwise
    """
    try:
        s3_client = boto3.client("s3")
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"validate_s3_bucket_access: Bucket '{bucket_name}' is accessible")
        return True

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.warning(
            f"validate_s3_bucket_access: Bucket '{bucket_name}' validation failed: {error_code}"
        )
        return False

    except NoCredentialsError:
        logger.warning("validate_s3_bucket_access: AWS credentials not available")
        return False

    except Exception as e:
        logger.warning(f"validate_s3_bucket_access: Unexpected error: {e}")
        return False
