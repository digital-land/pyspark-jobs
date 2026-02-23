"""S3 Writer utilities for data transformation and writing."""

import re
from typing import List, Optional

import boto3
from pyspark.sql.functions import lit

from jobs.utils.logger_config import get_logger, log_execution_time

logger = get_logger(__name__)

df_entity = None


@log_execution_time
def write_parquet(df, output_path: str, partition_by: Optional[List[str]] = None):
    """Write DataFrame in Parquet format.

    Args:
        df: PySpark DataFrame to write.
        output_path: Destination path (local or s3://).
        partition_by: Columns to partition by. If None, writes without partitioning.
    """
    logger.info(f"write_parquet: Writing data to {output_path}")

    row_count = df.count()
    optimal_partitions = max(1, min(200, row_count // 1000000))

    writer = (
        df.coalesce(optimal_partitions)
        .write.mode("append")
        .option("maxRecordsPerFile", 1000000)
        .option("compression", "snappy")
    )

    if partition_by:
        writer.partitionBy(*partition_by).parquet(output_path)
    else:
        writer.parquet(output_path)

    logger.info(f"write_parquet: Successfully wrote {row_count} rows")


def cleanup_temp_path(env, dataset_name):
    """Delete all objects in the temp S3 path for a dataset."""
    s3_client = boto3.client("s3")
    bucket_name = f"{env}-collection-data"
    prefix = f"dataset/temp/{dataset_name}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            logger.info(f"Deleted {len(objects)} objects from {prefix}")


def resolve_geometry(
    geometry_wkt: Optional[str], point_wkt: Optional[str]
) -> Optional[dict]:
    """Convert geometry WKT to GeoJSON, falling back to point WKT if geometry is absent."""
    wkt = geometry_wkt or point_wkt
    return wkt_to_geojson(wkt) if wkt else None


def wkt_to_geojson(wkt_string):
    """Convert WKT geometry string to GeoJSON geometry object."""
    if not wkt_string:
        return None

    wkt_string = wkt_string.strip()

    if wkt_string.startswith("POINT"):
        coords = re.findall(r"[-\d.]+", wkt_string)
        return {"type": "Point", "coordinates": [float(coords[0]), float(coords[1])]}

    elif wkt_string.startswith("POLYGON"):
        rings = re.findall(r"\(([^()]+)\)", wkt_string)
        coordinates = []
        for ring in rings:
            points = []
            coords = re.findall(r"([-\d.]+)\s+([-\d.]+)", ring)
            for lon, lat in coords:
                points.append([float(lon), float(lat)])
            coordinates.append(points)
        return {"type": "Polygon", "coordinates": coordinates}

    elif wkt_string.startswith("MULTIPOLYGON"):
        wkt_string = wkt_string.replace("MULTIPOLYGON ", "").strip()
        polygons = []
        depth = 0
        current_polygon = ""

        for char in wkt_string:
            if char == "(":
                depth += 1
                if depth > 1:
                    current_polygon += char
            elif char == ")":
                depth -= 1
                if depth > 0:
                    current_polygon += char
                elif depth == 0 and current_polygon:
                    rings = re.findall(r"\(([^()]+)\)", current_polygon)
                    coordinates = []
                    for ring in rings:
                        points = []
                        coords = re.findall(r"([-\d.]+)\s+([-\d.]+)", ring)
                        for lon, lat in coords:
                            points.append([float(lon), float(lat)])
                        coordinates.append(points)
                    polygons.append(coordinates)
                    current_polygon = ""
            elif depth > 0:
                current_polygon += char

        if len(polygons) == 1:
            return {"type": "Polygon", "coordinates": polygons[0]}
        return {"type": "MultiPolygon", "coordinates": polygons}

    return None


def s3_rename_and_move(dataset_name, file_type, bucket_name):
    """Rename and move files in S3."""
    s3_client = boto3.client("s3")
    unique_data_filename = f"{dataset_name}.{file_type}"
    target_key = f"dataset/{unique_data_filename}"

    try:
        s3_client.head_object(Bucket=bucket_name, Key=target_key)
        s3_client.delete_object(Bucket=bucket_name, Key=target_key)
        logger.info(f"Deleted existing file: {target_key}")
    except s3_client.exceptions.ClientError:
        logger.info(f"No existing file to delete: {target_key}")

    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=f"dataset/temp/{dataset_name}/"
    )
    data_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(f".{file_type}")
    ]

    for data_file in data_files:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": data_file},
            Key=target_key,
        )
        s3_client.delete_object(Bucket=bucket_name, Key=data_file)
        logger.info(f"Renamed: {data_file} -> {target_key}")


def ensure_schema_fields(df, dataset_name):
    """Ensure DataFrame has all required fields from schema specification."""
    try:
        import requests

        url = f"https://raw.githubusercontent.com/digital-land/specification/main/content/dataset/{dataset_name}.md"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        content = response.text
        fields = []
        in_frontmatter = False
        in_fields_section = False

        for line in content.split("\n"):
            if line.strip() == "---":
                if not in_frontmatter:
                    in_frontmatter = True
                else:
                    break
                continue

            if in_frontmatter:
                if line.startswith("fields:"):
                    in_fields_section = True
                    continue
                if in_fields_section:
                    if line.startswith("- field:"):
                        field_name = line.split("- field:")[1].strip()
                        fields.append(field_name)
                    elif not line.startswith(" ") and not line.startswith("-"):
                        in_fields_section = False

        if not fields:
            return df

        current_columns = set(df.columns)
        missing_fields = [field for field in fields if field not in current_columns]

        if missing_fields:
            existing_cols = df.columns
            for field in missing_fields:
                df = df.withColumn(field, lit(""))
            final_columns = existing_cols + missing_fields
            df = df.select(final_columns)

        return df
    except Exception as e:
        logger.error(f"Error ensuring schema fields: {e}")
        return df
