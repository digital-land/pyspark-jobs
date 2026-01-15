"""S3 Writer utilities for data transformation and writing."""
import json
import re
from datetime import date as date_type
from datetime import datetime as datetime_type

import boto3
from pyspark.sql.functions import (
    col,
    dayofmonth,
    lit,
    month,
    to_date,
    year,
)
from pyspark.sql.types import TimestampType

from jobs.transform.entity_transformer import EntityTransformer
from jobs.utils.df_utils import count_df, show_df
from jobs.utils.flatten_csv import flatten_json_column
from jobs.utils.geometry_utils import calculate_centroid
from jobs.utils.logger_config import get_logger, log_execution_time
from jobs.utils.s3_utils import cleanup_dataset_data, read_csv_from_s3

logger = get_logger(__name__)

df_entity = None


@log_execution_time
def transform_data_entity_format(df, data_set, spark, env=None):
    """Transform Entity-Attribute-Value (EAV) format data into entity records."""
    transformer = EntityTransformer()
    return transformer.transform(df, data_set, spark, env)


@log_execution_time
def normalise_dataframe_schema(df, schema_name, data_set, spark, env=None):
    """Normalize dataframe schema based on table type."""
    try:
        from jobs.main_collection_data import load_metadata

        dataset_json_transformed_path = "config/transformed_source.json"
        logger.info(
            f"normalise_dataframe_schema: Transforming data for table: {schema_name}"
        )
        json_data = load_metadata(dataset_json_transformed_path)
        show_df(df, 5, env)

        # Extract the list of fields
        if schema_name in ["fact", "fact_res", "entity"]:
            fields = json_data.get("schema_fact_res_fact_entity", [])
        elif schema_name == "issue":
            fields = json_data.get("schema_issue", [])
        else:
            fields = []

        logger.info(f"normalise_dataframe_schema: Fields for {schema_name}: {fields}")

        # Replace hyphens with underscores in column names
        for column in df.columns:
            if "-" in column:
                df = df.withColumnRenamed(column, column.replace("-", "_"))

        logger.info(f"normalise_dataframe_schema: Columns after renaming: {df.columns}")
        show_df(df, 5, env)

        if schema_name == "entity":
            return transform_data_entity_format(df, data_set, spark, env)
        else:
            raise ValueError(f"Unknown table name: {schema_name}")

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


@log_execution_time
def write_to_s3(df, output_path, dataset_name, table_name, env=None):
    """Write DataFrame to S3 in Parquet format with partitioning."""
    try:
        from datetime import datetime

        logger.info(f"write_to_s3: Writing data to S3 at {output_path}")

        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.info(
            f"write_to_s3: Cleaned up {cleanup_summary['objects_deleted']} objects"
        )

        df = df.withColumn("dataset", lit(dataset_name))
        df = df.withColumn("entry_date_parsed", to_date("entry_date", "yyyy-MM-dd"))
        df = (
            df.withColumn("year", year("entry_date_parsed"))
            .withColumn("month", month("entry_date_parsed"))
            .withColumn("day", dayofmonth("entry_date_parsed"))
        )
        df = df.drop("entry_date_parsed")

        row_count = df.count()
        optimal_partitions = max(1, min(200, row_count // 1000000))

        df = df.withColumn(
            "processed_timestamp",
            lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
        )

        if table_name == "entity":
            global df_entity
            df_entity = df

        df.coalesce(optimal_partitions).write.partitionBy(
            "dataset", "year", "month", "day"
        ).mode("append").option("maxRecordsPerFile", 1000000).option(
            "compression", "snappy"
        ).parquet(output_path)

        logger.info(f"write_to_s3: Successfully wrote {row_count} rows")

    except Exception as e:
        logger.error(f"write_to_s3: Failed to write to S3: {e}", exc_info=True)
        raise


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


def s3_rename_and_move(env, dataset_name, file_type, bucket_name):
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


@log_execution_time
def write_to_s3_format(df, output_path, dataset_name, table_name, spark, env):
    """Write DataFrame to S3 in CSV, JSON, and GeoJSON formats."""
    try:
        count = count_df(df, env)
        logger.info(f"write_to_s3_format: Input DataFrame contains {count} records")

        path_bake = f"s3://{env}-collection-data/{dataset_name}-collection/dataset/{dataset_name}.csv"
        df_bake = read_csv_from_s3(spark, path_bake)
        df_bake = df_bake.select("entity", "json")

        df = df.join(df_bake, df["entity"] == df_bake["entity"], how="left").select(
            df["*"], df_bake["json"]
        )

        temp_output_path = f"s3://{env}-collection-data/dataset/temp/{dataset_name}/"

        df = normalise_dataframe_schema(df, table_name, dataset_name, spark, env)
        show_df(df, 5, env)

        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.info(f"write_to_s3_format: Cleaned up {cleanup_summary['objects_deleted']} objects")

        df = df.withColumn("dataset", lit(dataset_name))
        row_count = df.count()

        df = calculate_centroid(df)
        temp_df = df
        temp_df = flatten_json_column(temp_df)

        for column in temp_df.columns:
            if "_" in column:
                temp_df = temp_df.withColumnRenamed(column, column.replace("_", "-"))

        temp_df = ensure_schema_fields(temp_df, dataset_name)

        cleanup_temp_path(env, dataset_name)
        temp_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            temp_output_path
        )

        s3_rename_and_move(env, dataset_name, "csv", bucket_name=f"{env}-collection-data")

        # Write JSON
        json_buffer = '{"entities":['
        first = True
        for row in temp_df.toLocalIterator():
            if not first:
                json_buffer += ","
            first = False
            row_dict = row.asDict()
            for key, value in row_dict.items():
                if isinstance(value, (date_type, datetime_type)):
                    row_dict[key] = value.isoformat() if value else ""
                elif value is None:
                    row_dict[key] = ""
            json_buffer += json.dumps(row_dict)
        json_buffer += "]}"

        s3_client = boto3.client("s3")
        target_key = f"dataset/{dataset_name}.json"

        try:
            s3_client.head_object(Bucket=f"{env}-collection-data", Key=target_key)
            s3_client.delete_object(Bucket=f"{env}-collection-data", Key=target_key)
        except s3_client.exceptions.ClientError:
            pass

        s3_client.put_object(
            Bucket=f"{env}-collection-data", Key=target_key, Body=json_buffer
        )
        logger.info(f"write_to_s3_format: JSON file written to {target_key}")

        # Write GeoJSON
        target_key_geojson = f"dataset/{dataset_name}.geojson"

        try:
            s3_client.head_object(Bucket=f"{env}-collection-data", Key=target_key_geojson)
            s3_client.delete_object(Bucket=f"{env}-collection-data", Key=target_key_geojson)
        except s3_client.exceptions.ClientError:
            pass

        mpu = s3_client.create_multipart_upload(
            Bucket=f"{env}-collection-data", Key=target_key_geojson
        )
        parts = []
        part_num = 1

        try:
            from datetime import date, datetime

            header = '{"type":"FeatureCollection","name":"' + dataset_name + '","features":['
            buffer = header

            batch_size = 10000
            num_partitions = max(1, row_count // batch_size)

            first_row = True
            for partition_id, rows in enumerate(
                temp_df.repartition(num_partitions).toLocalIterator()
            ):
                row_dict = rows.asDict()
                geometry_wkt = row_dict.pop("geometry", None)
                row_dict.pop("point", None)

                for key, value in row_dict.items():
                    if isinstance(value, (date, datetime)):
                        row_dict[key] = value.isoformat() if value else ""
                    elif value is None:
                        row_dict[key] = ""

                geojson_geom = wkt_to_geojson(geometry_wkt) if geometry_wkt else None
                feature = {
                    "type": "Feature",
                    "properties": row_dict,
                    "geometry": geojson_geom,
                }

                if not first_row:
                    buffer += ","
                first_row = False
                buffer += json.dumps(feature)

                if len(buffer.encode("utf-8")) > 5 * 1024 * 1024:
                    part = s3_client.upload_part(
                        Bucket=f"{env}-collection-data",
                        Key=target_key_geojson,
                        PartNumber=part_num,
                        UploadId=mpu["UploadId"],
                        Body=buffer,
                    )
                    parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                    part_num += 1
                    buffer = ""

            buffer += "]}"
            part = s3_client.upload_part(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                PartNumber=part_num,
                UploadId=mpu["UploadId"],
                Body=buffer,
            )
            parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

            s3_client.complete_multipart_upload(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                UploadId=mpu["UploadId"],
                MultipartUpload={"Parts": parts},
            )
            logger.info(f"write_to_s3_format: GeoJSON file written to {target_key_geojson}")
        except Exception as e:
            logger.error(f"Error during GeoJSON multipart upload: {e}")
            s3_client.abort_multipart_upload(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                UploadId=mpu["UploadId"],
            )
            raise

        if "json" in df.columns:
            df = df.drop("json")

        return df
    except Exception as e:
        logger.error(f"write_to_s3_format: Failed to write to S3: {e}", exc_info=True)
        raise
    finally:
        if "temp_df" in locals() and temp_df is not None:
            try:
                temp_df.unpersist()
            except Exception:
                pass


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
