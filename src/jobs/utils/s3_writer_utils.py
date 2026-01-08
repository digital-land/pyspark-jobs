import json
import re
from datetime import date
from datetime import date as date_type
from datetime import datetime
from datetime import datetime as datetime_type

import boto3
import requests
from pyspark.sql.functions import (
    coalesce,
    col,
    collect_list,
    concat_ws,
    dayofmonth,
    desc,
    expr,
    first,
    lit,
    monotonically_increasing_id,
    month,
    row_number,
    struct,
    to_date,
    to_json,
    when,
    year,
)
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

from jobs.transform_collection_data import (
    transform_data_entity,
    transform_data_fact,
    transform_data_fact_res,
    transform_data_issue,
)
from jobs.utils.df_utils import count_df, show_df
from jobs.utils.flatten_csv import flatten_geojson_column, flatten_json_column

# Import geometry utilities
from jobs.utils.geometry_utils import calculate_centroid
from jobs.utils.logger_config import (
    get_logger,
    log_execution_time,
    set_spark_log_level,
    setup_logging,
)
from jobs.utils.s3_dataset_typology import get_dataset_typology
from jobs.utils.s3_format_utils import flatten_s3_json, s3_csv_format
from jobs.utils.s3_utils import cleanup_dataset_data, read_csv_from_s3

logger = get_logger(__name__)

df_entity = None


@log_execution_time
def transform_data_entity_format(df, data_set, spark, env=None):
    try:
        logger.info("transform_data_entity:Transforming data for Entity table")
        show_df(df, 20, env)
        # 1) Select the top record per (entity, field) using priority, entry_date, entry_number
        # Fallback if 'priority' is missing: use entry_date, entry_number
        if "priority" in df.columns:
            ordering_cols = [desc("priority"), desc("entry_date"), desc("entry_number")]
        else:
            ordering_cols = [desc("entry_date"), desc("entry_number")]

        w = Window.partitionBy("entity", "field").orderBy(*ordering_cols)
        df_ranked = (
            df.withColumn("row_num", row_number().over(w))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        # 2) Pivot to get one row per entity
        pivot_df = df_ranked.groupBy("entity").pivot("field").agg(first("value"))
        show_df(pivot_df, 5, env)

        logger.info(
            "transform_data_entity:Adding Typology data as the column missing after flattening"
        )
        # filtered_df = pivot_df.filter(col("field") == "typology").select("field", "value")

        # Add a new column "typology" to the pivoted DataFrame by applying the get_dataset_typology function
        typology_value = get_dataset_typology(data_set)
        logger.info(
            f"transform_data_entity: Fetched typology value from dataset specification for dataset: {data_set} is {typology_value}"
        )

        pivot_df = pivot_df.withColumn("typology", lit(typology_value))
        show_df(pivot_df, 5, env)

        # 3) Normalise column names (kebab-case -> snake_case)
        logger.info(
            "transform_data_entity: Normalising column names from kebab-case to snake_case"
        )
        for column in pivot_df.columns:
            if "-" in column:
                pivot_df = pivot_df.withColumnRenamed(column, column.replace("-", "_"))

        # 4) Set dataset and drop legacy geojson if present
        logger.info(
            f"transform_data_entity: Setting dataset column to {data_set} and dropping geojson column if exists"
        )
        pivot_df = pivot_df.withColumn("dataset", lit(data_set))
        if "geojson" in pivot_df.columns:
            pivot_df = pivot_df.drop("geojson")

        # 5) Organisation join to fetch organisation_entity
        logger.info(
            "transform_data_entity: Joining organisation to get organisation_entity"
        )
        organisation_df = spark.read.option("header", "true").csv(
            f"s3://{env}-collection-data/organisation/dataset/organisation.csv"
        )
        pivot_df = (
            pivot_df.join(
                organisation_df,
                pivot_df.organisation == organisation_df.organisation,
                "left",
            )
            .select(
                pivot_df["*"], organisation_df["entity"].alias("organisation_entity")
            )
            .drop("organisation")
        )

        # 6) Join typology from dataset specification
        # (Step removed: typology already retrieved earlier)

        # 7) Build json from any non-standard columns
        standard_columns = {
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        }
        if "geometry" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("geometry", lit(None).cast("string"))
        # Ensure expected columns exist before projection
        if "end_date" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("end_date", lit(None).cast("date"))
        if "start_date" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("start_date", lit(None).cast("date"))
        if "name" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("name", lit("").cast("string"))
        if "point" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn("point", lit(None).cast("string"))
        diff_columns = [c for c in pivot_df.columns if c not in standard_columns]
        if diff_columns:
            pivot_df = pivot_df.withColumn(
                "json", to_json(struct(*[col(c) for c in diff_columns]))
            )
        else:
            pivot_df = pivot_df.withColumn("json", lit("{}"))

        # 8) Normalise date columns
        for date_col in ["end_date", "entry_date", "start_date"]:
            if date_col in pivot_df.columns:
                pivot_df = pivot_df.withColumn(
                    date_col,
                    when(col(date_col) == "", None)
                    .when(col(date_col).isNull(), None)
                    .otherwise(to_date(col(date_col), "yyyy-MM-dd")),
                )

        # 9) Normalise geometry columns
        for geom_col in ["geometry", "point"]:
            if geom_col in pivot_df.columns:
                pivot_df = pivot_df.withColumn(
                    geom_col,
                    when(col(geom_col) == "", None)
                    .when(col(geom_col).isNull(), None)
                    .when(col(geom_col).startswith("POINT"), col(geom_col))
                    .when(col(geom_col).startswith("POLYGON"), col(geom_col))
                    .when(col(geom_col).startswith("MULTIPOLYGON"), col(geom_col))
                    .otherwise(None),
                )

        # 10) Final projection and safety dedupe
        out = pivot_df.select(
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        ).dropDuplicates(["entity"])

        logger.info(
            "transform_data_entity:Transform data for Entity table after pivoting and normalization"
        )
        show_df(out, 5, env)

        return out
    except Exception as e:
        logger.error(f"transform_data_entity:Error occurred - {e}")
        raise


@log_execution_time
def normalise_dataframe_schema(df, schema_name, data_set, spark, env=None):
    try:
        from jobs.main_collection_data import load_metadata

        dataset_json_transformed_path = "config/transformed_source.json"
        logger.info(
            f"normalise_dataframe_schema: Transforming data for table: {schema_name} using schema from {dataset_json_transformed_path}"
        )
        json_data = load_metadata(dataset_json_transformed_path)
        logger.info(
            f"normalise_dataframe_schema: Transforming data with schema with json data: {json_data}"
        )
        show_df(df, 5, env)

        # Extract the list of fields
        fields = []
        if (
            schema_name == "fact"
            or schema_name == "fact_res"
            or schema_name == "entity"
        ):
            fields = json_data.get("schema_fact_res_fact_entity", [])
            logger.info(
                f"normalise_dataframe_schema: Fields to select from json data {fields} for {schema_name}"
            )
        elif schema_name == "issue":
            fields = json_data.get("schema_issue", [])
            logger.info(
                f"normalise_dataframe_schema: Fields to select from json data {fields} for {schema_name}"
            )

        # Replace hyphens with underscores in column names
        for col in df.columns:
            if "-" in col:
                new_col = col.replace("-", "_")
                df = df.withColumnRenamed(col, new_col)
        logger.info(
            f"normalise_dataframe_schema: DataFrame columns after renaming hyphens: {df.columns}"
        )
        df.printSchema()
        logger.info(
            "normalise_dataframe_schema: DataFrame schema after renaming hyphens"
        )
        show_df(df, 5, env)

        # Get actual DataFrame columns
        df_columns = df.columns

        # Find fields that are present in both DataFrame and json
        if set(fields) == set(df.columns):
            logger.info(
                "normalise_dataframe_schema: All fields are present in the DataFrame"
            )
        else:
            logger.warning(
                "normalise_dataframe_schema: Some fields are missing in the DataFrame"
            )

        if schema_name == "entity":
            logger.info(
                "normalise_dataframe_schema: Transforming data for Entity table"
            )
            show_df(df, 5, env)
            return transform_data_entity_format(df, data_set, spark, env)
        else:
            raise ValueError(f"Unknown table name: {schema_name}")

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


# -------------------- S3 Writer --------------------
df_entity = None


@log_execution_time
def write_to_s3(df, output_path, dataset_name, table_name, env=None):
    try:
        logger.info(
            f"write_to_s3: Writing data to S3 at {output_path} for dataset {dataset_name}"
        )

        # Check and clean up existing data for this dataset before writing
        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.info(
            f"write_to_s3: Cleaned up {cleanup_summary['objects_deleted']} objects for dataset '{dataset_name}'"
        )
        if cleanup_summary["errors"]:
            logger.warning(
                f"write_to_s3: Cleanup had {len(cleanup_summary['errors'])} errors: {cleanup_summary['errors']}"
            )
        logger.debug(f"write_to_s3: Full cleanup summary: {cleanup_summary}")

        # Add dataset as partition column
        df = df.withColumn("dataset", lit(dataset_name))

        # Convert entry-date to date type and use it for partitioning
        df = df.withColumn("entry_date_parsed", to_date("entry_date", "yyyy-MM-dd"))
        df = (
            df.withColumn("year", year("entry_date_parsed"))
            .withColumn("month", month("entry_date_parsed"))
            .withColumn("day", dayofmonth("entry_date_parsed"))
        )

        # Drop the temporary parsing column
        df = df.drop("entry_date_parsed")

        # Calculate optimal partitions based on data size
        row_count = df.count()
        optimal_partitions = max(
            1, min(200, row_count // 1000000)
        )  # ~1M records per partition

        # adding time stamp to the dataframe for parquet file
        df = df.withColumn(
            "processed_timestamp",
            lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
        )
        logger.info("write_to_s3: DataFrame after adding processed_timestamp column")
        show_df(df, 5, env)

        if table_name == "entity":
            global df_entity
            (
                show_df(df_entity, 5, env)
                if df_entity
                else logger.info("write_to_s3: df_entity is None")
            )
            df_entity = df

        # Write to S3 with multilevel partitioning
        # Use "append" mode since we already cleaned up the specific dataset partition
        df.coalesce(optimal_partitions).write.partitionBy(
            "dataset", "year", "month", "day"
        ).mode("append").option("maxRecordsPerFile", 1000000).option(
            "compression", "snappy"
        ).parquet(
            output_path
        )

        logger.info(
            f"write_to_s3: Successfully wrote {row_count} rows to {output_path} with {optimal_partitions} partitions"
        )

    except Exception as e:
        logger.error(f"write_to_s3: Failed to write to S3: {e}", exc_info=True)
        raise


# --------------------delete existing temp files from s3 bucket--------------------
def cleanup_temp_path(env, dataset_name):
    """Delete all objects in the temp S3 path for a dataset."""
    import boto3

    s3_client = boto3.client("s3")
    bucket_name = f"{env}-collection-data"
    prefix = f"dataset/temp/{dataset_name}/"
    # List and delete all objects
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            logger.info(f"Deleted {len(objects)} objects from {prefix}")


# ------------------wkt to geojson converter-----------------
def wkt_to_geojson(wkt_string):
    """Convert WKT geometry string to GeoJSON geometry object."""
    if not wkt_string:
        return None

    import re

    wkt_string = wkt_string.strip()

    # Parse POINT
    if wkt_string.startswith("POINT"):
        coords = re.findall(r"[-\d.]+", wkt_string)
        return {"type": "Point", "coordinates": [float(coords[0]), float(coords[1])]}

    # Parse POLYGON
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

    # Parse MULTIPOLYGON
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

        # Simplify single polygon MultiPolygon to Polygon
        if len(polygons) == 1:
            return {"type": "Polygon", "coordinates": polygons[0]}
        return {"type": "MultiPolygon", "coordinates": polygons}

    return None


# ------------------round point coordinates to 6 decimal places-----------------
def round_point_coordinates(df):
    """Round POINT coordinates to 6 decimal places."""
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType

    def round_point_udf(point_str):
        if not point_str or not point_str.startswith("POINT"):
            return point_str
        try:
            coords = re.findall(r"[-\d.]+", point_str)
            if len(coords) == 2:
                lon = round(float(coords[0]), 6)
                lat = round(float(coords[1]), 6)
                return f"POINT ({lon} {lat})"
        except Exception:
            pass
        return point_str

    round_udf = udf(round_point_udf, StringType())

    if "point" in df.columns:
        df = df.withColumn("point", round_udf(col("point")))

    return df


# ------------------fetch schema from github specification-----------------
def fetch_dataset_schema_fields(dataset_name):
    """Fetch dataset schema fields from GitHub specification."""
    try:
        url = f"https://raw.githubusercontent.com/digital-land/specification/main/content/dataset/{dataset_name}.md"
        logger.info(f"Fetching schema from: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        content = response.text
        fields = []

        # Parse YAML frontmatter format
        in_frontmatter = False
        in_fields_section = False

        for line in content.split("\n"):
            if line.strip() == "---":
                if not in_frontmatter:
                    in_frontmatter = True
                else:
                    break  # End of frontmatter
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

        logger.info(f"Fetched {len(fields)} fields from specification: {fields}")
        return fields
    except Exception as e:
        logger.warning(f"Failed to fetch schema from GitHub: {e}")
        return []


def ensure_schema_fields(df, dataset_name):
    """Ensure DataFrame has all required fields from schema specification with empty columns at end."""
    try:
        schema_fields = fetch_dataset_schema_fields(dataset_name)
        if not schema_fields:
            logger.info("No schema fields fetched, returning DataFrame as-is")
            return df

        current_columns = set(df.columns)
        missing_fields = [
            field for field in schema_fields if field not in current_columns
        ]

        if missing_fields:
            logger.info(
                f"Adding {len(missing_fields)} missing fields at end: {missing_fields}"
            )
            # Keep existing column order and add missing fields at the end
            existing_cols = df.columns
            for field in missing_fields:
                df = df.withColumn(field, lit(""))
            # Reorder: existing columns first, then missing fields
            final_columns = existing_cols + missing_fields
            df = df.select(final_columns)
            logger.info(
                f"Column order preserved with {len(missing_fields)} empty columns added at end"
            )
        else:
            logger.info("All schema fields already present in DataFrame")

        return df
    except Exception as e:
        logger.error(f"Error ensuring schema fields: {e}")
        return df


# ------------------s3 writer format with csv and json-----------------
def s3_rename_and_move(env, dataset_name, file_type, bucket_name):
    # get unique csv filename from temp_output_path and rename to datasetname.csv
    s3_client = boto3.client("s3")
    unique_data_filename = f"{dataset_name}.{file_type}"
    target_key = f"dataset/{unique_data_filename}"
    logger.info(f"Renaming {file_type} files for dataset: {dataset_name}")

    # Delete existing file if it exists
    try:
        s3_client.head_object(Bucket=bucket_name, Key=target_key)
        s3_client.delete_object(Bucket=bucket_name, Key=target_key)
        logger.info(f"Deleted existing file: {target_key}")
    except s3_client.exceptions.ClientError:
        logger.info(f"No existing file to delete: {target_key}")

    # List files matching pattern
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=f"dataset/temp/{dataset_name}/"
    )
    data_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(f".{file_type}")
    ]
    logger.info(
        f"Found {len(data_files)} {file_type} files to rename for dataset: {dataset_name}"
    )
    # Copy and delete each file
    for data_file in data_files:
        # Copy to new location
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": data_file},
            Key=target_key,
        )
        # Delete original
        s3_client.delete_object(Bucket=bucket_name, Key=data_file)
        logger.info(f"Renamed: {data_file} -> {target_key}")


# -------------------- S3 Writer Format--------------------
def write_to_s3_format(df, output_path, dataset_name, table_name, spark, env):
    try:
        count = count_df(df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame Before leftjoin for {table_name} table contains {count} records"
            )

        # Load bake data from S3 and join with main dataframe to enrich with json column
        path_bake = f"s3://{env}-collection-data/{dataset_name}-collection/dataset/{dataset_name}.csv"
        logger.info(f"Reading bake data from {path_bake}")
        df_bake = read_csv_from_s3(spark, path_bake)

        logger.info("Selecting entity and json columns from bake dataframe")
        show_df(df_bake, 5, env)
        df_bake = df_bake.select("entity", "json")
        show_df(df_bake, 5, env)

        logger.info("Performing left join on entity column")
        df = df.join(df_bake, df["entity"] == df_bake["entity"], how="left").select(
            df["*"], df_bake["json"]
        )
        # dispay count after join
        count = count_df(df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame After leftjoin for {table_name} table contains {count} records"
            )
        logger.info("Join completed, showing result dataframe")
        show_df(df, 5, env)

        temp_output_path = f"s3://{env}-collection-data/dataset/temp/{dataset_name}/"

        # Normalise dataframe schema
        df = normalise_dataframe_schema(df, table_name, dataset_name, spark, env)
        logger.info(
            f"write_to_s3_format: DataFrame after transformation for dataset {dataset_name} and table {table_name}"
        )
        show_df(df, 5, env)

        logger.info(
            f"write_to_s3_format: Writing data to S3 at {output_path} for dataset {dataset_name}"
        )

        # count after schema normalisation
        count = count_df(df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame After schema normalisation for {table_name} table contains {count} records"
            )

        # Check and clean up (deleting old files) existing data for this dataset before writing
        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.info(
            f"write_to_s3_format: Cleaned up {cleanup_summary['objects_deleted']} objects for dataset '{dataset_name}'"
        )
        if cleanup_summary["errors"]:
            logger.warning(
                f"write_to_s3_format: Cleanup had {len(cleanup_summary['errors'])} errors: {cleanup_summary['errors']}"
            )
        logger.debug(f"write_to_s3_format: Full cleanup summary: {cleanup_summary}")

        # Add dataset_name as a column
        logger.info(
            f"write_to_s3_format: Adding dataset column with value {dataset_name}"
        )
        df = df.withColumn("dataset", lit(dataset_name))
        show_df(df, 5, env)

        # count after add dataset column
        count = count_df(df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame After adding dataset column for {table_name} table contains {count} records"
            )

        # Calculate optimal partitions based on data size
        logger.info(
            "write_to_s3_format: Calculating optimal partitions based on data size"
        )
        row_count = df.count()
        optimal_partitions = max(
            1, min(200, row_count // 1000000)
        )  # ~1M records per partition

        # Calculate centroid with 6 decimal places
        logger.info(
            f"write_to_s3_format: Calculating centroid with 6 decimal places for {table_name} table"
        )
        df = calculate_centroid(df)
        show_df(df, 5, env)

        # count after centroid calculation
        count = count_df(df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame After centroid calculation for {table_name} table contains {count} records"
            )

        # Create a copy of the main dataframe to a temporary dataframe for further processing
        temp_df = df

        # Flatten JSON columns
        logger.info(f"write_to_s3_format: Flattening json data for: {dataset_name}")
        temp_df = flatten_json_column(temp_df)
        show_df(temp_df, 5, env)

        # count after flattening json data
        count = count_df(temp_df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame After flattening json data for {table_name} table contains {count} records"
            )

        # Normalise column names (kebab-case -> snake_case)
        logger.info(
            "write_to_s3_format: Normalising column names from kebab_case to snake-case"
        )
        for column in temp_df.columns:
            if "_" in column:
                temp_df = temp_df.withColumnRenamed(column, column.replace("_", "-"))
        
         #count after normalise column names
        count = count_df(temp_df, env)
        if count is not None:
            logger.info(f"write_to_s3_format: Input DataFrame After normalising column names for {table_name} table contains {count} records")


        # count after normalise column names
        count = count_df(temp_df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame After normalising column names for {table_name} table contains {count} records"
            )

        # Ensure all schema fields are present
        logger.info(
            f"write_to_s3_format: Ensuring schema fields for dataset: {dataset_name}"
        )
        temp_df = ensure_schema_fields(temp_df, dataset_name)

        # count after schema fields ensured
        count = count_df(temp_df, env)
        if count is not None:
            logger.info(
                f"write_to_s3_format: Input DataFrame After schema fields ensured and before writing csvfor {table_name} table contains {count} records"
            )

        # Write csv to S3 path
        # Use "append" mode since we already cleaned up the specific dataset partition
        logger.info(f"write_to_s3_format: Writing csv data for: {dataset_name}")
        show_df(temp_df, 5, env)

        cleanup_temp_path(env, dataset_name)

        temp_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            temp_output_path
        )

        # Rename the csv file to dataset_name.csv
        logger.info(f"write_to_s3_format: Renaming csv for: {dataset_name}")
        s3_rename_and_move(
            env, dataset_name, "csv", bucket_name=f"{env}-collection-data"
        )

        # Write JSON data
        logger.info(f"write_to_s3_format: Writing json data for: {dataset_name}")

        def convert_row(row):
            row_dict = row.asDict()
            for key, value in row_dict.items():
                if isinstance(value, (date_type, datetime_type)):
                    row_dict[key] = value.isoformat() if value else ""
                elif value is None:
                    row_dict[key] = ""
            return row_dict

        # Stream JSON to avoid OOM
        json_buffer = '{"entities":['
        first = True
        for row in temp_df.toLocalIterator():
            if not first:
                json_buffer += ","
            first = False
            json_buffer += json.dumps(convert_row(row))
        json_buffer += "]}"
        json_output = json_buffer

        s3_client = boto3.client("s3")
        target_key = f"dataset/{dataset_name}.json"

        # Delete existing file if it exists
        try:
            s3_client.head_object(Bucket=f"{env}-collection-data", Key=target_key)
            s3_client.delete_object(Bucket=f"{env}-collection-data", Key=target_key)
            logger.info(f"Deleted existing file: {target_key}")
        except s3_client.exceptions.ClientError:
            logger.info(f"No existing file to delete: {target_key}")

        s3_client.put_object(
            Bucket=f"{env}-collection-data", Key=target_key, Body=json_output
        )
        logger.info(f"write_to_s3_format: JSON file written to {target_key}")

        # Write GeoJSON data
        logger.info(f"write_to_s3_format: Writing geojson data for: {dataset_name}")
        target_key_geojson = f"dataset/{dataset_name}.geojson"

        # Delete existing file
        try:
            s3_client.head_object(
                Bucket=f"{env}-collection-data", Key=target_key_geojson
            )
            s3_client.delete_object(
                Bucket=f"{env}-collection-data", Key=target_key_geojson
            )
            logger.info(f"Deleted existing file: {target_key_geojson}")
        except s3_client.exceptions.ClientError:
            logger.info(f"No existing file to delete: {target_key_geojson}")

        # Use multipart upload for efficient streaming
        mpu = s3_client.create_multipart_upload(
            Bucket=f"{env}-collection-data", Key=target_key_geojson
        )
        parts = []
        part_num = 1

        try:
            # Write header
            header = (
                '{"type":"FeatureCollection","name":"' + dataset_name + '","features":['
            )
            buffer = header

            # Process in partitions to avoid OOM
            batch_size = 10000
            num_partitions = max(1, row_count // batch_size)
            logger.info(f"Processing GeoJSON with {num_partitions} partitions")

            first_row = True
            for partition_id, rows in enumerate(
                temp_df.repartition(num_partitions).toLocalIterator()
            ):
                if partition_id % 200000 == 0:
                    logger.info(f"Processing GeoJSON row {partition_id}/{row_count}")

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

                # Upload part when buffer reaches 5MB
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

            # Write footer and remaining buffer
            buffer += "]}"
            part = s3_client.upload_part(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                PartNumber=part_num,
                UploadId=mpu["UploadId"],
                Body=buffer,
            )
            parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

            # Complete multipart upload
            s3_client.complete_multipart_upload(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                UploadId=mpu["UploadId"],
                MultipartUpload={"Parts": parts},
            )
            logger.info(
                f"write_to_s3_format: GeoJSON file written to {target_key_geojson}"
            )
        except Exception as e:
            logger.error(f"Error during GeoJSON multipart upload: {e}")
            s3_client.abort_multipart_upload(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                UploadId=mpu["UploadId"],
            )
            raise

        logger.info(
            f"write_to_s3_format: csv, json and geojson files successfully written for dataset: {dataset_name}"
        )

        # Drop the json column no longer needed before returning for wrting to postgres
        if "json" in df.columns:
            df = df.drop("json")

        return df
    except Exception as e:
        logger.error(f"write_to_s3_format: Failed to write to S3: {e}", exc_info=True)
        raise
    finally:
        # Clean up temp_df if it exists
        if "temp_df" in locals() and temp_df is not None:
            try:
                temp_df.unpersist()
            except Exception:
                pass
