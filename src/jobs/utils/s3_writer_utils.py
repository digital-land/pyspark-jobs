from jobs.transform_collection_data import transform_data_issue
from jobs.transform_collection_data import transform_data_entity, transform_data_fact, transform_data_fact_res
from jobs.utils.s3_dataset_typology import get_dataset_typology
from jobs.utils.s3_format_utils import flatten_s3_json, s3_csv_format
from jobs.utils.s3_utils import cleanup_dataset_data
from jobs.utils.logger_config import setup_logging, get_logger, log_execution_time, set_spark_log_level
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType
from datetime import datetime
from pyspark.sql.functions import (coalesce,collect_list,concat_ws,dayofmonth,expr,first,month,to_date,year,row_number,lit)
from pyspark.sql.functions import (
    row_number, lit, first, to_json, struct, col, when, to_date, desc, expr
)
from pyspark.sql.window import Window
from jobs.utils.flatten_csv import flatten_json_column, flatten_geojson_column
import boto3
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# Import geometry utilities
from jobs.utils.geometry_utils import calculate_centroid

logger = get_logger(__name__)

df_entity = None
@log_execution_time

def transform_data_entity_format(df,data_set,spark):
    try:
        logger.info("transform_data_entity:Transforming data for Entity table")
        df.show(20)
        # 1) Select the top record per (entity, field) using priority, entry_date, entry_number
        # Fallback if 'priority' is missing: use entry_date, entry_number
        if "priority" in df.columns:
            ordering_cols = [desc("priority"), desc("entry_date"), desc("entry_number")]
        else:
            ordering_cols = [desc("entry_date"), desc("entry_number")]

        w = Window.partitionBy("entity", "field").orderBy(*ordering_cols)
        df_ranked = df.withColumn("row_num", row_number().over(w)) \
                      .filter(col("row_num") == 1) \
                      .drop("row_num")

        # 2) Pivot to get one row per entity
        pivot_df = df_ranked.groupBy("entity").pivot("field").agg(first("value"))
        pivot_df.show(5)

        logger.info("transform_data_entity:Adding Typology data as the column missing after flattening")
        # filtered_df = pivot_df.filter(col("field") == "typology").select("field", "value")

        # Add a new column "typology" to the pivoted DataFrame by applying the get_dataset_typology function 
        typology_value = get_dataset_typology(data_set)
        logger.info(f"transform_data_entity: Fetched typology value from dataset specification for dataset: {data_set} is {typology_value}")

        pivot_df = pivot_df.withColumn("typology", lit(typology_value))
        pivot_df.show(5)
        
        
        # 3) Normalise column names (kebab-case -> snake_case)
        logger.info(f"transform_data_entity: Normalising column names from kebab-case to snake_case")
        for column in pivot_df.columns:
            if "-" in column:
                pivot_df = pivot_df.withColumnRenamed(column, column.replace("-", "_"))

        # 4) Set dataset and drop legacy geojson if present
        logger.info(f"transform_data_entity: Setting dataset column to {data_set} and dropping geojson column if exists")
        pivot_df = pivot_df.withColumn("dataset", lit(data_set))
        if "geojson" in pivot_df.columns:
            pivot_df = pivot_df.drop("geojson")

        # 5) Organisation join to fetch organisation_entity
        logger.info(f"transform_data_entity: Joining organisation to get organisation_entity")
        organisation_df = spark.read.option("header", "true").csv("s3://development-collection-data/organisation/dataset/organisation.csv")
        pivot_df = pivot_df.join(
            organisation_df,
            pivot_df.organisation == organisation_df.organisation,
            "left"
        ).select(
            pivot_df["*"],
            organisation_df["entity"].alias("organisation_entity")
        ).drop("organisation")

        # 6) Join typology from dataset specification
        # (Step removed: typology already retrieved earlier)

        # 7) Build json from any non-standard columns
        standard_columns = {
            "dataset", "end_date", "entity", "entry_date", "geometry", "json",
            "name", "organisation_entity", "point", "prefix", "reference",
            "start_date", "typology"
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
            pivot_df = pivot_df.withColumn("json", to_json(struct(*[col(c) for c in diff_columns])))
        else:
            pivot_df = pivot_df.withColumn("json", lit("{}"))

        # 8) Normalise date columns
        for date_col in ["end_date", "entry_date", "start_date"]:
            if date_col in pivot_df.columns:
                pivot_df = pivot_df.withColumn(
                    date_col,
                    when(col(date_col) == "", None)
                    .when(col(date_col).isNull(), None)
                    .otherwise(to_date(col(date_col), "yyyy-MM-dd"))
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
                    .otherwise(None)
                )

        # 10) Final projection and safety dedupe
        out = pivot_df.select(
            "dataset", "end_date", "entity", "entry_date", "geometry", "json",
            "name", "organisation_entity", "point", "prefix", "reference",
            "start_date", "typology"
        ).dropDuplicates(["entity"])

        logger.info("transform_data_entity:Transform data for Entity table after pivoting and normalization")
        out.show(5)

        return out
    except Exception as e:
        logger.error(f"transform_data_entity:Error occurred - {e}")
        raise
@log_execution_time


def normalise_dataframe_schema(df, schema_name, data_set,spark):      
    try:
        from jobs.main_collection_data import load_metadata
        dataset_json_transformed_path = "config/transformed_source.json"
        logger.info(f"normalise_dataframe_schema: Transforming data for table: {schema_name} using schema from {dataset_json_transformed_path}")
        json_data = load_metadata(dataset_json_transformed_path)
        logger.info(f"normalise_dataframe_schema: Transforming data with schema with json data: {json_data}")
        df.show(5)

        # Extract the list of fields
        fields = []
        if (schema_name == 'fact' or schema_name == 'fact_res' or schema_name == 'entity'):
            fields = json_data.get("schema_fact_res_fact_entity", [])
            logger.info(f"normalise_dataframe_schema: Fields to select from json data {fields} for {schema_name}")
        elif (schema_name == 'issue'):
            fields = json_data.get("schema_issue", [])
            logger.info(f"normalise_dataframe_schema: Fields to select from json data {fields} for {schema_name}")

        # Replace hyphens with underscores in column names
        for col in df.columns:
            if "-" in col:
                new_col = col.replace("-", "_")
                df = df.withColumnRenamed(col, new_col)
        logger.info(f"normalise_dataframe_schema: DataFrame columns after renaming hyphens: {df.columns}")
        df.printSchema()
        logger.info(f"normalise_dataframe_schema: DataFrame schema after renaming hyphens")
        df.show(5)

        # Get actual DataFrame columns
        df_columns = df.columns

        # Find fields that are present in both DataFrame and json    
        if set(fields) == set(df.columns):
            logger.info("normalise_dataframe_schema: All fields are present in the DataFrame")
        else:
            logger.warning("normalise_dataframe_schema: Some fields are missing in the DataFrame")
            
        if schema_name == 'entity':
            logger.info("normalise_dataframe_schema: Transforming data for Entity table")
            df.show(5)
            return transform_data_entity_format(df,data_set,spark)        
        else:
            raise ValueError(f"Unknown table name: {schema_name}")

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise



# # -------------------- S3 Writer Format--------------------
# def write_to_s3_format(df, output_path, dataset_name, table_name,spark,env):
#     df = normalise_dataframe_schema(df,table_name,dataset_name,spark)
#     logger.info(f"write_to_s3_format: DataFrame after transformation for dataset {dataset_name} and table {table_name}")
#     df.show(5)
#     output_path=f"s3://{env}-pd-batch-emr-studio-ws-bucket/csv/{dataset_name}.csv"
#     output_path1=f"s3://{env}-pd-batch-emr-studio-ws-bucket/json/{dataset_name}.json"

#     #output_path=f"s3://{env}-collection-data/dataset/{dataset_name}_test.csv"
#     try:   
#         logger.info(f"write_to_s3_format: Writing data to S3 at {output_path} for dataset {dataset_name}") 
        
#         # Check and clean up existing data for this dataset before writing
#         cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
#         logger.info(f"write_to_s3_format: Cleaned up {cleanup_summary['objects_deleted']} objects for dataset '{dataset_name}'")
#         if cleanup_summary['errors']:
#             logger.warning(f"write_to_s3_format: Cleanup had {len(cleanup_summary['errors'])} errors: {cleanup_summary['errors']}")
#         logger.debug(f"write_to_s3_format: Full cleanup summary: {cleanup_summary}")

#         # Add dataset as partition column
#         df = df.withColumn("dataset", lit(dataset_name))
                
#         # Convert entry-date to date type and use it for partitioning       
#         # Calculate optimal partitions based on data size
#         row_count = df.count()
#         optimal_partitions = max(1, min(200, row_count // 1000000))  # ~1M records per partition
        
#         #adding time stamp to the dataframe for parquet file
#         df = df.withColumn("processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
#         logger.info(f"write_to_s3_format: DataFrame after adding processed_timestamp column")
#         df.show(5)
    

#         if table_name == 'entity':
#             global df_entity
#             df_entity.show(5) if df_entity else logger.info("write_to_s3_format: df_entity is None")
#             df_entity = df

#         logger.info(f"write_to_s3_format: Invoking s3_csv_format for dataset {dataset_name}") 

#         df_csv = s3_csv_format(df)
#         # Write to S3 with multilevel partitioning
#         # Use "append" mode since we already cleaned up the specific dataset partition
#         df_csv.show(5)
#         df_csv.coalesce(1) \
#           .write \
#           .mode("overwrite")  \
#           .option("header", "true") \
#           .csv(output_path)
        
#         df_json=flatten_s3_json(df)
#         df_json.show(5)
#         df_json.coalesce(1) \
#           .write \
#           .mode("overwrite") \
#           .json(output_path1)

#         logger.info(f"write_to_s3_format: Successfully wrote {row_count} rows to {output_path} with {optimal_partitions} partitions")

#     except Exception as e:
#         logger.error(f"write_to_s3_format: Failed to write to S3: {e}", exc_info=True)
#         raise

# -------------------- S3 Writer --------------------
df_entity = None
@log_execution_time
def write_to_s3(df, output_path, dataset_name, table_name):
    try:   
        logger.info(f"write_to_s3: Writing data to S3 at {output_path} for dataset {dataset_name}") 
        
        # Check and clean up existing data for this dataset before writing
        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.info(f"write_to_s3: Cleaned up {cleanup_summary['objects_deleted']} objects for dataset '{dataset_name}'")
        if cleanup_summary['errors']:
            logger.warning(f"write_to_s3: Cleanup had {len(cleanup_summary['errors'])} errors: {cleanup_summary['errors']}")
        logger.debug(f"write_to_s3: Full cleanup summary: {cleanup_summary}")
        
        # Add dataset as partition column
        df = df.withColumn("dataset", lit(dataset_name))
                
        # Convert entry-date to date type and use it for partitioning
        df = df.withColumn("entry_date_parsed", to_date("entry_date", "yyyy-MM-dd"))
        df = df.withColumn("year", year("entry_date_parsed")) \
            .withColumn("month", month("entry_date_parsed")) \
            .withColumn("day", dayofmonth("entry_date_parsed"))
        
        # Drop the temporary parsing column
        df = df.drop("entry_date_parsed")
        
        # Calculate optimal partitions based on data size
        row_count = df.count()
        optimal_partitions = max(1, min(200, row_count // 1000000))  # ~1M records per partition
        
        #adding time stamp to the dataframe for parquet file
        df = df.withColumn("processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
        logger.info(f"write_to_s3: DataFrame after adding processed_timestamp column")
        df.show(5)
    

        if table_name == 'entity':
            global df_entity
            df_entity.show(5) if df_entity else logger.info("write_to_s3: df_entity is None")
            df_entity = df

        # Write to S3 with multilevel partitioning
        # Use "append" mode since we already cleaned up the specific dataset partition
        df.coalesce(optimal_partitions) \
          .write \
          .partitionBy("dataset", "year", "month", "day") \
          .mode("append") \
          .option("maxRecordsPerFile", 1000000) \
          .option("compression", "snappy") \
          .parquet(output_path)
        
        logger.info(f"write_to_s3: Successfully wrote {row_count} rows to {output_path} with {optimal_partitions} partitions")
        
    except Exception as e:
        logger.error(f"write_to_s3: Failed to write to S3: {e}", exc_info=True)
        raise
#--------------------delete existing temp files from s3 bucket--------------------
def cleanup_temp_path(env, dataset_name):
    """Delete all objects in the temp S3 path for a dataset."""
    import boto3
    s3_client = boto3.client("s3")
    bucket_name = f"{env}-collection-data"
    prefix = f"dataset/temp/{dataset_name}/"
    # List and delete all objects
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            objects = [{'Key': obj['Key']} for obj in page['Contents']]
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects})
            logger.info(f"Deleted {len(objects)} objects from {prefix}")
  
#------------------wkt to geojson converter-----------------
def wkt_to_geojson(wkt_string):
    """Convert WKT geometry string to GeoJSON geometry object."""
    if not wkt_string:
        return None
    
    import re
    wkt_string = wkt_string.strip()
    
    # Parse POINT
    if wkt_string.startswith('POINT'):
        coords = re.findall(r'[-\d.]+', wkt_string)
        return {"type": "Point", "coordinates": [float(coords[0]), float(coords[1])]}
    
    # Parse POLYGON
    elif wkt_string.startswith('POLYGON'):
        rings = re.findall(r'\(([^()]+)\)', wkt_string)
        coordinates = []
        for ring in rings:
            points = []
            coords = re.findall(r'([-\d.]+)\s+([-\d.]+)', ring)
            for lon, lat in coords:
                points.append([float(lon), float(lat)])
            coordinates.append(points)
        return {"type": "Polygon", "coordinates": coordinates}
    
    # Parse MULTIPOLYGON
    elif wkt_string.startswith('MULTIPOLYGON'):
        wkt_string = wkt_string.replace('MULTIPOLYGON ', '').strip()
        polygons = []
        depth = 0
        current_polygon = ''
        
        for char in wkt_string:
            if char == '(':
                depth += 1
                if depth > 1:
                    current_polygon += char
            elif char == ')':
                depth -= 1
                if depth > 0:
                    current_polygon += char
                elif depth == 0 and current_polygon:
                    rings = re.findall(r'\(([^()]+)\)', current_polygon)
                    coordinates = []
                    for ring in rings:
                        points = []
                        coords = re.findall(r'([-\d.]+)\s+([-\d.]+)', ring)
                        for lon, lat in coords:
                            points.append([float(lon), float(lat)])
                        coordinates.append(points)
                    polygons.append(coordinates)
                    current_polygon = ''
            elif depth > 0:
                current_polygon += char
        
        # Simplify single polygon MultiPolygon to Polygon
        if len(polygons) == 1:
            return {"type": "Polygon", "coordinates": polygons[0]}
        return {"type": "MultiPolygon", "coordinates": polygons}
    
    return None

#------------------s3 writer format with csv and json-----------------
def s3_rename_and_move(env, dataset_name, file_type,bucket_name):
    #get unique csv filename from temp_output_path and rename to datasetname.csv
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
        Bucket=bucket_name, 
        Prefix=f"dataset/temp/{dataset_name}/"
        )
        data_files = [obj['Key'] for obj in response.get('Contents', []) 
             if obj['Key'].endswith(f'.{file_type}')]
        logger.info(f"Found {len(data_files)} {file_type} files to rename for dataset: {dataset_name}")
        # Copy and delete each file
        for data_file in data_files:
        # Copy to new location
            s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': data_file},
            Key=target_key
            )
            # Delete original
            s3_client.delete_object(Bucket=bucket_name, Key=data_file)
            logger.info(f"Renamed: {data_file} -> {target_key}")

# -------------------- S3 Writer Format--------------------
def write_to_s3_format(df, output_path, dataset_name, table_name,spark,env):
    temp_output_path = f"s3://{env}-collection-data/dataset/temp/{dataset_name}/"
    #output_path = f"s3://{env}-collection-data/dataset/"

    df = normalise_dataframe_schema(df,table_name,dataset_name,spark)
    logger.info(f"write_to_s3_format: DataFrame after transformation for dataset {dataset_name} and table {table_name}")
    df.show(5)

    try:   
        logger.info(f"write_to_s3_format: Writing data to S3 at {output_path} for dataset {dataset_name}") 
        
        # Check and clean up existing data for this dataset before writing
        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.info(f"write_to_s3_format: Cleaned up {cleanup_summary['objects_deleted']} objects for dataset '{dataset_name}'")
        if cleanup_summary['errors']:
            logger.warning(f"write_to_s3_format: Cleanup had {len(cleanup_summary['errors'])} errors: {cleanup_summary['errors']}")
        logger.debug(f"write_to_s3_format: Full cleanup summary: {cleanup_summary}")

        # Add dataset as partition column
        logger.info(f"write_to_s3_format: Adding dataset column with value {dataset_name}")
        df = df.withColumn("dataset", lit(dataset_name))
        df.show(5)
                     
        # Calculate optimal partitions based on data size
        logger.info(f"write_to_s3_format: Calculating optimal partitions based on data size")
        row_count = df.count()
        optimal_partitions = max(1, min(200, row_count // 1000000))  # ~1M records per partition
        
        # Adding time stamp to the dataframe for parquet file
        # df = df.withColumn("processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
        # logger.info(f"write_to_s3_format: DataFrame after adding processed_timestamp column")
        # df.show(5)

        logger.info(f"write_to_s3_format: Invocation of calculate_centroid method for {table_name} table")
        df = calculate_centroid(df)
        df.show(5)

        temp_df = df

        logger.info(f"write_to_s3_format: Flattening json data for: {dataset_name}") 
        temp_df = flatten_json_column(temp_df)
        temp_df.show(5)

        logger.info(f"write_to_s3_format: Normalising column names from kebab_case to snake-case")
        for column in temp_df.columns:
            if "_" in column:
                temp_df = temp_df.withColumnRenamed(column, column.replace("_", "-"))

        # Write to S3 with multilevel partitioning
        # Use "append" mode since we already cleaned up the specific dataset partition
        logger.info(f"write_to_s3_format: Writing csv data for: {dataset_name}") 
        temp_df.show(5)

        cleanup_temp_path(env, dataset_name)
        
        temp_df.coalesce(1) \
          .write \
          .mode("overwrite")  \
          .option("header", "true") \
          .csv(temp_output_path)
        
        logger.info(f"write_to_s3_format: Renaming csv for: {dataset_name}") 
        s3_rename_and_move(env, dataset_name, "csv",bucket_name=f"{env}-collection-data")

        # Write JSON data
        logger.info(f"write_to_s3_format: Writing json data for: {dataset_name}") 
        json_data = temp_df.toJSON().collect()
        json_output = '{"entities": [' + ','.join(json_data) + ']}'
        
        s3_client = boto3.client("s3")
        target_key = f"dataset/{dataset_name}.json"
        
        # Delete existing file if it exists
        try:
            s3_client.head_object(Bucket=f"{env}-collection-data", Key=target_key)
            s3_client.delete_object(Bucket=f"{env}-collection-data", Key=target_key)
            logger.info(f"Deleted existing file: {target_key}")
        except s3_client.exceptions.ClientError:
            logger.info(f"No existing file to delete: {target_key}")
        
        s3_client.put_object(Bucket=f"{env}-collection-data", Key=target_key, Body=json_output)
        logger.info(f"write_to_s3_format: JSON file written to {target_key}")

        # Write GeoJSON data
        logger.info(f"write_to_s3_format: Writing geojson data for: {dataset_name}")
        geojson_features = []
        for row in temp_df.collect():
            row_dict = row.asDict()
            geometry_wkt = row_dict.pop('geometry', None)
            row_dict.pop('point', None)
            
            # Convert date/datetime objects to strings
            from datetime import date, datetime
            for key, value in row_dict.items():
                if isinstance(value, (date, datetime)):
                    row_dict[key] = value.isoformat() if value else ""
                elif value is None:
                    row_dict[key] = ""
            
            geojson_geom = None
            if geometry_wkt:
                geojson_geom = wkt_to_geojson(geometry_wkt)
            
            feature = {
                "type": "Feature",
                "properties": row_dict,
                "geometry": geojson_geom
            }
            geojson_features.append(feature)
        
        geojson_output = {
            "type": "FeatureCollection",
            "name": dataset_name,
            "features": geojson_features
        }
        
        import json
        geojson_str = json.dumps(geojson_output)
        target_key_geojson = f"dataset/{dataset_name}.geojson"
        
        try:
            s3_client.head_object(Bucket=f"{env}-collection-data", Key=target_key_geojson)
            s3_client.delete_object(Bucket=f"{env}-collection-data", Key=target_key_geojson)
            logger.info(f"Deleted existing file: {target_key_geojson}")
        except s3_client.exceptions.ClientError:
            logger.info(f"No existing file to delete: {target_key_geojson}")
        
        s3_client.put_object(Bucket=f"{env}-collection-data", Key=target_key_geojson, Body=geojson_str)
        logger.info(f"write_to_s3_format: GeoJSON file written to {target_key_geojson}")

        logger.info(f"write_to_s3_format: csv, json and geojson files successfully written for dataset {dataset_name}")
        return df
    except Exception as e:
        logger.error(f"write_to_s3_format: Failed to write to S3: {e}", exc_info=True)
        raise


