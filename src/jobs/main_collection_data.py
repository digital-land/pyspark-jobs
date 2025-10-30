import configparser
import os
import boto3
import pkgutil
import json
import sys
import argparse
from jobs.transform_collection_data import (transform_data_fact, transform_data_fact_res,
                                      transform_data_issue, transform_data_entity) 
from jobs.dbaccess.postgres_connectivity import (create_table, write_to_postgres, get_aws_secret,
                                                  create_and_prepare_staging_table, commit_staging_to_production,
                                                  calculate_centroid_wkt, ENTITY_TABLE_NAME)
#from jobs.utils.point_sedona import sedona_test
from jobs.utils.s3_format_utils import flatten_s3_json, s3_csv_format
from jobs.utils.s3_utils import cleanup_dataset_data
from jobs.csv_s3_writer import write_dataframe_to_csv_s3, import_csv_to_aurora, cleanup_temp_csv_files
#import sqlite3
from datetime import datetime
from dataclasses import fields
#from jobs import transform_collection_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce,collect_list,concat_ws,dayofmonth,expr,first,month,to_date,year,row_number,lit)
from pyspark.sql.types import (StringType,StructField,StructType,TimestampType)
from pyspark.sql.window import Window

# Import the new logging module
from jobs.utils.logger_config import setup_logging, get_logger, log_execution_time, set_spark_log_level

#from utils.path_utils import load_json_from_repo

# -------------------- Logging Setup --------------------
# Setup logging for EMR Serverless (console output goes to CloudWatch automatically)
def initialize_logging(args):
    setup_logging(
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        # Note: In EMR Serverless, console logs are automatically captured by CloudWatch
        # File logging is optional for local debugging
        log_file=os.getenv("LOG_FILE") if os.getenv("LOG_FILE") else None,
        environment=os.getenv("ENVIRONMENT", args.load_type)
)

logger = get_logger(__name__)

# -------------------- Spark Session --------------------
@log_execution_time
def create_spark_session(app_name="EMR Transform Job"):

    try:
        logger.info(f"Creating Spark session with app name: {app_name}")

        #from utils.path_utils import resolve_desktop_path
        ##jar_path = resolve_desktop_path("../MHCLG/sqlite-jar/sqlite-jdbc-3.36.0.3.jar")
        ##jar_path = config['AWS']['S3_SQLITE_JDBC_JAR']
        ##logger.info(f"Using JAR path: {jar_path}")

        # Configure PostgreSQL JDBC driver for EMR Serverless 7.9.0
        # The driver JAR should be available via --jars parameter in EMR configuration
        # Optimized configurations for EMR 7.9.0 (Spark 3.5.x, Java 17)
        spark_session = (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate())
        
        # Set Spark logging level to reduce verbosity
        #set_spark_log_level("WARN")
        
        return spark_session

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}", exc_info=True)
        return None

# -------------------- Metadata Loader --------------------
@log_execution_time
def load_metadata(uri: str) -> dict:
    """
    Load a JSON configuration file from either an S3 URI or a local file path.

    Args:
        uri (str): S3 URI (e.g., s3://bucket/key) or local file path

    Returns:
        dict: Parsed JSON content.

    Raises:
        FileNotFoundError: If the file is not found.
        ValueError: If the file content is invalid.
    """
    logger.info(f"Loading metadata from {uri}")
    try:
        if uri.lower().startswith("s3://"):
            # Handle S3 path
            s3 = boto3.client("s3")
            bucket, key = uri.replace("s3://", "", 1).split("/", 1)
            response = s3.get_object(Bucket=bucket, Key=key)
            return json.load(response["Body"])
        else:
            # Handle local file path or file within .whl package
            try:
                # Try to load using pkgutil if running from .whl
                package_name = __package__ or 'jobs'  # will be 'jobs'
                logger.info(f"Attempting to load from package using pkgutil with package_name: {package_name} and uri: {uri}")
                data = pkgutil.get_data(package_name, uri)
                if data:
                    logger.info("Successfully loaded from package using pkgutil")
                    return json.loads(data.decode('utf-8'))
                else:
                    raise FileNotFoundError(f"pkgutil.get_data could not find {uri}")
            except Exception as e:
                # If pkgutil fails, try to load from the file system
                logger.warning(f"pkgutil.get_data failed: {e}, attempting to read from file system. Error: {e}")
                try:
                    # Check if the path is absolute
                    if os.path.isabs(uri):
                        filepath = uri
                    else:
                        # Construct the absolute path relative to the script's location
                        script_dir = os.path.dirname(os.path.abspath(__file__))
                        filepath = os.path.join(script_dir, uri)

                    logger.info(f"Attempting to load from file system with filepath: {filepath}")
                    with open(filepath, 'r') as f:
                        logger.info("Successfully loaded from file system")
                        return json.load(f)
                except FileNotFoundError as e:
                    logger.error(f"Configuration file not found in file system: {e}")
                    raise
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading metadata from {uri}: {e}")
        raise


# -------------------- Data Reader --------------------
def read_data(spark, input_path):
    try:
        logger.info(f"Reading data from {input_path}")
        return spark.read.csv(input_path, header=True, inferSchema=True)
    
    except Exception as e:
        logger.error(f"Error reading data from {input_path}: {str(e)}")
        raise

# -------------------- Data Transformer --------------------
@log_execution_time
def transform_data(df, schema_name, data_set,spark):      
    try:
        dataset_json_transformed_path = "config/transformed_source.json"
        logger.info(f"transform_data: Transforming data for table: {schema_name} using schema from {dataset_json_transformed_path}")
        json_data = load_metadata(dataset_json_transformed_path)
        logger.info(f"transform_data: Transforming data with schema with json data: {json_data}")
        df.show(5)

        # Extract the list of fields
        fields = []
        if (schema_name == 'fact' or schema_name == 'fact_res' or schema_name == 'entity'):
            fields = json_data.get("schema_fact_res_fact_entity", [])
            logger.info(f"transform_data: Fields to select from json data {fields} for {schema_name}")
        elif (schema_name == 'issue'):
            fields = json_data.get("schema_issue", [])
            logger.info(f"transform_data: Fields to select from json data {fields} for {schema_name}")

        # Replace hyphens with underscores in column names
        for col in df.columns:
            if "-" in col:
                new_col = col.replace("-", "_")
                df = df.withColumnRenamed(col, new_col)
        logger.info(f"transform_data: DataFrame columns after renaming hyphens: {df.columns}")
        df.printSchema()
        logger.info(f"transform_data: DataFrame schema after renaming hyphens")
        df.show(5)

        # Get actual DataFrame columns
        df_columns = df.columns

        # Find fields that are present in both DataFrame and json    
        if set(fields) == set(df.columns):
            logger.info("transform_data: All fields are present in the DataFrame")
        else:
            logger.warning("transform_data: Some fields are missing in the DataFrame")
            
        if schema_name == 'fact_res':
            logger.info("transform_data: Transforming data for Fact Resource table")
            return transform_data_fact_res(df)
        elif schema_name == 'fact':
            logger.info("transform_data: Transforming data for Fact table")
            return transform_data_fact(df)
        elif schema_name == 'entity':
            logger.info("transform_data: Transforming data for Entity table")
            df.show(5)
            return transform_data_entity(df,data_set,spark)
        elif schema_name == 'issue':
            logger.info("transform_data: Transforming data for Issue table")
            return transform_data_issue(df)
        else:
            raise ValueError(f"Unknown table name: {schema_name}")

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


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

# -------------------- S3 Writer --------------------
df_entity = None
@log_execution_time
def write_to_s3_format(df, output_path, dataset_name, table_name):
    output_path=f"s3://development-pd-batch-emr-studio-ws-bucket/csv/{dataset_name}.csv"
    output_path1=f"s3://development-pd-batch-emr-studio-ws-bucket/json/{dataset_name}.json"

    #output_path=f"s3://{env}-collection-data/dataset/{dataset_name}_test.csv"
    try:   
        logger.info(f"write_to_s3_format: Writing data to S3 at {output_path} for dataset {dataset_name}") 
        
        # Check and clean up existing data for this dataset before writing
        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.info(f"write_to_s3_format: Cleaned up {cleanup_summary['objects_deleted']} objects for dataset '{dataset_name}'")
        if cleanup_summary['errors']:
            logger.warning(f"write_to_s3_format: Cleanup had {len(cleanup_summary['errors'])} errors: {cleanup_summary['errors']}")
        logger.debug(f"write_to_s3_format: Full cleanup summary: {cleanup_summary}")

        # Add dataset as partition column
        df = df.withColumn("dataset", lit(dataset_name))
                
        # Convert entry-date to date type and use it for partitioning       
        # Calculate optimal partitions based on data size
        row_count = df.count()
        optimal_partitions = max(1, min(200, row_count // 1000000))  # ~1M records per partition
        
        #adding time stamp to the dataframe for parquet file
        df = df.withColumn("processed_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
        logger.info(f"write_to_s3_format: DataFrame after adding processed_timestamp column")
        df.show(5)
    

        if table_name == 'entity':
            global df_entity
            df_entity.show(5) if df_entity else logger.info("write_to_s3_format: df_entity is None")
            df_entity = df

        logger.info(f"write_to_s3_format: Invoking s3_csv_format for dataset {dataset_name}") 

        df_csv = s3_csv_format(df)
        # Write to S3 with multilevel partitioning
        # Use "append" mode since we already cleaned up the specific dataset partition
        df_csv.show(5)
        df_csv.coalesce(1) \
          .write \
          .mode("overwrite")  \
          .option("header", "true") \
          .csv(output_path)
        
        df_json=flatten_s3_json(df)
        df_json.show(5)
        df_json.coalesce(1) \
          .write \
          .mode("overwrite") \
          .json(output_path1)

        logger.info(f"write_to_s3_format: Successfully wrote {row_count} rows to {output_path} with {optimal_partitions} partitions")

    except Exception as e:
        logger.error(f"write_to_s3_format: Failed to write to S3: {e}", exc_info=True)
        raise

# -------------------- SQLite Writer --------------------
@log_execution_time
def generate_sqlite(df):
    # Step 4: Write to SQLite
    # Write to SQLite using JDBC
    try:
        # Output SQLite DB to Desktop
        # Note: This is commented out as it's not used in production
        # from utils.path_utils import resolve_desktop_path
        # sqlite_path = resolve_desktop_path("../MHCLG/tgt-data/sqlite-output/transport_access_node.db")
        sqlite_path = "/tmp/transport_access_node.db"  # Temporary path for development

        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:sqlite:{sqlite_path}") \
            .option("dbtable", "fact_resource") \
            .option("driver", "org.sqlite.JDBC") \
            .mode("overwrite") \
            .save()
        logger.info('sqlite data inserted successfully')

    except Exception as e:
        logger.error(f"Failed to write to SQLite: {e}", exc_info=True)
        raise

# -------------------- Enhanced Postgres Writer with CSV S3 Import --------------------
@log_execution_time
def write_dataframe_to_postgres(df, table_name, data_set, env, use_jdbc=False):
    """
    Write DataFrame to PostgreSQL using either Aurora S3 import or JDBC.
    
    Args:
        df: PySpark DataFrame to write
        table_name: Target table name
        data_set: Dataset name
        env: Environment name 
        use_jdbc: If True, use JDBC method; if False, use Aurora S3 import (default)
    """
    try:
        method = "JDBC" if use_jdbc else "Aurora S3 Import"
        logger.info(f"Write_PG: Writing to Postgres using {method} for table: {table_name}")
        
        # Only process entity table for now (can be extended to other tables)
        if table_name == 'entity':
            row_count = df.count()
            logger.info(f"Write_PG: Processing {row_count} rows for {table_name} table")
            
            if use_jdbc:
                # Use traditional JDBC method
                logger.info("Write_PG: Using JDBC import method")
                _write_dataframe_to_postgres_jdbc(df, table_name, data_set, env)
            else:
                # Use Aurora S3 import method
                logger.info("Write_PG: Using Aurora S3 import method")
                
                # Determine S3 CSV output path (under the same parquet path)
                csv_output_path = f"s3://{env}-collection-target-data/csv-temp/"
                logger.info(f"Write_PG: CSV output path: {csv_output_path}")
                
                csv_path = None
                try:
                    # Step 1: Write DataFrame to temporary CSV in S3
                    csv_path = write_dataframe_to_csv_s3(
                        df, 
                        csv_output_path, 
                        table_name, 
                        data_set,
                        cleanup_existing=True
                    )
                    logger.info(f"Write_PG: Successfully wrote temporary CSV to S3: {csv_path}")
                    
                    # Step 2: Import CSV from S3 to Aurora (this includes automatic cleanup)
                    # Use actual database table name (ENTITY_TABLE_NAME) instead of logical name
                    import_result = import_csv_to_aurora(
                        csv_path,
                        ENTITY_TABLE_NAME,  # Use actual DB table name (configured in postgres_connectivity.py)
                        data_set,
                        env,
                        use_s3_import=True,
                        truncate_table=True  # Clean existing data for this dataset
                    )
                    
                    if import_result["import_successful"]:
                        logger.info(f"Write_PG: Aurora S3 import completed successfully")
                        logger.info(f"Write_PG: Method used: {import_result['import_method_used']}")
                        logger.info(f"Write_PG: Rows imported: {import_result['rows_imported']}")
                        logger.info(f"Write_PG: Duration: {import_result['import_duration']:.2f} seconds")
                        logger.info("Write_PG: Temporary CSV files have been cleaned up")
                        
                        if import_result.get("warnings"):
                            logger.warning(f"Write_PG: Import warnings: {import_result['warnings']}")
                    else:
                        logger.error(f"Write_PG: Aurora S3 import failed: {import_result['errors']}")
                        logger.info("Write_PG: Falling back to JDBC method")
                        # CSV cleanup happens in import_csv_to_aurora function
                        _write_dataframe_to_postgres_jdbc(df, table_name, data_set, env)
                        
                except Exception as e:
                    logger.error(f"Write_PG: Aurora S3 import failed: {e}")
                    logger.info("Write_PG: Falling back to JDBC method")
                    
                    # Clean up temporary CSV files if they were created but import failed
                    if csv_path:
                        logger.info("Write_PG: Cleaning up temporary CSV files after S3 import failure")
                        cleanup_temp_csv_files(csv_path)
                    
                    _write_dataframe_to_postgres_jdbc(df, table_name, data_set, env)
                        
        else:
            # For non-entity tables, use traditional JDBC method
            logger.info(f"Write_PG: Using JDBC method for non-entity table: {table_name}")
            _write_dataframe_to_postgres_jdbc(df, table_name, data_set)
             
    except Exception as e:
        logger.error(f"Write_PG: Failed to write to Postgres: {e}", exc_info=True)
        raise


def _write_dataframe_to_postgres_jdbc(df, table_name, data_set, env, use_staging=True):
    """
    JDBC writer with optional staging table support.
    
    This method can write directly to the entity table or use a staging table
    to minimize lock contention on the production entity table.
    
    Args:
        df: PySpark DataFrame to write
        table_name: Logical table identifier (e.g., 'entity', 'fact')
        data_set: Dataset name
        use_staging: If True, uses staging table pattern (recommended for high-load scenarios)
    """
    from jobs.dbaccess.postgres_connectivity import get_performance_recommendations
    
    # Get performance recommendations based on dataset size
    row_count = df.count()
    recommendations = get_performance_recommendations(row_count)
    logger.info(f"_write_dataframe_to_postgres_jdbc: Performance recommendations for {row_count} rows: {recommendations}")
    
    conn_params = get_aws_secret(env)
    
    # Use staging pattern for entity table (logical name comparison is correct here)
    if use_staging and table_name == 'entity':
        logger.info("_write_dataframe_to_postgres_jdbc: Using STAGING TABLE pattern for entity table")
        logger.info("_write_dataframe_to_postgres_jdbc: This minimizes lock contention on production table")
        
        try:
            # Step 1: Create staging table
            logger.info("_write_dataframe_to_postgres_jdbc: Step 1/3 - Creating staging table")
            staging_table_name = create_and_prepare_staging_table(
                conn_params=conn_params,
                dataset_value=data_set
            )
            logger.info(f"_write_dataframe_to_postgres_jdbc: Created staging table: {staging_table_name}")
            
            # Step 2: Write data to staging table
            logger.info(f"_write_dataframe_to_postgres_jdbc: Step 2/3 - Writing {row_count:,} rows to staging table")
            write_to_postgres(
                df, 
                data_set,
                conn_params,
                method=recommendations["method"],
                batch_size=recommendations["batch_size"],
                num_partitions=recommendations["num_partitions"],
                target_table=staging_table_name  # Write to staging table
            )
            logger.info(f"_write_dataframe_to_postgres_jdbc: Successfully wrote data to staging table '{staging_table_name}'")

            # Step 2b: Calculate the centroid of the multiplygon data into the point field
            logger.info(
                "_write_dataframe_to_postgres_jdbc: "
                f"Calculating centroids for multipolygon geometries in {staging_table_name}"
            )
            rows_updated = calculate_centroid_wkt(
                conn_params,
                target_table=staging_table_name  # Update centroids in staging table
            )
            logger.info(
                "_write_dataframe_to_postgres_jdbc: "
                f"Updated {rows_updated} centroids in staging table"
            )

            
            # Step 3: Atomically commit staging data to production entity table
            logger.info("_write_dataframe_to_postgres_jdbc: Step 3/3 - Committing staging data to production entity table")
            commit_result = commit_staging_to_production(
                conn_params=conn_params,
                staging_table_name=staging_table_name,
                dataset_value=data_set
            )
            
            if commit_result["success"]:
                logger.info(
                    f"_write_dataframe_to_postgres_jdbc: ✓ STAGING COMMIT SUCCESSFUL - "
                    f"Deleted {commit_result['rows_deleted']:,} old rows, "
                    f"Inserted {commit_result['rows_inserted']:,} new rows in {commit_result['total_duration']:.2f}s"
                )
                logger.info(
                    f"_write_dataframe_to_postgres_jdbc: Lock time on entity table: "
                    f"~{commit_result['total_duration']:.2f}s (vs. several minutes with direct write)"
                )
            else:
                logger.error(f"_write_dataframe_to_postgres_jdbc: Staging commit failed: {commit_result.get('error')}")
                raise Exception(f"Staging commit failed: {commit_result.get('error')}")
            
        except Exception as e:
            logger.error(f"_write_dataframe_to_postgres_jdbc: Staging table approach failed: {e}")
            logger.info("_write_dataframe_to_postgres_jdbc: Falling back to direct write to entity table")
            
            # Fallback to direct write
            write_to_postgres(
                df, 
                data_set,
                conn_params,
                method=recommendations["method"],
                batch_size=recommendations["batch_size"],
                num_partitions=recommendations["num_partitions"]
            )
    else:
        # Direct write to production table (original behavior)
        logger.info(f"_write_dataframe_to_postgres_jdbc: Using DIRECT WRITE to {table_name} table")
        write_to_postgres(
            df, 
            data_set,
            conn_params,
            method=recommendations["method"],
            batch_size=recommendations["batch_size"],
            num_partitions=recommendations["num_partitions"]
        )
    
    logger.info(f"_write_dataframe_to_postgres_jdbc: JDBC writing completed using {recommendations['method']} method")

# -------------------- Main --------------------

env = None
@log_execution_time
def main(args):
    logger.info(f"Main: Initialize logging and invoking initialize_logging method")

    initialize_logging(args)  # Initialize logging with args

    # Determine import method from command line argument
    use_jdbc = hasattr(args, 'use_jdbc') and args.use_jdbc
    import_method = "JDBC" if use_jdbc else "Aurora S3 Import"
    logger.info(f"Main: Using import method: {import_method}")

    logger.info(f"Main: Starting ETL process for Collection Data {args.load_type} and dataset {args.data_set}")
    try: 
        logger.info("Main: Starting main ETL process for collection Data")          
        start_time = datetime.now()
        logger.info(f"Main: Spark session started at: {start_time}")    
        #s3://development-collection-data/transport-access-node-collection/transformed/transport-access-node
        load_type = args.load_type
        data_set = args.data_set
        s3_uri = args.path
        global env
        env = args.env
        logger.info(f"Main: env variable for the dataset: {env}")

        s3_uri = s3_uri + data_set + "-collection"

        table_names=["fact","fact_res","entity","issue"]
        
        spark = create_spark_session()

        if spark is None:
            raise Exception("Failed to create Spark session")
        logger.info(f"Main: Spark session created successfully for dataset: {data_set}")
        
        #TODO :remove below line after testing
        #sedona_test()

        if(load_type == 'full'):
            #invoke full load logic
            logger.info(f"Main: Load type is {load_type} and dataset is {data_set} and path is {s3_uri}")

            output_path = f"s3://{env}-collection-target-data/"
            logger.info(f" Main: Target output path: {output_path}")
                         
            df = None  # Initialise df to avoid UnboundLocalError
            
            for table_name in table_names:
                if(table_name== 'fact' or table_name== 'fact_res' or table_name== 'entity'):
                    full_path = f"{s3_uri}"+"/transformed/"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                
                    if df is None:
                        # Read CSV using the dynamic schema
                        logger.info("Main: dataframe is empty")
                        df = spark.read.option("header", "true").csv(full_path)
                        df.cache()  # Cache the DataFrame for performance
                    
                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    df.show(5)

                    #revise this code and for converting spark session as singleton in future
                    processed_df = transform_data(df,table_name,data_set,spark)
                    logger.info(f"Main: Transforming data for {table_name} table completed")
                    df.show(5)

                    # Write to S3 for Fact Resource table
                    write_to_s3(processed_df, f"{output_path}{table_name}", data_set,table_name)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")
                    if(table_name== 'entity'):  
                        write_to_s3_format(processed_df, f"{output_path}{table_name}", data_set,table_name)  
                elif(table_name== 'issue'):
                    full_path = f"{s3_uri}"+"/issue/"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    # Read CSV using the dynamic schema
                    df = spark.read.option("header", "true").csv(full_path)
                    df.cache()  # Cache the DataFrame for performance

                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    df.show(5)
                    processed_df = transform_data(df,table_name,data_set,spark)                                      

                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact table
                    write_to_s3(processed_df, f"{output_path}{table_name}", data_set, table_name)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")              

            logger.info("Main: Writing to target s3 output path: process completed")

            # Write dataframe to Postgres for Entity table
            global df_entity 
            if df_entity is not None:
                df_entity.show(5)
                df_entity = df_entity.drop("processed_timestamp","year","month", "day")    
                table_name = 'entity'
                logger.info(f"Main: before writing to postgres, df_entity dataframe is below")
                df_entity.show(5)
                write_dataframe_to_postgres(df_entity, table_name, data_set, env, use_jdbc)
            else:
                logger.info("Main: df_entity is None, skipping Postgres write")

        elif(load_type == 'delta'):
            #invoke delta load logic
            logger.info(f"Main: Delta load type specified: {load_type}")
            
        elif(load_type == 'sample'):
            #invoke sample load logic
            logger.info(f"Main: Sample load type is {load_type} and dataset is {data_set} and path is {s3_uri}")    
                       
            logger.info(f"Main: Processing dataset with path information : {s3_uri}")         

            logger.info("Main: Set target s3 output path")
            output_path = f"s3://{env}-collection-target-data/"
            logger.info(f" Main: Target output path: {output_path}")
                         
            df = None  # Initialise df to avoid UnboundLocalError
            
            for table_name in table_names:
                if(table_name== 'fact' or table_name== 'fact_res' or table_name== 'entity'):
                    full_path = f"{s3_uri}"+"/transformed/sample-"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    
                    if df is None:
                        # Read CSV using the dynamic schema
                        logger.info("Main: dataframe is empty")
                        df = spark.read.option("header", "true").csv(full_path)
                        df.cache()  # Cache the DataFrame for performance
                    
                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    df.show(5)
                    #revise this code and for converting spark session as singleton in future
                    processed_df = transform_data(df,table_name,data_set,spark)
                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact Resource table  
                    sample_dataset_name = f"sample-{data_set}"
                    write_to_s3(processed_df, f"{output_path}{table_name}", sample_dataset_name, table_name)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")

                    # Write to Postgres for Entity table
                    if (table_name == 'entity'):
                        write_dataframe_to_postgres(processed_df, table_name, data_set, env, use_jdbc)
                        logger.info(f"Main: Writing to Postgres for {table_name} table completed")  


                elif(table_name== 'issue'):
                    full_path = f"{s3_uri}"+"/issue/"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    # Read CSV using the dynamic schema
                    df = spark.read.option("header", "true").csv(full_path)
                    df.cache()  # Cache the DataFrame for performance

                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    df.show(5)
                    processed_df = transform_data(df,table_name,data_set,spark)                                      

                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact table
                    sample_dataset_name = f"sample-{data_set}"
                    write_to_s3(processed_df, f"{output_path}{table_name}", sample_dataset_name, table_name)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")                                     
        else:
            logger.error(f"Main: Invalid load type specified: {load_type}")
            raise ValueError(f"Invalid load type: {load_type}")        

    except Exception as e:
        logger.exception("Main: An error occurred during the ETL process: %s", str(e))
    finally:
        if 'spark' in locals():
            try:
                spark.stop()
                logger.info(f"Main: Spark session stopped")
            except:
                logger.warning("Main: Error stopping Spark session")
            
        if 'start_time' in locals():
            try:
                end_time = datetime.now()
                logger.info(f"Spark session ended at: {end_time}")
                # Duration
                duration = end_time - start_time
                logger.info(f"Total duration: {duration}")
            except:
                logger.warning("Main: Error calculating duration")
        else:
            logger.info("Main: ETL process completed (no timing information available)")