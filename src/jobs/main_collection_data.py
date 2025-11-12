import os
import boto3
import pkgutil
import json
import sys
import argparse
from jobs.transform_collection_data import (transform_data_fact, transform_data_fact_res,
                                      transform_data_issue, transform_data_entity) 
from jobs.utils.s3_utils import cleanup_dataset_data
from datetime import datetime

from pyspark.sql import SparkSession

# Import the new logging module
from jobs.utils.logger_config import setup_logging, get_logger, log_execution_time, set_spark_log_level
from jobs.utils.s3_writer_utils import write_to_s3, write_to_s3_format
from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
from jobs.utils.df_utils import show_df

# -------------------- Logging Setup --------------------
# Setup logging for EMR Serverless (console output goes to CloudWatch automatically)
def initialize_logging(args):
    try:
        setup_logging(
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE") if os.getenv("LOG_FILE") else None,
            environment=args.env
        )
    except AttributeError as e:
        print(f"Error: Missing required argument attribute: {e}")
        raise
    except Exception as e:
        print(f"Error initializing logging: {e}")
        raise

logger = get_logger(__name__)
df_entity = None

# -------------------- Spark Session --------------------
@log_execution_time
def create_spark_session(app_name="EMR Transform Job"):
    try:
        logger.info(f"Creating Spark session with app name: {app_name}")

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
            except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError, AttributeError) as e:
                # If pkgutil fails, try to load from the file system
                logger.warning(f"pkgutil.get_data failed: {e}, attempting to read from file system")
                try:
                    # Check if the path is absolute
                    if os.path.isabs(uri):
                        filepath = uri
                    else:
                        # Construct the absolute path relative to the script's location
                        script_dir = os.path.dirname(os.path.abspath(__file__))
                        filepath = os.path.normpath(os.path.join(script_dir, uri))
                        
                        # Validate path doesn't escape base directory
                        if not filepath.startswith(script_dir):
                            raise ValueError(f"Invalid file path: path traversal detected in {uri}")

                    logger.info(f"Attempting to load from file system with filepath: {filepath}")
                    with open(filepath, 'r') as f:
                        logger.info("Successfully loaded from file system")
                        return json.load(f)
                except FileNotFoundError as e:
                    logger.error(f"Configuration file not found in file system: {e}")
                    raise
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"Error reading or parsing file: {e}")
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
        # Validate DataFrame
        if df is None:
            raise ValueError("DataFrame is None")
        if df.rdd.isEmpty():
            raise ValueError("DataFrame is empty")
        
        dataset_json_transformed_path = "config/transformed_source.json"
        logger.info(f"transform_data: Transforming data for table: {schema_name} using schema from {dataset_json_transformed_path}")
        json_data = load_metadata(dataset_json_transformed_path)
        logger.info(f"transform_data: Transforming data with schema with json data: {json_data}")
        show_df(df, 5, env)

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
        show_df(df, 5, env)

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
            show_df(df, 5, env)
            return transform_data_entity(df,data_set,spark,env)
        elif schema_name == 'issue':
            logger.info("transform_data: Transforming data for Issue table")
            return transform_data_issue(df)
        else:
            raise ValueError(f"Unknown table name: {schema_name}")

    except (ValueError, KeyError, AttributeError) as e:
        logger.error(f"transform_data:Error transforming data: {e}")
        raise
    except Exception as e:
        logger.error(f"transform_data: Unexpected error transforming data: {e}", exc_info=True)
        raise

# -------------------- Main --------------------

# -------------------- Input Validation --------------------
def validate_s3_path(s3_path):
    """Validate S3 path format."""
    if not s3_path or not isinstance(s3_path, str):
        raise ValueError("S3 path must be a non-empty string")
    if not s3_path.startswith('s3://'):
        raise ValueError(f"Invalid S3 path format: {s3_path}. Must start with s3://")
    if len(s3_path) <= 5:
        raise ValueError(f"Invalid S3 path: {s3_path}. Path too short")

env = None
@log_execution_time
def main(args):
    global env, df_entity
    logger.info(f"Main: Initialize logging and invoking initialize_logging method")

    initialize_logging(args)  # Initialize logging with args

    # Validate input arguments
    if not hasattr(args, 'load_type') or not args.load_type:
        raise ValueError("main:load_type is required")
    if not hasattr(args, 'data_set') or not args.data_set:
        raise ValueError("main:data_set is required")
    if not hasattr(args, 'path') or not args.path:
        raise ValueError("main:path is required")
    if not hasattr(args, 'env') or not args.env:
        raise ValueError("main:env is required")
    
    # Validate load_type
    allowed_load_types = ['full', 'delta', 'sample']
    if args.load_type not in allowed_load_types:
        raise ValueError(f"Invalid load_type: {args.load_type}. Must be one of {allowed_load_types}")
    
    # Validate environment
    allowed_envs = ['development', 'staging', 'production', 'local']
    if args.env not in allowed_envs:
        raise ValueError(f"Invalid env: {args.env}. Must be one of {allowed_envs}")
    
    # Validate S3 path
    validate_s3_path(args.path)

    # Determine import method from command line argument
    use_jdbc = hasattr(args, 'use_jdbc') and args.use_jdbc
    import_method = "JDBC" if use_jdbc else "Aurora S3 Import"
    logger.info(f"Main: Using import method: {import_method}")

    logger.info(f"Main: Starting ETL process for Collection Data {args.load_type} and dataset {args.data_set}")
    try: 
        logger.info("Main: Starting main ETL process for collection Data")          
        start_time = datetime.now()
        logger.info(f"Main: Spark session started at: {start_time}")    
        load_type = args.load_type
        data_set = args.data_set
        s3_uri = args.path
        env = args.env
        logger.info(f"Main: env variable for the dataset: {env}")

        s3_uri = s3_uri + data_set + "-collection"

        table_names=["fact","fact_res","entity","issue"]
        
        spark = create_spark_session()

        if spark is None:
            raise Exception("Failed to create Spark session")
        logger.info(f"Main: Spark session created successfully for dataset: {data_set}")
        
        if(load_type == 'full'):
            #invoke full load logic
            logger.info(f"Main: Load type is {load_type} and dataset is {data_set} and path is {s3_uri}")

            output_path = f"s3://{env}-parquet-datasets/"
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
                    show_df(df, 5, env)

                    if(table_name== 'entity'):
                        logger.info(f"Main: Invocation of write_to_s3_format method for {table_name} table")
                        df_entity = write_to_s3_format(df, f"{output_path}{table_name}", data_set,table_name,spark,env)

                    #TODO: revise this code and for converting spark session as singleton in future
                    processed_df = transform_data(df,table_name,data_set,spark)
                    logger.info(f"Main: Transforming data for {table_name} table completed")
                    show_df(df, 5, env)

                    # Write to S3 for Fact Resource table
                    write_to_s3(processed_df, f"{output_path}{table_name}", data_set,table_name,env)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")
                elif(table_name== 'issue'):
                    full_path = f"{s3_uri}"+"/issue/"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    # Read CSV using the dynamic schema
                    df = spark.read.option("header", "true").csv(full_path)
                    df.cache()  # Cache the DataFrame for performance

                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    show_df(df, 5, env)
                    processed_df = transform_data(df,table_name,data_set,spark)                                      

                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact table
                    write_to_s3(processed_df, f"{output_path}{table_name}", data_set, table_name, env)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")              

            logger.info("Main: Writing to target s3 output path: process completed")

            # Write dataframe to Postgres for Entity table
            if df_entity is not None:
                show_df(df_entity, 5, env)
                df_entity = df_entity.drop("processed_timestamp","year","month", "day")    
                table_name = 'entity'
                logger.info(f"Main: before writing to postgres, df_entity dataframe is below")
                show_df(df_entity, 5, env)
                write_dataframe_to_postgres_jdbc(df_entity, table_name, data_set, env, use_staging=True)
            else:
                logger.info("Main: df_entity is None, skipping Postgres write")

        # elif(load_type == 'delta'):
        #     #invoke delta load logic
        #     logger.info(f"Main: Delta load type specified: {load_type}")
            
        # elif(load_type == 'sample'):
        #     #invoke sample load logic
        #     logger.info(f"Main: Sample load type is {load_type} and dataset is {data_set} and path is {s3_uri}")    
                       
        #     logger.info(f"Main: Processing dataset with path information : {s3_uri}")         

        #     logger.info("Main: Set target s3 output path")
        #     output_path = f"s3://{env}-parquet-datasets/"
        #     logger.info(f" Main: Target output path: {output_path}")
                         
        #     df = None  # Initialise df to avoid UnboundLocalError
            
        #     for table_name in table_names:
        #         if(table_name== 'fact' or table_name== 'fact_res' or table_name== 'entity'):
        #             full_path = f"{s3_uri}"+"/transformed/sample-"+data_set+"/*.csv"
        #             logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    
        #             if df is None:
        #                 # Read CSV using the dynamic schema
        #                 logger.info("Main: dataframe is empty")
        #                 df = spark.read.option("header", "true").csv(full_path)
        #                 df.cache()  # Cache the DataFrame for performance
                    
        #             # Show schema and sample data 
        #             df.printSchema() 
        #             logger.info(f"Main: Schema information for the loaded dataframe")
        #             df.show(5)
        #             #revise this code and for converting spark session as singleton in future
        #             processed_df = transform_data(df,table_name,data_set,spark)
        #             logger.info(f"Main: Transforming data for {table_name} table completed")

        #             # Write to S3 for Fact Resource table  
        #             sample_dataset_name = f"sample-{data_set}"
        #             write_to_s3(processed_df, f"{output_path}{table_name}", sample_dataset_name, table_name)
        #             logger.info(f"Main: Writing to s3 for {table_name} table completed")

        #             # Write to Postgres for Entity table
        #             if (table_name == 'entity'):
        #                 write_dataframe_to_postgres(processed_df, table_name, data_set, env, use_jdbc)
        #                 logger.info(f"Main: Writing to Postgres for {table_name} table completed")  


        #         elif(table_name== 'issue'):
        #             full_path = f"{s3_uri}"+"/issue/"+data_set+"/*.csv"
        #             logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
        #             # Read CSV using the dynamic schema
        #             df = spark.read.option("header", "true").csv(full_path)
        #             df.cache()  # Cache the DataFrame for performance

        #             # Show schema and sample data 
        #             df.printSchema() 
        #             logger.info(f"Main: Schema information for the loaded dataframe")
        #             df.show(5)
        #             processed_df = transform_data(df,table_name,data_set,spark)                                      

        #             logger.info(f"Main: Transforming data for {table_name} table completed")

        #             # Write to S3 for Fact table
        #             sample_dataset_name = f"sample-{data_set}"
        #             write_to_s3(processed_df, f"{output_path}{table_name}", sample_dataset_name, table_name)
        #             logger.info(f"Main: Writing to s3 for {table_name} table completed")                                     
        else:
            logger.error(f"Main: Invalid load type specified: {load_type}")
            raise ValueError(f"Invalid load type: {load_type}")        

    except (ValueError, AttributeError, KeyError) as e:
        logger.exception("Main: An error occurred during the ETL process: %s", str(e))
        raise
    except Exception as e:
        logger.exception("Main: Unexpected error during the ETL process: %s", str(e))
        raise
    finally:
        if 'spark' in locals() and spark is not None:
            try:
                spark.stop()
                logger.info("Main: Spark session stopped")
            except (AttributeError, Exception) as e:
                logger.warning(f"Main: Error stopping Spark session: {e}")
            
        if 'start_time' in locals() and start_time is not None:
            try:
                end_time = datetime.now()
                logger.info(f"Spark session ended at: {end_time}")
                duration = end_time - start_time
                logger.info(f"Total duration: {duration}")
            except (AttributeError, TypeError) as e:
                logger.warning(f"Main: Error calculating duration: {e}")
        else:
            logger.info("Main: ETL process completed (no timing information available)")