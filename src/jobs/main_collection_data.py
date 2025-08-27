import configparser
import os
import boto3
import pkgutil
import json
import sys
import argparse
from jobs.transform_collection_data import (transform_data_fact, transform_data_fact_res,
                                       transform_data_issue, transform_data_entity) 
from jobs.dbaccess.postgres_connectivity import create_table,write_to_postgres, get_aws_secret
from jobs.utils.s3_utils import cleanup_dataset_data
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
setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    # Note: In EMR Serverless, console logs are automatically captured by CloudWatch
    # File logging is optional for local debugging
    log_file=os.getenv("LOG_FILE") if os.getenv("LOG_FILE") else None,
    environment=os.getenv("ENVIRONMENT", "production")
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
        spark_session = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
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
                package_name = __package__  # will be 'jobs'
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

        # Extract the list of fields
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
        df.show()

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
@log_execution_time
def write_to_s3(df, output_path, dataset_name):
    try:   
        logger.info(f"Writing data to S3 at {output_path} for dataset {dataset_name}") 
        
        # Check and clean up existing data for this dataset before writing
        cleanup_summary = cleanup_dataset_data(output_path, dataset_name)
        logger.debug(f"S3 cleanup summary: {cleanup_summary}")
        
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
        
        # Write to S3 with multilevel partitioning
        df.coalesce(optimal_partitions) \
          .write \
          .partitionBy("dataset", "year", "month", "day") \
          .mode("overwrite") \
          .option("maxRecordsPerFile", 1000000) \
          .option("compression", "snappy") \
          .parquet(output_path)
        
        logger.info(f"Successfully wrote {row_count} rows to {output_path} with {optimal_partitions} partitions")
        
    except Exception as e:
        logger.error(f"Failed to write to S3: {e}", exc_info=True)
        raise

# -------------------- SQLite Writer --------------------

def generate_sqlite(df):
    # Step 4: Write to SQLite
    # Write to SQLite using JDBC
    try:
        # Output SQLite DB to Desktop
        from utils.path_utils import resolve_desktop_path
        sqlite_path = resolve_desktop_path("../MHCLG/tgt-data/sqlite-output/transport_access_node.db")

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



# -------------------- Main --------------------
@log_execution_time
def main(args):
    logger.info(f"Main: Starting ETL process for Collection Data {args.load_type} and dataset {args.data_set}")
    try: 

        logger.info("Main: Starting main ETL process for collection Data")          
        start_time = datetime.now()
        logger.info(f"Main: Spark session started at: {start_time}")    
        #s3://development-collection-data/transport-access-node-collection/transformed/transport-access-node
        load_type = args.load_type
        data_set = args.data_set
        s3_uri = args.path
        env=args.env
        logger.info(f"Main: env variable for the dataset: {env}")

        s3_uri=s3_uri+data_set+"-collection"

        table_names=["fact","fact_res","entity","issue"]
        
        spark = create_spark_session()
        logger.info(f"Main: Spark session created successfully for dataset: {data_set}")

        if(load_type == 'full'):
            
            #invoke full load logic
            logger.info(f"Main: Full load type specified: {load_type}")
            logger.info(f"Main: Load type is {load_type} and dataset is {data_set} and path is {s3_uri}")             
            
       
            logger.info(f"Main: Processing dataset with path information : {s3_uri}")            

            # todo: for coming sprint
            #write_to_postgres(processed_df, config)
            #logger.info(f"writing data to postgress")
            ##generate_sqlite(processed_df)

            logger.info("Main: Set target s3 output path")
            output_path = f"s3://{env}-pyspark-assemble-parquet/{data_set}/"
            logger.info(f" Main: Target output path: {output_path}")
                         
            df = None  # Initialise df to avoid UnboundLocalError
            
            for table_name in table_names:
                if(table_name== 'fact' or table_name== 'fact_res' or table_name== 'entity'):
                    full_path = f"{s3_uri}"+"/transformed/"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    
                    if df is None or df.rdd.isEmpty():
                        # Read CSV using the dynamic schema
                        logger.info("Main: dataframe is empty")
                        df = spark.read.option("header", "true").csv(full_path)
                        df.cache()  # Cache the DataFrame for performance
                    
                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    df.show()
                    #revise this code and for converting spark session as singleton in future
                    processed_df = transform_data(df,table_name,data_set,spark)
                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact Resource table
                    write_to_s3(processed_df, f"{output_path}output-parquet-{table_name}", data_set)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")

                      # Write to Postgres for Entity table using optimized method
                    if (table_name == 'entity'):
                        from jobs.dbaccess.postgres_connectivity import get_performance_recommendations
                        
                        # Get performance recommendations based on dataset size
                        row_count = processed_df.count()
                        recommendations = get_performance_recommendations(row_count)
                        logger.info(f"Main: Performance recommendations for {row_count} rows: {recommendations}")
                        
                        # Write to PostgreSQL using optimized JDBC writer with recommendations
                        write_to_postgres(
                            processed_df, 
                            data_set,
                            get_aws_secret(),
                            method=recommendations["method"],
                            batch_size=recommendations["batch_size"],
                            num_partitions=recommendations["num_partitions"]
                        )
                        logger.info(f"Main: Writing to Postgres for {table_name} table completed using {recommendations['method']} method")
                        
                        # Note: For SQLite conversion, use the separate parquet_to_sqlite.py script:
                        # python src/jobs/parquet_to_sqlite.py --input s3://bucket/output-parquet-entity --output ./entity.sqlite  

                elif(table_name== 'issue'):
                    full_path = f"{s3_uri}"+"/issue/"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    # Read CSV using the dynamic schema
                    df = spark.read.option("header", "true").csv(full_path)
                    df.cache()  # Cache the DataFrame for performance

                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    df.show()
                    processed_df = transform_data(df,table_name,data_set,spark)                                      

                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact table
                    write_to_s3(processed_df, f"{output_path}output-parquet-{table_name}", data_set)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")  

            logger.info("Main: Writing to target s3 output path: process completed")           
        elif(load_type == 'delta'):
            #invoke delta load logic
            logger.info(f"Main: Delta load type specified: {load_type}")


            
        elif(load_type == 'sample'):
            #invoke sample load logic
            logger.info(f"Main: Sample load type specified: {load_type}")
            logger.info(f"Main: Load type is {load_type} and dataset is {data_set} and path is {s3_uri}")    
                       

                     
            logger.info(f"Main: Processing dataset with path information : {s3_uri}")         

            logger.info("Main: Set target s3 output path")
            output_path = f"s3://{env}-pyspark-assemble-parquet/sample-{data_set}/"
            logger.info(f" Main: Target output path: {output_path}")
                         
            df = None  # Initialise df to avoid UnboundLocalError
            
            for table_name in table_names:
                if(table_name== 'fact' or table_name== 'fact_res' or table_name== 'entity'):
                    full_path = f"{s3_uri}"+"/transformed/sample-"+data_set+"/*.csv"
                    logger.info(f"Main: Dataset input path including csv file path: {full_path}")
                    
                    
                    if df is None or df.rdd.isEmpty():
                        # Read CSV using the dynamic schema
                        logger.info("Main: dataframe is empty")
                        df = spark.read.option("header", "true").csv(full_path)
                        df.cache()  # Cache the DataFrame for performance
                    
                    # Show schema and sample data 
                    df.printSchema() 
                    logger.info(f"Main: Schema information for the loaded dataframe")
                    df.show()
                    #revise this code and for converting spark session as singleton in future
                    processed_df = transform_data(df,table_name,data_set,spark)
                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact Resource table
                    write_to_s3(processed_df, f"{output_path}output-parquet-{table_name}", data_set)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")

                    # Write to Postgres for Entity table
                    if (table_name == 'entity'):
                        write_to_postgres(processed_df, data_set,get_aws_secret())
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
                    df.show()
                    processed_df = transform_data(df,table_name,data_set,spark)                                      

                    logger.info(f"Main: Transforming data for {table_name} table completed")

                    # Write to S3 for Fact table
                    write_to_s3(processed_df, f"{output_path}output-parquet-{table_name}", data_set)
                    logger.info(f"Main: Writing to s3 for {table_name} table completed")                                     
        else:
            logger.error(f"Main: Invalid load type specified: {load_type}")
            raise ValueError(f"Invalid load type: {load_type}")        

    except Exception as e:
        logger.exception("Main: An error occurred during the ETL process: %s", str(e))
    finally:
        spark.stop()
        logger.info(f"Main: Spark session stopped")
            
        end_time = datetime.now()
        logger.info(f"Spark session ended at: {end_time}")
        # Duration
        duration = end_time - start_time
        logger.info(f"Total duration: {duration}")