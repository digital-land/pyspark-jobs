import configparser
import logging
import os
import boto3
import pkgutil
import json
import sys
import argparse
from jobs.transform_collection_data import (transform_data_fact, transform_data_fact_res,
                                       transform_data_issues, transform_data_entity) 
#import sqlite3
from datetime import datetime
from dataclasses import fields
from logging import config
from logging.config import dictConfig
#from jobs import transform_collection_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce,collect_list,concat_ws,dayofmonth,expr,first,month,to_date,year,row_number)
from pyspark.sql.types import (StringType,StructField,StructType,TimestampType)
from pyspark.sql.window import Window

#from utils.path_utils import load_json_from_repo

# -------------------- Logging Configuration --------------------
LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "[%(asctime)s] %(levelname)s - %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "INFO",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}

dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# -------------------- Spark Session --------------------
def create_spark_session(config,app_name="EMR Transform Job"):
    try:
        logger.info(f"Creating Spark session with app name: {app_name}")

        #from utils.path_utils import resolve_desktop_path
        ##jar_path = resolve_desktop_path("../MHCLG/sqlite-jar/sqlite-jdbc-3.36.0.3.jar")
        ##jar_path = config['AWS']['S3_SQLITE_JDBC_JAR']
        ##logger.info(f"Using JAR path: {jar_path}")

        ##spark_session= SparkSession.builder.appName(app_name) \
        ##   .config("spark.jars", jar_path) \
        ##   .getOrCreate()
        spark_session = SparkSession.builder.appName(app_name).getOrCreate()
        return spark_session

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}", exc_info=True)
        return None

# -------------------- Metadata Loader --------------------

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
def transform_data(df, table_name):      
    try:
        dataset_json_transformed_path = "config/transformed_source.json"
        logger.info(f"transform_data: Transforming data for table: {table_name} using schema from {dataset_json_transformed_path}")
        json_data = load_metadata(dataset_json_transformed_path)
        logger.info(f"transform_data: Transforming data with schema with json data: {json_data}")

        # Extract the list of fields
        fields = json_data.get("transport-access-node", [])
        logger.info(f"transform_data: Fields to select from json data {fields}")

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
            
        if table_name == 'fact-res':
            logger.info("transform_data: Transforming data for Fact Resource table")
            return transform_data_fact_res(df)
        elif table_name == 'fact':
            logger.info("transform_data: Transforming data for Fact table")
            return transform_data_fact(df)
        elif table_name == 'entity':
            logger.info("transform_data: Transforming data for Entity table")
            return transform_data_entity(df)
        elif table_name == 'issues':
            logger.info("transform_data: Transforming data for Issues table")
            return transform_data_issues(df)
        else:
            raise ValueError(f"Unknown table name: {table_name}")

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

# -------------------- S3 Writer --------------------
def write_to_s3(df, output_path):
    try:   
        logger.info(f"Writing data to S3 at {output_path}") 
        
        # Convert entry-date to date type and use it for partitioning
        df = df.withColumn("entry_date_parsed", to_date("entry_date", "yyyy-MM-dd"))
        df = df.withColumn("year", year("entry_date_parsed")) \
              .withColumn("month", month("entry_date_parsed")) \
              .withColumn("day", dayofmonth("entry_date_parsed"))
        
        # Drop the temporary parsing column
        df = df.drop("entry_date_parsed")
        
        # Write to S3 partitioned by year, month, day
        df.write \
          .partitionBy("year", "month", "day") \
          .mode("overwrite") \
          .option("header", "true") \
          .parquet(output_path)
        
        logger.info(f"Successfully wrote data to {output_path}")
        
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

# -------------------- PostgreSQL Writer --------------------

##writing to postgres db
def write_to_postgres(df, config):
    try:
        logger.info(f"Writing data to PostgreSQL table {config['TABLE_NAME']}")
        df.write \
            .format(config['PG_JDBC']) \
            .option("url", config['PG_URL']) \
            .option("dbtable", config['TABLE_NAME']) \
            .option("user", config['USER_NAME']) \
            .option("password", config['PASSWORD']) \
            .option("driver", config['DRIVER']) \
            .mode("overwrite") \
            .save()
    except Exception as e:
        logger.error(f"Failed to write to PostgreSQL: {e}", exc_info=True)
        raise



# -------------------- Main --------------------
def main(args):
    logger.info(f"Main: Starting ETL process for Collection Data {args.load_type} and dataset {args.data_set}")
    try:        
        load_type = args.load_type
        data_set = args.data_set
        path = args.path

        logger.info(f"Main: Load type is {load_type} and dataset is {data_set} and path is {path}")

        logger.info("Main: Starting main ETL process for collection Data")          
        start_time = datetime.now()
        logger.info(f"Main: Spark session started at: {start_time}")  
        
        # Define paths to JSON configuration files
        dataset_json_path = "config/datasets.json"  
        # Relative path within the package
        logger.info(f"Main: JSON configuration files path for datasets: {dataset_json_path}")              
         # Load AWS configuration
        config_json_datasets = load_metadata(dataset_json_path)
        logger.info(f"Main: JSON configuration files for config files: {config_json_datasets}")
 
        for dataset, path_info in config_json_datasets.items():
            if not path_info.get("enabled", False):
                logger.info(f"Main: Skipping dataset with false as enabled flag: {dataset}")
                continue
            logger.info(f"Main: Started Processing enabled dataset : {dataset}")
            logger.info(f"Main: Processing dataset with path information : {path_info}")
            
            full_path = f"{path_info['path']}*.csv"
            logger.info(f"Main: Dataset input path including csv file path: {full_path}")


            spark = create_spark_session(config_json_datasets)
            logger.info(f"Main: Spark session created successfully for dataset: {dataset}")

            # Read CSV using the dynamic schema
            df = spark.read.option("header", "true").csv(full_path)
            df.cache()  # Cache the DataFrame for performance

            # Show schema and sample data 
            df.printSchema() 
            logger.info(f"Main: Schema information for the loaded dataframe")
            df.show()

            # todo: for coming sprint
            #write_to_postgres(processed_df, config)
            #logger.info(f"writing data to postgress")
            ##generate_sqlite(processed_df)

            logger.info("Main: Writing to target s3 output path: process started")
            output_path = f"s3://development-collection-data/emr-data-processing/assemble-parquet/{dataset}/"
            logger.info(f" Main: Writing to output path: {output_path}")

            processed_df = transform_data(df,'fact-res')
            logger.info("Main: Transforming data for FACT RESOURCE table completed")

            # Write to S3 for Fact Resource table
            write_to_s3(processed_df, f"{output_path}output-parquet-fact-res")
            logger.info("Main: Writing to s3 for FACT RESOURCE table completed")

            processed_df = transform_data(df,'fact')
            logger.info("Main: Transforming data for FACT table completed")

            # Write to S3 for Fact table
            write_to_s3(processed_df, f"{output_path}output-parquet-fact")
            logger.info("Main: Writing to s3 for FACT table completed")

            logger.info("Main: Writing to target s3 output path: process completed")           

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


#if __name__ == "__main__":    
    #main()
