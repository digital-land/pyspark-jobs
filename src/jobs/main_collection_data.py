import configparser
import logging
import os
import boto3
import pkgutil
import json
import sys
#import sqlite3

from datetime import datetime
from dataclasses import fields
from logging import config
from logging.config import dictConfig
from jobs import transform_collection_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce,collect_list,concat_ws,dayofmonth,expr,first,month,to_date,year)
from pyspark.sql.types import (StringType,StructField,StructType,TimestampType)
from pyspark.sql.exceptions import AnalysisException
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

# -------------------- Json File Reader --------------------
def read_json_file(file_path):    
    """
    Reads a JSON file from either a local path or an S3 URI.
    
    Args:
        file_path (str): Path to the JSON file. Can be local or S3 URI.
    
    Returns:
        dict: Parsed JSON content.
    """    
    if file_path.startswith("s3://"):
        try:
            s3 = boto3.client("s3")
            bucket, key = file_path.replace("s3://", "").split("/", 1)
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except Exception as e:
            logger.error(f"Failed to read JSON from S3: {e}")
            raise
    else:
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except Exception as e:
            logger.error(f"Failed to read JSON from local path: {e}")
            raise

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
# def load_metadata(s3_uri):
#     """
#     Load a JSON configuration file from an S3 URI.
#     Example URI: S3://development-collection-data/emr-data-processing/src0/pyspark-jobs/src/config/configuration.json
#     """
#     logger.info(f"Loading metadata from {s3_uri}")

#     if not s3_uri.startswith("s3://"):
#         raise ValueError("Invalid S3 URI")
#     try:
#         s3 = boto3.client('s3')
#         parts = s3_uri.replace("s3://", "").split("/", 1)
#         bucket = parts[0]
#         key = parts[1]
    
#         response = s3.get_object(Bucket=bucket, Key=key)
#         return json.load(response['Body'])
  
#     except Exception as e:
#         logger.exception(f"Unexpected error while loading metadata from {s3_uri}")
#         raise


def load_metadata(uri: str) -> dict:
    """
    Load a JSON configuration file from either an S3 URI or a package-relative path.

    Args:
        uri (str): S3 URI (e.g., s3://bucket/key) or package-relative path (e.g., config/datasets.json)

    Returns:
        dict: Parsed JSON content.

    Raises:
        FileNotFoundError: If the file is not found.
        ValueError: If the file content is invalid.
    """
    logger.info(f"Loading metadata from {uri}")

    try:
        if uri.lower().startswith("s3://"):
            s3 = boto3.client("s3")
            bucket, key = uri.replace("s3://", "", 1).split("/", 1)
            response = s3.get_object(Bucket=bucket, Key=key)
            return json.load(response["Body"])
        else:
            data = pkgutil.get_data(__package__, uri)
            if data is None:
                raise FileNotFoundError(f"File not found in package: {uri}")
            return json.loads(data.decode("utf-8"))

    except Exception as e:
        logger.exception(f"Error loading metadata from {uri}: {e}")
        raise


# -------------------- Data Reader --------------------
def read_data(spark, input_path):
    try:
        logger.info(f"Reading data from {input_path}")
        return spark.read.csv(input_path, header=True, inferSchema=True)
    
    except AnalysisException as ae:
        logger.error(f"Spark analysis error while reading data from {input_path}: {ae}")
        raise
    except FileNotFoundError:
        logger.error(f"File not found: {input_path}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error while reading data from {input_path}")
        raise

# -------------------- Data Transformer --------------------
def transform_data(df, table_name):      
    try:
        json_data = read_json_file("src/config/transformed-source.json")
        logger.info(f"Transforming data with schema: {json_data}")

        # Extract the list of fields
        fields = json_data.get("transport-access-node", [])
        logger.info(f"Fields to select: {fields}")
        
        # Replace hyphens with underscores in column names
        for col in df.columns:
            if "-" in col:
                new_col = col.replace("-", "_")
                df = df.withColumnRenamed(col, new_col)

        # Get actual DataFrame columns
        df_columns = df.columns

        # Find fields that are present in both DataFrame and json    
        if set(fields) == set(df.columns):
            logger.info("All fields are present in the DataFrame")
        else:
            logger.warning("Some fields are missing")
            
        if table_name == 'fact-res':
            return transform_collection_data.transform_data_fact_res(df)
        elif table_name == 'fact':
            return transform_collection_data.transform_data_fact(df)
        elif table_name == 'entity':
            return transform_collection_data.transform_data_entity(df)
        elif table_name == 'issues':
            return transform_collection_data.transform_data_issues(df)
        else:
            raise ValueError(f"Unknown table name: {table_name}")

    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def populate_tables(df, table_name): 
    try:
        transformed_df = transform_data(df, table_name)
        logger.info(f"Successfully transformed data for table: {table_name}")
        return transformed_df
    except Exception as e:
        logger.error(f"Error populating tables: {e}")
        raise

# -------------------- S3 Writer --------------------
def write_to_s3(df, output_path):
    try:   
        logger.info(f"Writing data to S3 at {output_path}") 
    # Convert entry-date to date type and extract year, month, day
        
        #df = df.withColumn("start_date_parsed", to_date(coalesce("start_date", "entry_date"), "yyyy-MM-dd")) \
        # entry date is when added to the platform and start date is when it became legally applicable.
        #Confired with client that for partitioning we will use the available entry date when another date is not available. 873
        df = df.withColumn("entry_date", to_date("entry_date", "yyyy-MM-dd")) \
        .withColumn("year", year("entry_date_parsed")) \
        .withColumn("month", month("entry_date_parsed")) \
        .withColumn("day", dayofmonth("entry_date_parsed"))
        df.drop("entry_date_parsed")
    # -------------------- PostgreSQL Writer --------------------
        #Write to S3 partitioned by year, month, day
        df.write \
        .partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .option("header", "true") \
        .parquet(output_path)
    except Exception as e:
        logger.error(f"Failed to write to S3: {e}", exc_info=True)
        raise
    

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
def main():
    try:
        logger.info("Starting main ETL process")          
        start_time = datetime.now()
        logger.info(f"Spark session started at: {start_time}")
        
        # Define paths to JSON configuration files

        dataset_json_path = "src/config/datasets.json"        

        # base_path = os.path.dirname(__file__)
        # dataset_json_path = os.path.join(base_path, "../config/datasets.json")

        # dataset_json_path="s3://development-collection-data/emr-data-processing/src0/pyspark-jobs/src/config/datasets.json"
        # dataset_json_path="/home/MHCLG-Repo/pyspark-jobs/src/config/datasets.json"

        logger.info(f"Processing dataset: {dataset_json_path}")              
         # Load AWS configuration
        config_json_datasets = load_metadata(dataset_json_path)

        logger.info(f"Loaded configuration #872 : {config_json_datasets}")
        #logger.info(f"Loaded dataset configuration #872 : {config_dataset}")
 
        for dataset, path_info in config_json_datasets.items():
            if not path_info.get("enabled", False):
                logger.info(f"Skipping disabled dataset: {dataset}")
                continue
            logger.info(f"Processing dataset 111: {dataset}")
            logger.info(f"Processing dataset path_info 222: {path_info}")
            
            full_path = f"{path_info['path']}*.csv"

            logger.info(f"Processing dataset 555: {dataset}")
            logger.info(f"Dataset input path 666: {full_path}")

            spark = create_spark_session(config_json_datasets)
            #Below 2 lines code is for local testing
            #from src.utils.path_utils import resolve_desktop_path
            #csv_path = resolve_desktop_path("../MHCLG/src-data/*.csv")
            #This is for local testing   
          
            # Read CSV using the dynamic schema
            df = spark.read.option("header", "true").csv(full_path)
            df.cache()  # Cache the DataFrame for performance

            #df = read_data(spark,  config['S3_INPUT_PATH'])
            df.printSchema() 
            df.show()

            # Show schema and sample data    
            #write_to_postgres(processed_df, config)
            logger.info("Writing to output path")
            ##generate_sqlite(processed_df)
            output_path = f"s3://development-collection-data/emr-data-processing/assemble-parquet/{dataset}/"
            processed_df = transform_data(df,'fact-res')
            #processed_df=populate_tables(processed_df, 'fact-res')
            write_to_s3(processed_df, f"{output_path}output-parquet-fact-res")
            processed_df = transform_data(df,'fact')
            #processed_df=populate_tables(processed_df, 'fact')
            write_to_s3(processed_df, f"{output_path}output-parquet-fact")
            #processed_df = transform_data(df,'issues')
            #processed_df=populate_tables(processed_df, 'issues')
            #write_to_s3(processed_df, f"{output_path}output-parquet-issues")
            #processed_df = transform_data(df,'entity')
            #processed_df=populate_tables(processed_df, 'entity')
            #write_to_s3(processed_df, f"{output_path}output-parquet-entity")

    except Exception as e:
        logger.exception("An error occurred during the ETL process: %s", str(e))
    finally:
        try:
            spark.stop()            
            end_time = datetime.now()
            logger.info(f"Spark session ended at: {end_time}")
            # Duration
            duration = end_time - start_time
            logger.info(f"Total duration: {duration}")

        except Exception as e:
            logger.info("Spark session stopped.")
        except Exception as stop_err:
            logger.exception("An error occurred while stopping Spark: %s", str(e))

if __name__ == "__main__":
    main()
