import configparser
import logging
import os
import boto3
#import sqlite3
import sys
from dataclasses import fields
from logging import config
from logging.config import dictConfig
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (coalesce,collect_list,concat_ws,dayofmonth,expr,first,month,to_date,year)
from pyspark.sql.types import (StringType,StructField,StructType,TimestampType)
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
def load_metadata(s3_uri):
    """
    Load a JSON configuration file from an S3 URI.
    Example URI: S3://development-collection-data/emr-data-processing/src0/pyspark-jobs/config/configuration.json
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")
    try:
        s3 = boto3.client('s3')
        parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]
    
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.load(response['Body'])
  
    except Exception as e:
        logger.exception(f"Unexpected error while loading metadata from {s3_uri}")
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
def transform_data(df):      
    
    #json_data = load_json_from_repo("~/pyspark-jobs/config/transformed-source.json")
    #TODO: Update this to be dynamic and remove hardcoded path
    json_data = read_json_file("s3://development-collection-data/emr-data-processing/src0/pyspark-jobs/config/transformed-source.json")
    logger.info(f"Transforming data with schema: {json_data}")

    # Extract the list of fields
    fields = json_data.get("transport-access-node", [])
    logger.info(f"Fields to select: {fields}")
    
    # Replace hyphens with underscores in column names
    for col in df.columns:
        if "-" in col:
            df = df.withColumnRenamed(col, col.replace("-", "_"))

    # Get actual DataFrame columns
    df_columns = df.columns

    # Find fields that are present in both DataFrame and json    
    if set(fields) == set(df.columns):
        logger.info("All fields are present in the DataFrame")
    else:
        logger.warning("Some fields are missing from the DataFrame")
    
    return df

def populate_tables(df, table_name):   
    from utils.path_utils import load_json_from_repo
    json_data = load_json_from_repo("~/pyspark-jobs/config/transformed-target.json")

    # Extract the list of fields
    fields = json_data.get(table_name, [])
    
    # Select only those columns from the DataFrame that are for fact_resource table
    df_selected = df[fields]
    return df_selected


# -------------------- S3 Writer --------------------
def write_to_s3(df, output_path):   
    logger.info(f"Writing data to S3 at {output_path}") 
# Convert entry-date to date type and extract year, month, day
    #df.show()
    #spark.stop()clear
    df = df.withColumn("start_date_parsed", to_date(coalesce("start_date", "entry_date"), "yyyy-MM-dd")) \
    .withColumn("year", year("start_date_parsed")) \
    .withColumn("month", month("start_date_parsed")) \
    .withColumn("day", dayofmonth("start_date_parsed"))
    df.drop("start_date_parsed")
# -------------------- PostgreSQL Writer --------------------
    #Write to S3 partitioned by year, month, day
    df.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path)
    

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

# -------------------- PostgreSQL Writer --------------------

##writing to postgres db
def write_to_postgres(df, config):
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
# -------------------- Main --------------------
def main():
    try:
        logger.info("Starting main ETL process")        
        # Define paths to JSON configuration files
        dataset_json_path="s3://development-collection-data/emr-data-processing/src0/pyspark-jobs/config/datasets.json"
        dataset_json_path="/home/MHCLG-Repo/pyspark-jobs/config/datasets.json"

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

            #df = read_data(spark,  config['S3_INPUT_PATH'])
            df.printSchema() 
            df.show()
            processed_df = transform_data(df)
            # Show schema and sample data    
            #write_to_postgres(processed_df, config)
            logger.info("Writing to output path")
            ##generate_sqlite(processed_df)
            output_path = f"s3://development-collection-data/emr-data-processing/assemble-parquet/{dataset}/"
            populate_tables(processed_df, 'transport-access-node-fact-resource')
            write_to_s3(processed_df, f"{output_path}output-parquet-fact-res")
            populate_tables(processed_df, 'transport-access-node-fact')
            write_to_s3(processed_df, f"{output_path}output-parquet-fact")

    except Exception as e:
        logger.exception("An error occurred during the ETL process: %s", str(e))
    finally:
        try:
            spark.stop()
        except Exception as e:
            logger.info("Spark session stopped.")
        except Exception as stop_err:
            logger.exception("An error occurred while stopping Spark: %s", str(e))

if __name__ == "__main__":
    main()
