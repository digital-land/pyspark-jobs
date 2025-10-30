from jobs.utils.s3_format_utils import flatten_s3_json, s3_csv_format
from jobs.utils.s3_utils import cleanup_dataset_data
from jobs.utils.logger_config import setup_logging, get_logger, log_execution_time, set_spark_log_level
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType
from datetime import datetime

logger = get_logger(__name__)

df_entity = None
@log_execution_time

# -------------------- S3 Writer Format--------------------
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

