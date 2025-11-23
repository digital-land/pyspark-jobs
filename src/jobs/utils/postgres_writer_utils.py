from jobs.utils.logger_config import get_logger, log_execution_time
from jobs.dbaccess.postgres_connectivity import (
    get_aws_secret,
    write_to_postgres,
    create_and_prepare_staging_table,
    commit_staging_to_production,
    calculate_centroid_wkt,
    ENTITY_TABLE_NAME
)
from jobs.csv_s3_writer import write_dataframe_to_csv_s3, import_csv_to_aurora, cleanup_temp_csv_files

logger = get_logger(__name__)

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
                write_dataframe_to_postgres_jdbc(df, table_name, data_set, env)
            else:
                # Use Aurora S3 import method
                logger.info("Write_PG: Using Aurora S3 import method")
                
                # Determine S3 CSV output path (under the same parquet path)
                csv_output_path = f"s3://{env}-parquet-datasets/csv-temp/"
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
                        write_dataframe_to_postgres_jdbc(df, table_name, data_set, env)
                        
                except Exception as e:
                    logger.error(f"Write_PG: Aurora S3 import failed: {e}")
                    logger.info("Write_PG: Falling back to JDBC method")
                    
                    # Clean up temporary CSV files if they were created but import failed
                    if csv_path:
                        logger.info("Write_PG: Cleaning up temporary CSV files after S3 import failure")
                        cleanup_temp_csv_files(csv_path)
                    
                    write_dataframe_to_postgres_jdbc(df, table_name, data_set, env)
                        
        else:
            # For non-entity tables, use traditional JDBC method
            logger.info(f"Write_PG: Using JDBC method for non-entity table: {table_name}")
            write_dataframe_to_postgres_jdbc(df, table_name, data_set, env)
             
    except Exception as e:
        logger.error(f"Write_PG: Failed to write to Postgres: {e}", exc_info=True)
        raise


def write_dataframe_to_postgres_jdbc(df, table_name, data_set, env, use_staging=True):
    """
    JDBC writer with optional staging table support.
    
    This method can write directly to the entity table or use a staging table
    to minimize lock contention on the production entity table.
    
    Args:
        df: PySpark DataFrame to write
        table_name: Logical table identifier (e.g., 'entity', 'fact')
        data_set: Dataset name
        env: Environment name (development, staging, production)
        use_staging: If True, uses staging table pattern (recommended for high-load scenarios)
    """
    from jobs.dbaccess.postgres_connectivity import get_performance_recommendations
    
    # Get performance recommendations based on dataset size
    row_count = df.count()
    recommendations = get_performance_recommendations(row_count)
    logger.info(f"write_dataframe_to_postgres_jdbc: Performance recommendations for {row_count} rows: {recommendations}")
    
    conn_params = get_aws_secret(env)
    
    # Use staging pattern for entity table (logical name comparison is correct here)
    if use_staging and table_name == 'entity':
        logger.info("write_dataframe_to_postgres_jdbc: Using STAGING TABLE pattern for entity table")
        logger.info("write_dataframe_to_postgres_jdbc: This minimizes lock contention on production table")
        
        try:
            # Step 1: Create staging table
            logger.info("write_dataframe_to_postgres_jdbc: Step 1/3 - Creating staging table")
            staging_table_name = create_and_prepare_staging_table(
                conn_params=conn_params,
                dataset_value=data_set
            )
            logger.info(f"write_dataframe_to_postgres_jdbc: Created staging table: {staging_table_name}")
            
            # Step 2: Write data to staging table
            logger.info(f"write_dataframe_to_postgres_jdbc: Step 2/3 - Writing {row_count:,} rows to staging table")
            write_to_postgres(
                df, 
                data_set,
                conn_params,
                method=recommendations["method"],
                batch_size=recommendations["batch_size"],
                num_partitions=recommendations["num_partitions"],
                target_table=staging_table_name  # Write to staging table
            )
            logger.info(f"write_dataframe_to_postgres_jdbc: Successfully wrote data to staging table '{staging_table_name}'")

            # Step 2b: Calculate the centroid of the multiplygon data into the point field
            # logger.info(
            #     "_write_dataframe_to_postgres_jdbc: "
            #     f"Calculating centroids for multipolygon geometries in {staging_table_name}"
            # )
            # rows_updated = calculate_centroid_wkt(
            #     conn_params,
            #     target_table=staging_table_name  # Update centroids in staging table
            # )
            # logger.info(
            #     "_write_dataframe_to_postgres_jdbc: "
            #     f"Updated {rows_updated} centroids in staging table"
            # )
            
            # Step 3: Atomically commit staging data to production entity table
            logger.info("write_dataframe_to_postgres_jdbc: Step 3/3 - Committing staging data to production entity table")
            commit_result = commit_staging_to_production(
                conn_params=conn_params,
                staging_table_name=staging_table_name,
                dataset_value=data_set
            )
            
            if commit_result["success"]:
                logger.info(
                    f"write_dataframe_to_postgres_jdbc: ✓ STAGING COMMIT SUCCESSFUL - "
                    f"Deleted {commit_result['rows_deleted']:,} old rows, "
                    f"Inserted {commit_result['rows_inserted']:,} new rows in {commit_result['total_duration']:.2f}s"
                )
                logger.info(
                    f"write_dataframe_to_postgres_jdbc: Lock time on entity table: "
                    f"~{commit_result['total_duration']:.2f}s (vs. several minutes with direct write)"
                )
            else:
                logger.error(f"write_dataframe_to_postgres_jdbc: Staging commit failed: {commit_result.get('error')}")
                raise Exception(f"Staging commit failed: {commit_result.get('error')}")
            
        except Exception as e:
            logger.error(f"write_dataframe_to_postgres_jdbc: Staging table approach failed: {e}")
            logger.info("write_dataframe_to_postgres_jdbc: Falling back to direct write to entity table")
            
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
        logger.info(f"write_dataframe_to_postgres_jdbc: Using DIRECT WRITE to {table_name} table")
        write_to_postgres(
            df, 
            data_set,
            conn_params,
            method=recommendations["method"],
            batch_size=recommendations["batch_size"],
            num_partitions=recommendations["num_partitions"]
        )
    
    logger.info(f"write_dataframe_to_postgres_jdbc: JDBC writing completed using {recommendations['method']} method")