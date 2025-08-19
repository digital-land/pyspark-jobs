# -------------------- Postgres table creation --------------------

##writing to postgres db
import json
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)
import pg8000
from pg8000.exceptions import DatabaseError

from jobs.utils.aws_secrets_manager import get_secret_emr_compatible
import os
import time
from pathlib import Path

# Define your table schema
# https://github.com/digital-land/digital-land.info/blob/main/application/db/models.py - refered from here
#TODO: rename to actual name after testing and client approvals , as it replce the existing postgres entity data
dbtable_name = "pyspark_entity"
pyspark_entity_columns = {   
    "dataset": "TEXT",
    "end_date": "DATE",
    "entity": "TEXT",
    "entry_date": "DATE",
    "geojson": "JSONB",
    "geometry": "GEOMETRY(MULTIPOLYGON, 4326)",
    "json": "JSONB",
    "name": "TEXT",
    "organisation_entity": "BIGINT",
    "point": "GEOMETRY(POINT, 4326)",
    "prefix": "TEXT",
    "reference": "TEXT",
    "start_date": "DATE", 
    "typology": "TEXT",
}

# PostgreSQL connection parameters
#conn_params = get_secret()  # Assuming this function retrieves the secret as a dictionary
# read host, port,dbname,user, password


def get_aws_secret():
    """
    Retrieve AWS secrets for PostgreSQL connection with EMR Serverless compatibility.
    
    This function uses the EMR-compatible secret retrieval method that includes
    multiple fallback strategies to handle botocore issues in EMR environments.
    
    Returns:
        dict: PostgreSQL connection parameters
        
    Raises:
        Exception: If secrets cannot be retrieved or parsed
    """
    try:
        logger.info("Attempting to retrieve PostgreSQL secrets using EMR-compatible method")
        aws_secrets_json = get_secret_emr_compatible("dev/pyspark/postgres")
        
        # Parse the JSON string
        secrets = json.loads(aws_secrets_json)

        # Extract required fields
        username = secrets.get("username")
        password = secrets.get("password")
        dbName = secrets.get("dbName")
        host = secrets.get("host")
        port = secrets.get("port")
        
        # Validate required fields
        required_fields = ["username", "password", "dbName", "host", "port"]
        missing_fields = [field for field in required_fields if not secrets.get(field)]
        
        if missing_fields:
            error_msg = f"Missing required secret fields: {missing_fields}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"get_aws_secret: Retrieved secrets for {dbName} at {host}:{port} with user {username}")
        
        conn_params = {
            "database": dbName,  # pg8000 uses 'database' not 'dbname'
            "host": host,
            "port": int(port),  # Ensure port is integer
            "user": username,
            "password": password,
            "timeout": 30  # Connection timeout in seconds
        }
        
        # Don't log the actual connection params as they contain sensitive information
        logger.info("get_aws_secret: Successfully prepared connection parameters")
        return conn_params
        
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse secrets JSON: {e}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    except Exception as e:
        error_msg = f"Failed to retrieve AWS secrets: {e}"
        logger.error(error_msg)
        raise


# Create table if not exists
def create_table(conn_params, max_retries=3, retry_delay=5):
    """
    Create table with retry logic and better error handling.
    
    Args:
        conn_params (dict): Database connection parameters
        max_retries (int): Maximum number of connection attempts
        retry_delay (int): Delay between retries in seconds
    """
    import time
    from pg8000.exceptions import InterfaceError, DatabaseError
    
    conn = None
    cur = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"create_table: Attempting database connection (attempt {attempt + 1}/{max_retries})")
            logger.info(f"create_table: Connecting to {conn_params['host']}:{conn_params['port']}")
            
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            
            # Build CREATE TABLE SQL dynamically
            column_defs = ", ".join([f"{col} {dtype}" for col, dtype in pyspark_entity_columns.items()])
            create_query = f"CREATE TABLE IF NOT EXISTS {dbtable_name} ({column_defs});"
            logger.info(f"create_table: Creating table {dbtable_name} with columns: {list(pyspark_entity_columns.keys())}")

            cur.execute(create_query)
            conn.commit()
            logger.info(f"create_table: Table '{dbtable_name}' created successfully (if it didn't exist).")
            return  # Success, exit function
            
        except InterfaceError as e:
            logger.error(f"create_table: Network/Interface error (attempt {attempt + 1}/{max_retries}): {e}")
            if "Can't create a connection" in str(e) or "timeout" in str(e).lower():
                logger.error("create_table: This appears to be a network connectivity issue.")
                logger.error("create_table: Possible causes:")
                logger.error("create_table: 1. EMR Serverless not configured in correct VPC")
                logger.error("create_table: 2. Security group rules blocking connection")
                logger.error("create_table: 3. RDS database not accessible from EMR subnet")
            
            if attempt < max_retries - 1:
                logger.info(f"create_table: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("create_table: All connection attempts failed")
                raise
                
        except DatabaseError as e:
            logger.error(f"create_table: Database error: {e}")
            raise  # Don't retry database errors
            
        except Exception as e:
            logger.error(f"create_table: Unexpected error (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
            if attempt < max_retries - 1:
                logger.info(f"create_table: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
        finally:
            if cur:
                try:
                    cur.close()
                except Exception as e:
                    logger.warning(f"create_table: Error closing cursor: {e}")
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"create_table: Error closing connection: {e}")
            # Reset for next iteration
            conn = None
            cur = None

# -------------------- Geometry Helper Functions --------------------

def _prepare_geometry_columns(df):
    """
    Prepare geometry columns for PostgreSQL insertion with optimized WKT handling.
    Convert WKT strings to format that PostgreSQL can handle via JDBC.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame with potential geometry columns
        
    Returns:
        pyspark.sql.DataFrame: DataFrame with properly formatted geometry columns
    """
    from pyspark.sql.functions import when, col, expr
    
    logger.info("_prepare_geometry_columns: Processing DataFrame for optimized PostgreSQL geometry compatibility")
    
    # List of geometry columns that need special handling
    geometry_columns = ["geometry", "point"]
    
    processed_df = df
    for geom_col in geometry_columns:
        if geom_col in df.columns:
            logger.info(f"_prepare_geometry_columns: Processing geometry column: {geom_col}")
            
            # Use PostgreSQL ST_GeomFromText function for proper WKT conversion
            # This preserves geometry data instead of converting to NULL
            processed_df = processed_df.withColumn(
                geom_col,
                when(col(geom_col).isNull(), None)
                .when(col(geom_col) == "", None)
                .when(col(geom_col).startswith("POINT"), 
                      expr(f"concat('SRID=4326;', {geom_col})"))  # Add SRID for PostGIS
                .when(col(geom_col).startswith("POLYGON"), 
                      expr(f"concat('SRID=4326;', {geom_col})"))
                .when(col(geom_col).startswith("MULTIPOLYGON"), 
                      expr(f"concat('SRID=4326;', {geom_col})"))
                .otherwise(col(geom_col))
            )
            logger.info(f"_prepare_geometry_columns: Enhanced {geom_col} WKT handling with SRID for PostGIS compatibility")
    
    return processed_df

# -------------------- PostgreSQL Writer --------------------

##writing to postgres db
def write_to_postgres(df, conn_params, method="optimized", batch_size=None, num_partitions=None, **kwargs):
    """
    Insert DataFrame rows into PostgreSQL table using optimized PySpark JDBC writer.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame to insert
        conn_params (dict): PostgreSQL connection parameters
        method (str): Write method - "optimized" (default), "standard", "async"
        batch_size (int): Number of rows per batch (auto-calculated if None)
        num_partitions (int): Number of partitions (auto-calculated if None)
        **kwargs: Additional parameters for specific methods
    """
    logger.info(f"write_to_postgres: Using {method} method for PostgreSQL writes")
    
    if method == "async":
        return _write_to_postgres_async_batches(df, conn_params, batch_size or 5000, **kwargs)
    elif method == "standard":
        return _write_to_postgres_standard(df, conn_params)
    else:  # method == "optimized" (default) - handles any invalid method as optimized
        return _write_to_postgres_optimized(df, conn_params, batch_size, num_partitions)


def _write_to_postgres_standard(df, conn_params):
    """
    Original PostgreSQL writer (for comparison and fallback).
    """
    logger.info("_write_to_postgres_standard: Using original JDBC writer")
    
    # Ensure table exists before inserting
    create_table(conn_params)

    url = f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    properties = {
        "user": conn_params["user"],
        "password": conn_params["password"],
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"  # This helps with PostGIS geometry columns
    }

    try:
        # Convert DataFrame to handle geometry columns for PostgreSQL
        processed_df = _prepare_geometry_columns(df)
        
        processed_df.write \
            .jdbc(url=url, table=dbtable_name, mode="append", properties=properties)
        logger.info(f"_write_to_postgres_standard: Inserted {processed_df.count()} rows into {dbtable_name} using standard JDBC")
    except Exception as e:
        logger.error(f"_write_to_postgres_standard: Failed to write to PostgreSQL via JDBC: {e}", exc_info=True)
        raise


def _write_to_postgres_optimized(df, conn_params, batch_size=None, num_partitions=None):
    """
    Optimized PostgreSQL writer with performance improvements.
    
    Args:
        df: PySpark DataFrame to write
        conn_params: Database connection parameters
        batch_size: Number of rows per batch (auto-calculated if None)
        num_partitions: Number of partitions (auto-calculated if None)
    """
    logger.info("_write_to_postgres_optimized: Starting optimized PostgreSQL write operation")
    
    # Ensure table exists
    create_table(conn_params)
    
    # Auto-calculate optimal batch size if not specified
    if batch_size is None:
        row_count = df.count()
        if row_count < 10000:
            batch_size = 1000
        elif row_count < 100000:
            batch_size = 5000
        elif row_count < 1000000:
            batch_size = 10000
        else:
            batch_size = 20000
        logger.info(f"_write_to_postgres_optimized: Auto-calculated batch size: {batch_size} for {row_count} rows")
    
    # Auto-calculate optimal number of partitions if not specified
    if num_partitions is None:
        row_count = df.count()
        # Use one partition per 50k rows, minimum 1, maximum 20
        num_partitions = max(1, min(20, row_count // 50000))
        logger.info(f"_write_to_postgres_optimized: Auto-calculated partitions: {num_partitions} for {row_count} rows")
    
    # Repartition DataFrame for optimal parallel writing
    if df.rdd.getNumPartitions() != num_partitions:
        logger.info(f"_write_to_postgres_optimized: Repartitioning DataFrame from {df.rdd.getNumPartitions()} to {num_partitions} partitions")
        df = df.repartition(num_partitions)
    
    # Process geometry columns
    processed_df = _prepare_geometry_columns(df)
    
    # Build optimized JDBC URL and properties
    url = f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    
    # Optimized JDBC properties for better performance
    properties = {
        "user": conn_params["user"],
        "password": conn_params["password"],
        "driver": "org.postgresql.Driver",
        
        # Performance optimizations
        "batchsize": str(batch_size),                    # Batch size for inserts
        "reWriteBatchedInserts": "true",                 # Rewrite batched inserts for performance
        "stringtype": "unspecified",                     # For PostGIS compatibility
        
        # Connection optimizations
        "tcpKeepAlive": "true",                          # Keep connections alive
        "socketTimeout": "300",                          # 5 minute socket timeout
        "loginTimeout": "30",                            # 30 second login timeout
        "connectTimeout": "30",                          # 30 second connect timeout
        
        # Memory and caching optimizations
        "defaultRowFetchSize": "1000",                   # Fetch size for reads
        "prepareThreshold": "3",                         # Prepare statements after 3 uses
        "preparedStatementCacheQueries": "256",          # Cache prepared statements
        "preparedStatementCacheSizeMiB": "5",            # Cache size
        
        # Additional optimizations
        "ApplicationName": "PySpark-EMR-Optimized",      # For monitoring
        "assumeMinServerVersion": "12.0",                # Assume modern PostgreSQL
    }
    
    try:
        logger.info(f"_write_to_postgres_optimized: Writing {processed_df.count()} rows to PostgreSQL with batch size {batch_size}")
        logger.info(f"_write_to_postgres_optimized: Using {num_partitions} partitions for parallel writes")
        
        # Use optimized JDBC write with error handling
        processed_df.write \
            .mode("append") \
            .option("numPartitions", num_partitions) \
            .jdbc(url=url, table=dbtable_name, properties=properties)
        
        logger.info(f"_write_to_postgres_optimized: Successfully wrote data to {dbtable_name} using optimized JDBC writer")
        
    except Exception as e:
        logger.error(f"_write_to_postgres_optimized: Failed to write to PostgreSQL: {e}")
        logger.error("_write_to_postgres_optimized: Troubleshooting steps:")
        logger.error("_write_to_postgres_optimized: 1. Check if PostgreSQL JDBC driver is available")
        logger.error("_write_to_postgres_optimized: 2. Verify network connectivity to database")
        logger.error("_write_to_postgres_optimized: 3. Check database permissions for the user")
        logger.error("_write_to_postgres_optimized: 4. Verify table schema matches DataFrame columns")
        raise


def _write_to_postgres_copy(df, conn_params, use_copy_protocol=True, temp_s3_bucket=None, aurora_mode=None):
    """
    Ultra-fast PostgreSQL writer using COPY protocol with automatic temporary S3 staging.
    
    Supports both standard PostgreSQL COPY and AWS Aurora S3 import functions.
    
    This method can be 5-10x faster than JDBC for large datasets and automatically:
    - Uses the project's S3 bucket for temporary staging
    - Creates a unique temporary path with timestamp
    - Detects Aurora vs standard PostgreSQL
    - Uses appropriate import method (COPY vs aurora_s3_import)
    - Cleans up temporary files after successful import
    
    Args:
        df: PySpark DataFrame to write
        conn_params: Database connection parameters  
        use_copy_protocol: Whether to use COPY protocol (default: True)
        temp_s3_bucket: S3 bucket for temporary CSV files (defaults to project bucket)
        aurora_mode: Force Aurora mode (auto-detected if None)
    """
    if not use_copy_protocol or not pg8000:
        logger.info("_write_to_postgres_copy: COPY protocol disabled or pg8000 unavailable, falling back to optimized JDBC")
        return _write_to_postgres_optimized(df, conn_params)
    
    logger.info("_write_to_postgres_copy: Using ultra-fast COPY protocol for PostgreSQL writes")
    
    # Determine S3 bucket for temporary staging (priority order):
    # 1. Argument passed from main (from Airflow or default)
    # 2. Environment variable PYSPARK_TEMP_S3_BUCKET (override if needed)
    # 3. Final fallback (development-pyspark-jobs-codepackage)
    
    if temp_s3_bucket is None:
        # Should rarely happen now that argument has default, but maintain compatibility
        temp_s3_bucket = os.environ.get('PYSPARK_TEMP_S3_BUCKET', 'development-pyspark-jobs-codepackage')
        logger.info(f"_write_to_postgres_copy: Using fallback S3 bucket: {temp_s3_bucket}")
    else:
        # Check if environment variable override is set
        env_override = os.environ.get('PYSPARK_TEMP_S3_BUCKET')
        if env_override and env_override != temp_s3_bucket:
            logger.info(f"_write_to_postgres_copy: Environment variable override detected: {env_override}")
            temp_s3_bucket = env_override
        else:
            logger.info(f"_write_to_postgres_copy: Using S3 bucket from arguments: {temp_s3_bucket}")
    
    # Create unique temporary path with timestamp
    import uuid
    temp_session_id = str(uuid.uuid4())[:8]
    temp_timestamp = int(time.time())
    temp_s3_path = f"s3a://{temp_s3_bucket}/tmp/postgres-copy/{dbtable_name}-{temp_timestamp}-{temp_session_id}/"
    
    try:
        # Ensure table exists
        create_table(conn_params)
        
        # Validate S3 bucket exists before proceeding
        if not _validate_s3_bucket(temp_s3_bucket):
            logger.warning(f"_write_to_postgres_copy: S3 bucket {temp_s3_bucket} not accessible, falling back to optimized JDBC")
            return _write_to_postgres_optimized(df, conn_params)
        
        logger.info(f"_write_to_postgres_copy: Writing temporary CSV to {temp_s3_path}")
        processed_df = _prepare_geometry_columns(df)
        
        # Write as single CSV file for COPY protocol
        processed_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "false") \
            .option("delimiter", "|") \
            .csv(temp_s3_path)
        
        # Step 2: Detect Aurora and use appropriate import method
        if aurora_mode is None:
            # Auto-detect Aurora based on hostname
            hostname = conn_params.get('host', '')
            aurora_mode = '.aurora.' in hostname or '.cluster-' in hostname
            
        if aurora_mode:
            logger.info("_write_to_postgres_copy: Detected AWS Aurora PostgreSQL, using aurora_s3_import")
            _execute_aurora_s3_import(conn_params, temp_s3_path)
        else:
            logger.info("_write_to_postgres_copy: Using standard PostgreSQL COPY command")
            _execute_copy_command(conn_params, temp_s3_path)
        
        logger.info("_write_to_postgres_copy: COPY protocol completed successfully")
        
        # Step 3: Clean up temporary files
        try:
            _cleanup_temp_s3_files(temp_s3_path)
            logger.info("_write_to_postgres_copy: Temporary files cleaned up successfully")
        except Exception as cleanup_error:
            logger.warning(f"_write_to_postgres_copy: Failed to clean up temporary files (non-critical): {cleanup_error}")
            
    except Exception as e:
        logger.error(f"_write_to_postgres_copy: COPY protocol failed: {e}")
        if "NoSuchBucket" in str(e) or "does not exist" in str(e):
            logger.error("_write_to_postgres_copy: S3 bucket configuration issue detected")
            logger.error(f"_write_to_postgres_copy: Using bucket: {temp_s3_bucket}")
            logger.error("_write_to_postgres_copy: To use COPY protocol, ensure:")
            logger.error("_write_to_postgres_copy: 1. S3 bucket exists and is accessible")
            logger.error("_write_to_postgres_copy: 2. PostgreSQL has aws_s3 extension installed")
            logger.error("_write_to_postgres_copy: 3. PostgreSQL has network access to S3")
        
        # Attempt cleanup even on failure
        try:
            _cleanup_temp_s3_files(temp_s3_path)
            logger.info("_write_to_postgres_copy: Temporary files cleaned up after failure")
        except Exception:
            pass  # Ignore cleanup errors if main operation failed
        
        logger.info("_write_to_postgres_copy: Falling back to optimized JDBC writer")
        return _write_to_postgres_optimized(df, conn_params)


def _cleanup_temp_s3_files(temp_s3_path):
    """
    Clean up temporary S3 files created during COPY protocol operation.
    
    Args:
        temp_s3_path: S3 path to temporary files that need to be deleted
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        # Parse S3 path
        if temp_s3_path.startswith('s3a://'):
            s3_path = temp_s3_path[6:]  # Remove s3a:// prefix
        elif temp_s3_path.startswith('s3://'):
            s3_path = temp_s3_path[5:]  # Remove s3:// prefix
        else:
            s3_path = temp_s3_path
        
        bucket, prefix = s3_path.split('/', 1)
        
        s3_client = boto3.client('s3')
        
        # List all objects with the temporary prefix
        logger.info(f"_cleanup_temp_s3_files: Cleaning up temporary files in s3://{bucket}/{prefix}")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        objects_deleted = 0
        for page in pages:
            if 'Contents' in page:
                # Prepare list of objects to delete
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                
                if objects_to_delete:
                    # Delete objects in batch
                    response = s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    
                    # Count successful deletions
                    if 'Deleted' in response:
                        objects_deleted += len(response['Deleted'])
                    
                    # Log any deletion errors
                    if 'Errors' in response:
                        for error in response['Errors']:
                            logger.warning(f"_cleanup_temp_s3_files: Failed to delete {error['Key']}: {error['Message']}")
        
        logger.info(f"_cleanup_temp_s3_files: Successfully deleted {objects_deleted} temporary files from S3")
        
    except ImportError:
        logger.warning("_cleanup_temp_s3_files: boto3 not available, cannot clean up temporary S3 files")
    except Exception as e:
        # Handle both ClientError and other exceptions
        if hasattr(e, 'response') and 'Error' in getattr(e, 'response', {}):
            logger.warning(f"_cleanup_temp_s3_files: S3 client error during cleanup: {e}")
        else:
            logger.warning(f"_cleanup_temp_s3_files: Unexpected error during S3 cleanup: {e}")


def _validate_s3_bucket(bucket_name):
    """
    Validate that an S3 bucket exists and is accessible.
    
    Args:
        bucket_name: Name of the S3 bucket to validate
        
    Returns:
        bool: True if bucket is accessible, False otherwise
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        s3_client = boto3.client('s3')
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"_validate_s3_bucket: S3 bucket {bucket_name} is accessible")
        return True
        
    except ImportError:
        logger.warning("_validate_s3_bucket: boto3 not available, skipping S3 bucket validation")
        return True  # Assume it exists if we can't validate
    except Exception as e:
        # Handle both ClientError and other boto3 exceptions
        if hasattr(e, 'response') and 'Error' in e.response:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"_validate_s3_bucket: S3 bucket {bucket_name} does not exist")
            elif error_code == '403':
                logger.error(f"_validate_s3_bucket: Access denied to S3 bucket {bucket_name}")
            else:
                logger.error(f"_validate_s3_bucket: S3 bucket validation failed: {e}")
        else:
            logger.warning(f"_validate_s3_bucket: S3 bucket validation failed: {e}")
        return False


def _execute_copy_command(conn_params, csv_s3_path):
    """
    Execute PostgreSQL COPY command for ultra-fast bulk import.
    """
    if not pg8000:
        raise ImportError("pg8000 required for COPY protocol")
    
    conn = None
    cur = None
    
    try:
        conn = pg8000.connect(**conn_params)
        cur = conn.cursor()
        
        # Build column list for COPY command
        columns = ", ".join(pyspark_entity_columns.keys())
        
        # Note: This requires PostgreSQL aws_s3 extension or similar setup
        copy_sql = f"""
        COPY {dbtable_name} ({columns})
        FROM PROGRAM 'aws s3 cp {csv_s3_path} -'
        WITH (FORMAT csv, DELIMITER '|')
        """
        
        logger.info("_execute_copy_command: Executing COPY command...")
        cur.execute(copy_sql)
        conn.commit()
        
        logger.info("_execute_copy_command: COPY command completed successfully")
        
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _execute_aurora_s3_import(conn_params, csv_s3_path):
    """
    Execute Aurora PostgreSQL S3 import for ultra-fast bulk import.
    
    Uses Aurora's native aurora_s3_import() function instead of COPY FROM PROGRAM.
    """
    if not pg8000:
        raise ImportError("pg8000 required for Aurora S3 import")
    
    conn = None
    cur = None
    
    try:
        conn = pg8000.connect(**conn_params)
        cur = conn.cursor()
        
        # Extract S3 bucket and path from full S3 path
        # csv_s3_path format: s3a://bucket/path/to/files/
        s3_path_clean = csv_s3_path.replace('s3a://', '').rstrip('/')
        bucket_name = s3_path_clean.split('/')[0]
        s3_key_prefix = '/'.join(s3_path_clean.split('/')[1:])
        
        # Find the actual CSV file (Spark creates part-*.csv files)
        import boto3
        s3_client = boto3.client('s3')
        objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_key_prefix)
        
        csv_files = [obj['Key'] for obj in objects.get('Contents', []) 
                    if obj['Key'].endswith('.csv') and 'part-' in obj['Key']]
        
        if not csv_files:
            raise ValueError(f"No CSV files found in {csv_s3_path}")
        
        # Use the first (and typically only) CSV file
        csv_file_key = csv_files[0]
        s3_uri = f's3://{bucket_name}/{csv_file_key}'
        
        logger.info(f"_execute_aurora_s3_import: Importing from {s3_uri}")
        
        # Build column list for import
        columns = ", ".join(pyspark_entity_columns.keys())
        
        # Aurora S3 import function
        # Note: This requires Aurora to have S3 access via IAM role
        import_sql = f"""
        SELECT aurora_s3_import(
            '{dbtable_name}',
            '({columns})',
            '{s3_uri}',
            'FORMAT csv, DELIMITER ''|'', HEADER false'
        );
        """
        
        logger.info("_execute_aurora_s3_import: Executing Aurora S3 import...")
        cur.execute(import_sql)
        result = cur.fetchone()
        conn.commit()
        
        logger.info(f"_execute_aurora_s3_import: Aurora S3 import completed: {result}")
        
    except Exception as e:
        logger.error(f"_execute_aurora_s3_import: Aurora S3 import failed: {e}")
        # For Aurora, if S3 import fails, fall back to standard COPY attempt
        logger.info("_execute_aurora_s3_import: Attempting fallback to standard COPY...")
        _execute_copy_command(conn_params, csv_s3_path)
        
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _write_to_postgres_async_batches(df, conn_params, batch_size=5000, max_workers=4):
    """
    Async batch writer for maximum throughput on datasets that fit in memory.
    
    This method processes data in parallel batches for maximum performance.
    Best for datasets that fit in driver memory (typically < 100k rows).
    """
    if not pg8000:
        logger.warning("_write_to_postgres_async_batches: pg8000 not available - falling back to optimized JDBC")
        return _write_to_postgres_optimized(df, conn_params, batch_size)
    
    logger.info(f"_write_to_postgres_async_batches: Starting async batch write with {max_workers} workers, batch size {batch_size}")
    
    # Ensure table exists
    create_table(conn_params)
    
    # Collect DataFrame to Python for batch processing
    # Note: Only use this for reasonably sized datasets that fit in memory
    try:
        rows = df.collect()
        total_rows = len(rows)
        
        logger.info(f"_write_to_postgres_async_batches: Processing {total_rows} rows in batches of {batch_size}")
        
        if total_rows > 100000:
            logger.warning(f"_write_to_postgres_async_batches: Large dataset ({total_rows} rows) may cause memory issues")
            logger.info("_write_to_postgres_async_batches: Consider using 'optimized' or 'copy' method instead")
        
    except Exception as e:
        logger.error(f"_write_to_postgres_async_batches: Failed to collect DataFrame: {e}")
        logger.info("_write_to_postgres_async_batches: Falling back to optimized JDBC writer")
        return _write_to_postgres_optimized(df, conn_params, batch_size)
    
    import concurrent.futures
    
    def write_batch(batch_rows, batch_num):
        """Write a single batch of rows."""
        try:
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            
            # Build parameterized INSERT statement
            columns = list(pyspark_entity_columns.keys())
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {dbtable_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Prepare batch data
            batch_data = []
            for row in batch_rows:
                row_data = [getattr(row, col, None) for col in columns]
                batch_data.append(row_data)
            
            # Execute batch insert
            cur.executemany(insert_sql, batch_data)
            conn.commit()
            
            logger.info(f"_write_to_postgres_async_batches: Batch {batch_num} completed: {len(batch_rows)} rows")
            
        except Exception as e:
            logger.error(f"_write_to_postgres_async_batches: Batch {batch_num} failed: {e}")
            raise
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    
    # Process in parallel batches
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        for i in range(0, total_rows, batch_size):
            batch_rows = rows[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            future = executor.submit(write_batch, batch_rows, batch_num)
            futures.append(future)
        
        # Wait for all batches to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"_write_to_postgres_async_batches: Batch processing failed: {e}")
                raise
    
    logger.info(f"_write_to_postgres_async_batches: Async batch write completed: {total_rows} rows processed")


# -------------------- Performance Optimization Helpers --------------------

def get_performance_recommendations(row_count, available_memory_gb=8):
    """
    Get performance recommendations based on dataset size and available resources.
    
    Args:
        row_count: Number of rows to write
        available_memory_gb: Available memory in GB
    
    Returns:
        dict: Recommendations for optimal PostgreSQL writing
    """
    recommendations = {
        "method": "optimized",
        "batch_size": 10000,
        "num_partitions": 4,
        "notes": []
    }
    
    if row_count < 10000:
        recommendations.update({
            "method": "optimized",
            "batch_size": 1000,
            "num_partitions": 1,
            "notes": ["Small dataset - single partition recommended"]
        })
    
    elif row_count < 100000:
        recommendations.update({
            "method": "optimized", 
            "batch_size": 5000,
            "num_partitions": 2,
            "notes": ["Medium dataset - moderate parallelization"]
        })
    
    elif row_count < 1000000:
        recommendations.update({
            "method": "optimized",
            "batch_size": 10000,
            "num_partitions": 4,
            "notes": ["Large dataset - increased parallelization recommended"]
        })
    
    else:  # Very large datasets
        max_partitions = min(20, max(4, available_memory_gb // 2))
        recommendations.update({
            "method": "copy",  # Use COPY protocol for maximum speed with auto S3 staging
            "batch_size": 20000,
            "num_partitions": max_partitions,
            "notes": [
                "Very large dataset - COPY protocol recommended for maximum speed",
                "Automatic S3 staging using project bucket (development-pyspark-jobs-codepackage)",
                "Falls back to optimized JDBC if COPY protocol fails",
                f"Using {max_partitions} partitions based on available memory"
            ]
        })
    
    return recommendations


def get_copy_protocol_recommendation(s3_bucket=None):
    """
    Get recommendation for using COPY protocol with automatic S3 configuration.
    
    Args:
        s3_bucket: S3 bucket name for temporary CSV staging (defaults to project bucket)
        
    Returns:
        dict: Configuration for COPY protocol usage
    """
    if s3_bucket is None:
        s3_bucket = os.environ.get('PYSPARK_TEMP_S3_BUCKET', 'development-pyspark-jobs-codepackage')
    
    if _validate_s3_bucket(s3_bucket):
        return {
            "method": "copy",
            "temp_s3_bucket": s3_bucket,
            "reason": f"COPY protocol ready with S3 bucket: {s3_bucket}",
            "expected_performance": "5-10x faster than JDBC for large datasets",
            "automatic_features": [
                "Uses project's development S3 bucket by default",
                "Creates unique temporary paths with timestamp and UUID",
                "Automatically cleans up temporary files after completion",
                "Falls back to optimized JDBC if any issues occur"
            ]
        }
    else:
        return {
            "method": "optimized", 
            "reason": f"S3 bucket {s3_bucket} not accessible",
            "fallback_info": "Will use optimized JDBC instead (still 3-5x faster than standard)",
            "troubleshooting": [
                "Check if S3 bucket exists (should be development-pyspark-jobs-codepackage)",
                "Verify AWS credentials and permissions",
                "Ensure bucket is in the correct region",
                "PostgreSQL must have aws_s3 extension for COPY protocol"
            ]
        }


# -------------------- SQLite Output Functions --------------------
# NOTE: SQLite functionality has been moved to dedicated script:
# src/jobs/parquet_to_sqlite.py
#
# This provides better separation of concerns and allows SQLite conversion
# to run independently from the main data processing pipeline.
#
# Usage:
#   python src/jobs/parquet_to_sqlite.py --input s3://bucket/parquet/ --output ./data.sqlite


