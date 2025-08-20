# -------------------- Postgres table creation --------------------

##writing to postgres db
import json
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)
# Optional import for direct database connections (table creation)
try:
    import pg8000
    from pg8000.exceptions import DatabaseError
except ImportError:
    pg8000 = None
    DatabaseError = Exception

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
def create_table(conn_params, dataset_value,max_retries=3, retry_delay=5):
    """
    Create table with retry logic and better error handling.
    
    Args:
        conn_params (dict): Database connection parameters
        max_retries (int): Maximum number of connection attempts
        retry_delay (int): Delay between retries in seconds
    """
    import time
    if pg8000:
        from pg8000.exceptions import InterfaceError, DatabaseError
    else:
        logger.warning("create_table: pg8000 not available, table creation may not work properly")
        InterfaceError = DatabaseError = Exception
    
    conn = None
    cur = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"create_table: Attempting database connection (attempt {attempt + 1}/{max_retries})")
            logger.info(f"create_table: Connecting to {conn_params['host']}:{conn_params['port']}")
            
            if not pg8000:
                raise ImportError("pg8000 required for direct database connections")
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            
            # Build CREATE TABLE SQL dynamically
            column_defs = ", ".join([f"{col} {dtype}" for col, dtype in pyspark_entity_columns.items()])
            create_query = f"CREATE TABLE IF NOT EXISTS {dbtable_name} ({column_defs});"
            logger.info(f"create_table: Creating table {dbtable_name} with columns: {list(pyspark_entity_columns.keys())}")

            cur.execute(create_query)
            conn.commit()
            logger.info(f"create_table: Table '{dbtable_name}' created successfully (if it didn't exist).")

            # Select and delete records by dataset value if provided
            if dataset_value:
                delete_query = f"DELETE FROM {dbtable_name} WHERE dataset = %s;"
                logger.info(f"create_table: Selecting records to delete from {dbtable_name} where dataset = {dataset_value}")
                cur.execute(delete_query, (dataset_value,))
                conn.commit()
                logger.info(f"create_table: Deleted records from {dbtable_name} where dataset = {dataset_value}")

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
def write_to_postgres(df, dataset,conn_params, method="optimized", batch_size=None, num_partitions=None, **kwargs):
    """
    Insert DataFrame rows into PostgreSQL table using optimized PySpark JDBC writer.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame to insert
        conn_params (dict): PostgreSQL connection parameters
        method (str): Write method - "optimized" (default), "standard"
        batch_size (int): Number of rows per batch (auto-calculated if None)
        num_partitions (int): Number of partitions (auto-calculated if None)
        **kwargs: Additional parameters for specific methods
    """
    logger.info(f"write_to_postgres: Using {method} method for PostgreSQL writes")
    
    if method == "standard":
        return _write_to_postgres_standard(df, dataset,conn_params)
    else:  # method == "optimized" (default) - handles any invalid method as optimized
        return _write_to_postgres_optimized(df, dataset,conn_params, batch_size, num_partitions)


def _write_to_postgres_standard(df, dataset, conn_params):
    """
    Original PostgreSQL writer (for comparison and fallback).
    """
    logger.info("_write_to_postgres_standard: Using original JDBC writer")
    
    # Ensure table exists before inserting
    create_table(conn_params, dataset)

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


def _write_to_postgres_optimized(df, dataset, conn_params, batch_size=None, num_partitions=None):
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
    create_table(conn_params, dataset)

    # Auto-calculate optimal batch size if not specified
    if batch_size is None:
        row_count = df.count()
        if row_count < 10000:
            batch_size = 1000      # Small datasets
        elif row_count < 100000:
            batch_size = 2000      # Small-medium datasets
        elif row_count < 1000000:
            batch_size = 3000      # Medium datasets
        elif row_count < 10000000:
            batch_size = 4000      # Large datasets
        else:
            batch_size = 5000      # Very large datasets - proven PostgreSQL maximum
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
        
        # Connection optimizations (conservative timeouts for large datasets)
        "tcpKeepAlive": "true",                          # Keep connections alive
        "socketTimeout": "900",                          # 15 minute socket timeout for large batches
        "loginTimeout": "60",                            # 60 second login timeout
        "connectTimeout": "60",                          # 60 second connect timeout
        
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
            "batch_size": 2000,
            "num_partitions": 2,
            "notes": ["Small-medium dataset - conservative batch size for reliability"]
        })
    
    elif row_count < 1000000:
        recommendations.update({
            "method": "optimized",
            "batch_size": 3000,
            "num_partitions": 4,
            "notes": ["Medium dataset - production-tested batch size"]
        })
    
    elif row_count < 10000000:
        recommendations.update({
            "method": "optimized",
            "batch_size": 4000,
            "num_partitions": 8,
            "notes": ["Large dataset - conservative batch processing for stability"]
        })
    
    else:  # Very large datasets (10M+ rows)
        max_partitions = min(16, max(6, available_memory_gb // 2))
        recommendations.update({
            "method": "optimized",  # Use optimized method with small, reliable batch sizes
            "batch_size": 5000,
            "num_partitions": max_partitions,
            "notes": [
                "Very large dataset - conservative 5K batch size for maximum reliability",
                "Small batch sizes prevent Aurora PostgreSQL timeouts and memory issues",
                f"Using {max_partitions} partitions for parallel processing",
                "Proven safe batch size for 30M+ row datasets"
            ]
        })
    
    return recommendations


