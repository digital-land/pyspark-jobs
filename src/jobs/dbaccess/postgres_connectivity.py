# ================================================================================
# DATABASE TABLE CONFIGURATION
# ================================================================================
# 
# ⚠️  TEMPORARY CONFIGURATION - EASY TO REVERT ⚠️
# 
# To switch back to production 'entity' table, change line below:
#   ENTITY_TABLE_NAME = "pyspark_entity"  (current - temporary)
#   ENTITY_TABLE_NAME = "entity"          (production - to revert to)
# 
# This single change will update:
#   - All JDBC writes
#   - All Aurora S3 imports  
#   - Staging table naming
#   - Table creation logic
# ================================================================================

ENTITY_TABLE_NAME = "entity"

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
from datetime import datetime
from pathlib import Path

# Define your table schema
# https://github.com/digital-land/digital-land.info/blob/main/application/db/models.py - refered from here
dbtable_name = ENTITY_TABLE_NAME  # Use centralized config above
pyspark_entity_columns = {   
    "dataset": "TEXT",
    "end_date": "DATE",
    "entity": "BIGINT",
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
    "quality": "TEXT"
    #"processed_timestamp": "TIMESTAMP"  # New column for processing timestamp
}

# PostgreSQL connection parameters
#conn_params = get_secret()  # Assuming this function retrieves the secret as a dictionary
# read host, port,dbname,user, password


def get_aws_secret(environment="development"):
    """
    Retrieve AWS secrets for PostgreSQL connection with EMR Serverless compatibility.
    
    This function uses the EMR-compatible secret retrieval method that includes
    multiple fallback strategies to handle botocore issues in EMR environments.
    
    Args:
        environment (str): Environment name (development, staging, production)
                          Defaults to "development"
    
    Returns:
        dict: PostgreSQL connection parameters
        
    Raises:
        Exception: If secrets cannot be retrieved or parsed
    """
    try:
        logger.info(f"Attempting to retrieve PostgreSQL secrets using EMR-compatible method for environment: {environment}")
        
        # Construct secret path based on environment
        secret_path = f"/{environment}-pd-batch/postgres-secret"
        logger.info(f"Using secret path: {secret_path}")
        
        aws_secrets_json = get_secret_emr_compatible(secret_path)
        
        # Parse the JSON string
        secrets = json.loads(aws_secrets_json)

        # Extract required fields
        username = secrets.get("username")
        password = secrets.get("password")
        dbName = secrets.get("db_name")
        host = secrets.get("host")
        port = secrets.get("port")
        
        # Validate required fields
        required_fields = ["username", "password", "db_name", "host", "port"]
        missing_fields = [field for field in required_fields if not secrets.get(field)]
        
        if missing_fields:
            error_msg = f"Missing required secret fields: {missing_fields}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"get_aws_secret: Retrieved secrets for {dbName} at {host}:{port} with user {username}")
        
        # Create proper SSL context for Aurora PostgreSQL
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        conn_params = {
            "database": dbName,  # pg8000 uses 'database' not 'dbname'
            "host": host,
            "port": int(port),  # Ensure port is integer
            "user": username,
            "password": password,
            "timeout": 300,  # 5 minute timeout for long-running operations (DELETE on large datasets)
            "ssl_context": ssl_context  # Proper SSL context for Aurora
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


# -------------------- Staging Table Pattern --------------------

def cleanup_old_staging_tables(conn_params, max_age_hours=24, max_retries=3):
    """
    Clean up old staging tables that may have been left behind from failed jobs.
    
    Args:
        conn_params (dict): Database connection parameters
        max_age_hours (int): Maximum age in hours for staging tables to keep
        max_retries (int): Maximum number of connection attempts
    """
    import time
    if pg8000:
        from pg8000.exceptions import InterfaceError, DatabaseError
    else:
        logger.warning("cleanup_old_staging_tables: pg8000 not available")
        return
    
    conn = None
    cur = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"cleanup_old_staging_tables: Starting cleanup (attempt {attempt + 1}/{max_retries})")
            
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            
            # Find old staging tables
            find_staging_query = f"""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' 
                  AND tablename LIKE '{dbtable_name}_staging_%'
                  AND tablename ~ '{dbtable_name}_staging_[a-f0-9]{{8}}_[0-9]{{8}}_[0-9]{{6}}$'
            """
            
            cur.execute(find_staging_query)
            staging_tables = [row[0] for row in cur.fetchall()]
            
            if not staging_tables:
                logger.info("cleanup_old_staging_tables: No staging tables found")
                return
            
            logger.info(f"cleanup_old_staging_tables: Found {len(staging_tables)} staging tables")
            
            # Check age and drop old tables
            dropped_count = 0
            for table_name in staging_tables:
                try:
                    # Extract timestamp from table name
                    parts = table_name.split('_')
                    if len(parts) >= 4:
                        timestamp_str = f"{parts[-2]}_{parts[-1]}"
                        # Validate timestamp format before parsing
                        if len(timestamp_str) == 15 and timestamp_str[8] == '_' and timestamp_str[:8].isdigit() and timestamp_str[9:].isdigit():
                            table_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                            age_hours = (datetime.now() - table_time).total_seconds() / 3600
                            
                            if age_hours > max_age_hours:
                                cur.execute(f"DROP TABLE IF EXISTS {table_name};")
                                dropped_count += 1
                                logger.info(f"cleanup_old_staging_tables: Dropped old staging table {table_name} (age: {age_hours:.1f}h)")
                            else:
                                logger.debug(f"cleanup_old_staging_tables: Keeping recent staging table {table_name} (age: {age_hours:.1f}h)")
                        else:
                            logger.debug(f"cleanup_old_staging_tables: Skipping table with invalid timestamp format: {table_name}")
                except Exception as e:
                    logger.warning(f"cleanup_old_staging_tables: Error processing table {table_name}: {e}")
            
            conn.commit()
            logger.info(f"cleanup_old_staging_tables: Cleanup complete - dropped {dropped_count} old staging tables")
            return
            
        except Exception as e:
            if attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 30)
                logger.error(f"cleanup_old_staging_tables: Error (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"cleanup_old_staging_tables: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"cleanup_old_staging_tables: All {max_retries} attempts failed: {e}")
        finally:
            if cur:
                try:
                    cur.close()
                except Exception as e:
                    logger.warning(f"cleanup_old_staging_tables: Error closing cursor: {e}")
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"cleanup_old_staging_tables: Error closing connection: {e}")
            conn = None
            cur = None

def create_and_prepare_staging_table(conn_params, dataset_value, max_retries=3):
    """
    Create a temporary staging table for data loading.
    
    This approach minimizes lock contention on the main entity table by:
    1. Writing all data to a staging table first
    2. Performing validation on staging table
    3. Atomically swapping data from staging to production table
    
    Args:
        conn_params (dict): Database connection parameters
        dataset_value (str): Dataset value for the staging table
        max_retries (int): Maximum number of connection attempts
        
    Returns:
        str: Name of the staging table created
    """
    import time
    if pg8000:
        from pg8000.exceptions import InterfaceError, DatabaseError
    else:
        logger.warning("create_and_prepare_staging_table: pg8000 not available")
        InterfaceError = DatabaseError = Exception
    
    # Generate unique staging table name based on actual target table
    import hashlib
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_hash = hashlib.md5(dataset_value.encode()).hexdigest()[:8]
    staging_table_name = f"{dbtable_name}_staging_{dataset_hash}_{timestamp}"
    
    conn = None
    cur = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"create_and_prepare_staging_table: Creating staging table (attempt {attempt + 1}/{max_retries})")
            
            if not pg8000:
                raise ImportError("pg8000 required for direct database connections")
            
            # Clean up old staging tables before creating new one
            if attempt == 0:  # Only on first attempt
                try:
                    cleanup_old_staging_tables(conn_params)
                except Exception as e:
                    logger.warning(f"create_and_prepare_staging_table: Cleanup warning (non-critical): {e}")
            
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            
            # Set shorter timeout for staging table operations (no large deletes)
            cur.execute("SET statement_timeout = '60000';")  # 1 minute
            
            # Create staging table - use TEXT for columns that JDBC writes as TEXT
            # JDBC with stringtype=unspecified writes BIGINT and JSONB as TEXT
            staging_columns = []
            for col, dtype in pyspark_entity_columns.items():
                if 'JSONB' in dtype.upper() or 'BIGINT' in dtype.upper():
                    # Use TEXT for JSONB and BIGINT since PySpark JDBC writes them as TEXT
                    staging_columns.append(f"{col} TEXT")
                else:
                    staging_columns.append(f"{col} {dtype}")
            
            column_defs = ", ".join(staging_columns)
            create_staging_query = f"""
                CREATE TABLE {staging_table_name} (
                    {column_defs}
                );
            """
            
            logger.debug(f"create_and_prepare_staging_table: Creating staging with schema: {column_defs[:200]}...")
            cur.execute(create_staging_query)
            conn.commit()
            logger.info(f"create_and_prepare_staging_table: Created regular table (not TEMP) for cross-session JDBC compatibility")
            
            logger.info(f"create_and_prepare_staging_table: Successfully created staging table '{staging_table_name}'")
            
            # Ensure main entity table exists (with original JSONB columns)
            entity_column_defs = ", ".join([f"{col} {dtype}" for col, dtype in pyspark_entity_columns.items()])
            create_entity_query = f"CREATE TABLE IF NOT EXISTS {dbtable_name} ({entity_column_defs});"
            cur.execute(create_entity_query)
            conn.commit()
            
            # Add a function to cast staging table columns after data load
            logger.info(f"create_and_prepare_staging_table: Staging table ready for data load and casting")
            
            # Create index on dataset column if it doesn't exist (critical for fast DELETE)
            logger.info(f"create_and_prepare_staging_table: Ensuring index on dataset column for fast deletion")
            create_index_query = f"CREATE INDEX IF NOT EXISTS idx_{dbtable_name}_dataset ON {dbtable_name}(dataset);"
            cur.execute(create_index_query)
            conn.commit()
            logger.info(f"create_and_prepare_staging_table: Index on dataset column ensured")
            
            logger.info(f"create_and_prepare_staging_table: Verified main table '{dbtable_name}' exists")
            
            cur.close()
            conn.close()
            
            return staging_table_name
            
        except InterfaceError as e:
            error_str = str(e).lower()
            if attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 30)
                logger.error(f"create_and_prepare_staging_table: Network error (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"create_and_prepare_staging_table: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
                
        except Exception as e:
            if attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 30)
                logger.error(f"create_and_prepare_staging_table: Error (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                logger.info(f"create_and_prepare_staging_table: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"create_and_prepare_staging_table: All {max_retries} attempts failed")
                raise
        finally:
            if cur:
                try:
                    cur.close()
                except Exception as e:
                    logger.warning(f"create_and_prepare_staging_table: Error closing cursor: {e}")
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"create_and_prepare_staging_table: Error closing connection: {e}")
            conn = None
            cur = None
    
    # If we reach here, all attempts failed - try to clean up any staging table that might have been created
    try:
        cleanup_old_staging_tables(conn_params, max_age_hours=0)  # Clean up immediately on failure
    except Exception as e:
        logger.warning(f"create_and_prepare_staging_table: Final cleanup warning: {e}")


def commit_staging_to_production(conn_params, staging_table_name, dataset_value, max_retries=3):
    """
    Atomically move data from staging table to production entity table.
    
    This operation:
    1. Deletes existing data for the dataset from entity table (fast, indexed delete)
    2. Inserts data from staging table to entity table (bulk insert)
    3. Drops the staging table
    
    The operation is transactional - either all succeeds or all fails.
    
    Args:
        conn_params (dict): Database connection parameters
        staging_table_name (str): Name of the staging table
        dataset_value (str): Dataset value to replace in production table
        max_retries (int): Maximum number of attempts
        
    Returns:
        dict: Statistics about the commit operation
    """
    import time
    if pg8000:
        from pg8000.exceptions import InterfaceError, DatabaseError
    else:
        logger.warning("commit_staging_to_production: pg8000 not available")
        InterfaceError = DatabaseError = Exception
    
    conn = None
    cur = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"commit_staging_to_production: Starting commit operation (attempt {attempt + 1}/{max_retries})")
            
            if not pg8000:
                raise ImportError("pg8000 required for direct database connections")
            
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            
            # Set longer timeout for the production table operations
            cur.execute("SET statement_timeout = '300000';")  # 5 minutes
            
            # BEGIN TRANSACTION (implicit with pg8000)
            logger.info(f"commit_staging_to_production: Starting transaction for dataset '{dataset_value}'")
            
            # Step 1: Count rows in staging table
            cur.execute(f"SELECT COUNT(*) FROM {staging_table_name};")
            result = cur.fetchone()
            staging_count = result[0] if result else 0
            logger.info(f"commit_staging_to_production: Staging table has {staging_count:,} rows")
            
            if staging_count == 0:
                logger.warning(f"commit_staging_to_production: Staging table is empty, aborting commit")
                conn.rollback()
                return {
                    "success": False,
                    "error": "Staging table is empty",
                    "rows_deleted": 0,
                    "rows_inserted": 0
                }
            
            # Step 2: Delete existing data for this dataset from entity table
            logger.info(f"commit_staging_to_production: Deleting existing data for dataset '{dataset_value}'")
            start_delete = time.time()
            
            # Use indexed delete for better performance
            delete_query = f"DELETE FROM {dbtable_name} WHERE dataset = %s;"
            cur.execute(delete_query, (dataset_value,))
            deleted_count = cur.rowcount
            
            delete_duration = time.time() - start_delete
            logger.info(f"commit_staging_to_production: Deleted {deleted_count:,} existing rows in {delete_duration:.2f}s")
            
            # Step 3: Insert data from staging to entity table
            logger.info(f"commit_staging_to_production: Inserting data from staging to entity table")
            start_insert = time.time()
            
            # Build INSERT with explicit column names and proper type casting
            column_names = list(pyspark_entity_columns.keys())
            select_columns = []
            
            for col_name, col_type in pyspark_entity_columns.items():
                if 'JSONB' in col_type.upper():
                    select_columns.append(f"NULLIF({col_name}, '')::jsonb")
                elif 'BIGINT' in col_type.upper():
                    select_columns.append(f"NULLIF({col_name}, '')::bigint")
                else:
                    select_columns.append(col_name)
            
            column_list = ", ".join(column_names)
            select_str = ", ".join(select_columns)
            insert_query = f"""
                INSERT INTO {dbtable_name} ({column_list})
                SELECT {select_str} FROM {staging_table_name};
            """
            
            logger.info(f"commit_staging_to_production: Executing INSERT with explicit columns and casts")
            logger.debug(f"commit_staging_to_production: Query: {insert_query[:500]}...")
            cur.execute(insert_query)
            inserted_count = cur.rowcount
            
            insert_duration = time.time() - start_insert
            logger.info(f"commit_staging_to_production: Inserted {inserted_count:,} rows in {insert_duration:.2f}s")
            
            # Step 4: Verify row counts match
            if inserted_count != staging_count:
                logger.error(
                    f"commit_staging_to_production: Row count mismatch! "
                    f"Staging: {staging_count:,}, Inserted: {inserted_count:,}"
                )
                conn.rollback()
                return {
                    "success": False,
                    "error": f"Row count mismatch: {staging_count} vs {inserted_count}",
                    "rows_deleted": 0,
                    "rows_inserted": 0
                }
            
            # COMMIT TRANSACTION
            conn.commit()
            
            total_duration = delete_duration + insert_duration
            logger.info(
                f"commit_staging_to_production: ✓ Transaction committed successfully - "
                f"deleted {deleted_count:,}, inserted {inserted_count:,} in {total_duration:.2f}s"
            )
            
            # Step 5: Drop staging table (outside transaction)
            try:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table_name};")
                conn.commit()
                logger.info(f"commit_staging_to_production: Dropped staging table '{staging_table_name}'")
            except Exception as e:
                logger.warning(f"commit_staging_to_production: Failed to drop staging table (non-critical): {e}")
            
            cur.close()
            conn.close()
            
            return {
                "success": True,
                "rows_deleted": deleted_count,
                "rows_inserted": inserted_count,
                "delete_duration": delete_duration,
                "insert_duration": insert_duration,
                "total_duration": total_duration
            }
            
        except InterfaceError as e:
            error_str = str(e).lower()
            logger.error(f"commit_staging_to_production: Network error (attempt {attempt + 1}/{max_retries}): {e}")
            
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            
            if attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 30)
                logger.info(f"commit_staging_to_production: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
                
        except Exception as e:
            logger.error(f"commit_staging_to_production: Error (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
            
            if conn:
                try:
                    conn.rollback()
                    logger.info("commit_staging_to_production: Transaction rolled back")
                except:
                    pass
            
            if attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 30)
                logger.info(f"commit_staging_to_production: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
        finally:
            if cur:
                try:
                    cur.close()
                except Exception as e:
                    logger.warning(f"commit_staging_to_production: Error closing cursor: {e}")
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"commit_staging_to_production: Error closing connection: {e}")
            conn = None
            cur = None


# Create table if not exists
def create_table(conn_params, dataset_value, max_retries=5):
    """
    Create table with retry logic using exponential backoff.
    
    Args:
        conn_params (dict): Database connection parameters
        dataset_value (str): Dataset value for filtering/deletion
        max_retries (int): Maximum number of connection attempts (default: 5)
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
            
            # Set PostgreSQL statement timeout to 5 minutes for long-running DELETE operations
            # This prevents indefinite hangs on busy Aurora instances
            cur.execute("SET statement_timeout = '300000';")  # 5 minutes in milliseconds
            logger.info("create_table: Set statement_timeout to 5 minutes for long operations")
            
            # Check for any existing long-running queries that might be blocking
            # This prevents pileup of DELETE operations from previous failed attempts
            check_blocking_query = """
                SELECT pid, query_start, state, query
                FROM pg_stat_activity
                WHERE state = 'active'
                  AND query ILIKE '%DELETE FROM entity%'
                  AND query NOT ILIKE '%pg_stat_activity%'
                  AND pid != pg_backend_pid()
                  AND (now() - query_start) > interval '30 seconds';
            """
            cur.execute(check_blocking_query)
            blocking_queries = cur.fetchall()
            
            if blocking_queries:
                logger.warning(f"create_table: Found {len(blocking_queries)} existing DELETE operations still running from previous attempts")
                for pid, query_start, state, query in blocking_queries:
                    duration = (datetime.now() - query_start).total_seconds() if query_start else 0
                    logger.warning(f"create_table: - PID {pid}: running for {duration:.0f}s - {query[:100]}")
                    # Terminate the stuck query to prevent blocking
                    try:
                        cur.execute(f"SELECT pg_terminate_backend({pid});")
                        logger.info(f"create_table: Terminated stuck DELETE operation (PID {pid})")
                    except Exception as e:
                        logger.warning(f"create_table: Could not terminate PID {pid}: {e}")
                conn.commit()
                logger.info("create_table: Cleaned up stuck DELETE operations, proceeding with fresh attempt")
            
            # Build CREATE TABLE SQL dynamically
            column_defs = ", ".join([f"{col} {dtype}" for col, dtype in pyspark_entity_columns.items()])
            create_query = f"CREATE TABLE IF NOT EXISTS {dbtable_name} ({column_defs});"
            logger.info(f"create_table: Creating table {dbtable_name} with columns: {list(pyspark_entity_columns.keys())}")

            cur.execute(create_query)
            conn.commit()
            logger.info(f"create_table: Table '{dbtable_name}' created successfully (if it didn't exist).")

            # Select and delete records by dataset value if provided
            if dataset_value:
                # First, check how many records exist for this dataset
                count_query = f"SELECT COUNT(*) FROM {dbtable_name} WHERE dataset = %s;"
                cur.execute(count_query, (dataset_value,))
                result = cur.fetchone()
                record_count = result[0] if result else 0
                logger.info(f"create_table: Found {record_count} existing records for dataset '{dataset_value}'")
                
                if record_count > 0:
                    # Adaptive batch sizing based on dataset size for optimal performance
                    if record_count > 10000:
                        # Calculate optimal batch size based on record count
                        # For millions of records, use larger batches to reduce total number of operations
                        if record_count < 100000:
                            batch_size = 10000      # 100K records: 10 batches
                        elif record_count < 500000:
                            batch_size = 25000      # 500K records: 20 batches  
                        elif record_count < 1000000:
                            batch_size = 50000      # 1M records: 20 batches
                        elif record_count < 5000000:
                            batch_size = 100000     # 5M records: 50 batches
                        else:
                            batch_size = 200000     # 10M+ records: 50+ batches
                        
                        logger.info(f"create_table: Large dataset detected ({record_count:,} records). Deleting in batches of {batch_size:,} to avoid locks...")
                        
                        # Use CTID-based batch deletion for better performance on large datasets
                        total_deleted = 0
                        batch_num = 0
                        start_time = time.time()
                        
                        while True:
                            batch_num += 1
                            batch_start_time = time.time()
                            
                            delete_batch_query = f"""
                                DELETE FROM {dbtable_name} 
                                WHERE ctid IN (
                                    SELECT ctid FROM {dbtable_name} 
                                    WHERE dataset = %s 
                                    LIMIT {batch_size}
                                );
                            """
                            
                            # Execute DELETE and wait for completion
                            cur.execute(delete_batch_query, (dataset_value,))
                            deleted = cur.rowcount
                            total_deleted += deleted
                            
                            # Commit and wait for commit to complete
                            conn.commit()
                            
                            # Calculate progress and performance metrics
                            batch_duration = time.time() - batch_start_time
                            total_duration = time.time() - start_time
                            progress_pct = (total_deleted / record_count) * 100
                            
                            # Estimate remaining time
                            if total_deleted > 0:
                                avg_time_per_record = total_duration / total_deleted
                                remaining_records = record_count - total_deleted
                                est_remaining_time = avg_time_per_record * remaining_records
                                est_remaining_min = est_remaining_time / 60
                                
                                logger.info(
                                    f"create_table: Batch {batch_num} completed in {batch_duration:.1f}s - "
                                    f"deleted {deleted:,} records (total: {total_deleted:,}/{record_count:,} = {progress_pct:.1f}%) - "
                                    f"est. {est_remaining_min:.1f} min remaining"
                                )
                            else:
                                logger.info(f"create_table: Batch {batch_num} completed - deleted {deleted:,} records")
                            
                            # Check if deletion is complete
                            if deleted < batch_size:
                                logger.info(f"create_table: Batch deletion complete - last batch had {deleted:,} records in {total_duration:.1f}s total")
                                break  # No more records to delete
                        
                        # Quick verification using EXISTS instead of COUNT for better performance on large tables
                        logger.info(f"create_table: Verifying deletion completed successfully...")
                        verify_query = f"SELECT EXISTS(SELECT 1 FROM {dbtable_name} WHERE dataset = %s LIMIT 1);"
                        cur.execute(verify_query, (dataset_value,))
                        result = cur.fetchone()
                        records_exist = result[0] if result else False
                        
                        if not records_exist:
                            logger.info(f"create_table: ✓ Deletion verified - all {total_deleted:,} records successfully deleted for dataset '{dataset_value}' in {total_duration:.1f}s")
                        else:
                            # If records still exist, do a COUNT to see how many
                            cur.execute(count_query, (dataset_value,))
                            result = cur.fetchone()
                            remaining_count = result[0] if result else 0
                            logger.warning(f"create_table: ⚠ Verification failed - {remaining_count:,} records still remain for dataset '{dataset_value}' (deleted {total_deleted:,})")
                            
                            # Try one more batch deletion for remaining records
                            logger.info(f"create_table: Attempting to delete remaining {remaining_count:,} records...")
                            delete_query = f"DELETE FROM {dbtable_name} WHERE dataset = %s;"
                            cur.execute(delete_query, (dataset_value,))
                            final_deleted = cur.rowcount
                            conn.commit()
                            logger.info(f"create_table: Final cleanup deleted {final_deleted:,} remaining records")
                            
                    else:
                        # Small dataset - delete all at once
                        logger.info(f"create_table: Deleting {record_count} records from {dbtable_name} where dataset = {dataset_value}")
                        delete_query = f"DELETE FROM {dbtable_name} WHERE dataset = %s;"
                        
                        # Execute DELETE and wait for completion
                        cur.execute(delete_query, (dataset_value,))
                        deleted_count = cur.rowcount
                        
                        # Commit and wait for commit to complete
                        conn.commit()
                        logger.info(f"create_table: DELETE operation completed - {deleted_count} rows affected")
                        
                        # Verify deletion completed successfully
                        logger.info(f"create_table: Verifying deletion completed successfully...")
                        cur.execute(count_query, (dataset_value,))
                        result = cur.fetchone()
                        remaining_count = result[0] if result else 0
                        
                        if remaining_count == 0:
                            logger.info(f"create_table: ✓ Deletion verified - all {deleted_count} records successfully deleted for dataset '{dataset_value}'")
                        else:
                            logger.error(f"create_table: ✗ Deletion verification failed - {remaining_count} records still exist after DELETE (expected 0)")
                            raise DatabaseError(f"DELETE operation failed to remove all records for dataset '{dataset_value}' - {remaining_count} records remaining")
                else:
                    logger.info(f"create_table: No existing records to delete for dataset '{dataset_value}'")

            return  # Success, exit function
            
        except InterfaceError as e:
            error_str = str(e).lower()
            if attempt < max_retries - 1:
                # Exponential backoff: 5s, 10s, 20s, 40s, 60s (capped at 60s)
                retry_delay = min(5 * (2 ** attempt), 60)
                logger.error(f"create_table: Network/Interface error (attempt {attempt + 1}/{max_retries}): {e}")
                
                # Check for specific error types
                if "network error" in error_str or "timeout" in error_str:
                    logger.error("create_table: Network or timeout error detected")
                    logger.error("create_table: This is likely due to:")
                    logger.error("create_table: 1. DELETE operation timing out on large dataset")
                    logger.error("create_table: 2. Aurora instance busy or at capacity")
                    logger.error("create_table: 3. Network connectivity issues")
                    logger.error("create_table: 4. Database locks from concurrent operations")
                elif "can't create a connection" in error_str:
                    logger.error("create_table: Connection establishment failed")
                    logger.error("create_table: Possible causes:")
                    logger.error("create_table: 1. EMR Serverless not configured in correct VPC")
                    logger.error("create_table: 2. Security group rules blocking connection")
                    logger.error("create_table: 3. RDS database not accessible from EMR subnet")
                
                logger.info(f"create_table: Retrying in {retry_delay} seconds with exponential backoff...")
                time.sleep(retry_delay)
            else:
                logger.error(f"create_table: All {max_retries} connection attempts failed")
                raise
                
        except DatabaseError as e:
            error_str = str(e).lower()
            # Check if it's a retryable database error (timeout, deadlock, etc.)
            is_retryable = any(keyword in error_str for keyword in [
                'timeout', 'deadlock', 'lock', 'busy', 'could not serialize'
            ])
            
            if is_retryable and attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 60)
                logger.error(f"create_table: Retryable database error (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"create_table: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"create_table: Non-retryable database error: {e}")
                raise  # Don't retry non-retryable database errors
            
        except Exception as e:
            if attempt < max_retries - 1:
                # Exponential backoff for unexpected errors too
                retry_delay = min(5 * (2 ** attempt), 60)
                logger.error(f"create_table: Unexpected error (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                logger.info(f"create_table: Retrying in {retry_delay} seconds with exponential backoff...")
                time.sleep(retry_delay)
            else:
                logger.error(f"create_table: All {max_retries} attempts failed with unexpected errors")
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
    Prepare DataFrame for PostgreSQL insertion with proper data type casting.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame with potential geometry columns
        
    Returns:
        pyspark.sql.DataFrame: DataFrame with properly cast columns
    """
    from pyspark.sql.functions import when, col, expr
    from pyspark.sql.types import LongType
    
    logger.info("_prepare_geometry_columns: Processing DataFrame for PostgreSQL compatibility")
    
    processed_df = df
    
    # Cast entity column to LongType (maps to BIGINT in PostgreSQL)
    if "entity" in df.columns:
        logger.info("_prepare_geometry_columns: Casting entity column to LongType")
        processed_df = processed_df.withColumn("entity", col("entity").cast(LongType()))
    
    # Process geometry columns
    geometry_columns = ["geometry", "point"]
    for geom_col in geometry_columns:
        if geom_col in df.columns:
            logger.info(f"_prepare_geometry_columns: Processing geometry column: {geom_col}")
            processed_df = processed_df.withColumn(
                geom_col,
                when(col(geom_col).isNull(), None)
                .when(col(geom_col) == "", None)
                .when(col(geom_col).startswith("POINT"), 
                      expr(f"concat('SRID=4326;', {geom_col})"))
                .when(col(geom_col).startswith("POLYGON"), 
                      expr(f"concat('SRID=4326;', {geom_col})"))
                .when(col(geom_col).startswith("MULTIPOLYGON"), 
                      expr(f"concat('SRID=4326;', {geom_col})"))
                .otherwise(col(geom_col))
            )
    
    return processed_df

def calculate_centroid_wkt(conn_params, target_table=None, max_retries=3):
    """
    Calculate centroids for geometries and update the point column.
    Uses PostGIS ST_PointOnSurface for more accurate centroid calculation that 
    ensures the point falls within the geometry.
    
    Args:
        conn_params (dict): Database connection parameters
        target_table (str): Optional target table name. If provided, updates this table
                           instead of the main table. Used for staging table operations.
        max_retries (int): Maximum number of retry attempts
    
    Returns:
        int: Number of rows updated with calculated centroids
    """
    if pg8000:
        from pg8000.exceptions import InterfaceError
    else:
        logger.warning("calculate_centroid_wkt: pg8000 not available")
        InterfaceError = Exception
    
    # Determine which table to update
    table_name = target_table if target_table else dbtable_name
    
    conn = None
    cur = None
    
    for attempt in range(max_retries):
        try:
            logger.info(
                f"calculate_centroid_wkt: Connecting to database to calculate "
                f"centroids for table '{table_name}'"
            )
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            
            # Set longer timeout for potentially long-running geometry operations
            cur.execute("SET statement_timeout = '300000';")  # 5 minutes
            
            sql_update_pos = f"""
            UPDATE {table_name} t
            SET point = ST_PointOnSurface(
                CASE 
                    WHEN NOT ST_IsValid(t.geometry) 
                    THEN ST_MakeValid(t.geometry) 
                    ELSE t.geometry 
                END
            )::geometry(Point, 4326)
            WHERE point IS NULL AND geometry IS NOT NULL;
            """
            
            cur.execute(sql_update_pos)
            rows_updated = cur.rowcount
            conn.commit()
            
            logger.info(f"calculate_centroid_wkt: Updated {rows_updated} rows with calculated centroids")
            return rows_updated
            
        except InterfaceError as e:
            error_str = str(e).lower()
            if attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 30)
                logger.error(f"calculate_centroid_wkt: Network error (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"calculate_centroid_wkt: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
        
        except Exception as e:
            if attempt < max_retries - 1:
                retry_delay = min(5 * (2 ** attempt), 30)
                logger.error(f"calculate_centroid_wkt: Error (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"calculate_centroid_wkt: Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise
                
        finally:
            if cur:
                try:
                    cur.close()
                except Exception as e:
                    logger.warning(f"calculate_centroid_wkt: Error closing cursor: {e}")
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"calculate_centroid_wkt: Error closing connection: {e}")
            conn = None
            cur = None



# -------------------- PostgreSQL Writer --------------------

##writing to postgres db
def write_to_postgres(df, dataset, conn_params, method="optimized", batch_size=None, num_partitions=None, target_table=None, **kwargs):
    """
    Insert DataFrame rows into PostgreSQL table using optimized PySpark JDBC writer.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame to insert
        dataset (str): Dataset name
        conn_params (dict): PostgreSQL connection parameters
        method (str): Write method - "optimized" (default), "standard"
        batch_size (int): Number of rows per batch (auto-calculated if None)
        num_partitions (int): Number of partitions (auto-calculated if None)
        target_table (str): Target table name (defaults to global dbtable_name if None).
                           Use this to write to staging tables.
        **kwargs: Additional parameters for specific methods
    """
    logger.info(f"write_to_postgres: Using {method} method for PostgreSQL writes")
    
    return _write_to_postgres_optimized(df, dataset, conn_params, batch_size, num_partitions, target_table)


# def _write_to_postgres_standard(df, dataset, conn_params):
#     """
#     Original PostgreSQL writer (for comparison and fallback).
#     """
#     logger.info("_write_to_postgres_standard: Using original JDBC writer")
    
#     # Ensure table exists before inserting
#     create_table(conn_params, dataset)

#     url = f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
#     properties = {
#         "user": conn_params["user"],
#         "password": conn_params["password"],
#         "driver": "org.postgresql.Driver",
#         "stringtype": "unspecified"  # This helps with PostGIS geometry columns
#     }

#     try:
#         # Convert DataFrame to handle geometry columns for PostgreSQL
#         processed_df = _prepare_geometry_columns(df)
        
#         processed_df.write \
#             .jdbc(url=url, table=dbtable_name, mode="append", properties=properties)
#         logger.info(f"_write_to_postgres_standard: Inserted {processed_df.count()} rows into {dbtable_name} using standard JDBC")
#     except Exception as e:
#         logger.error(f"_write_to_postgres_standard: Failed to write to PostgreSQL via JDBC: {e}", exc_info=True)
#         raise


def _write_to_postgres_optimized(df, dataset, conn_params, batch_size=None, num_partitions=None, target_table=None, max_retries=5):
    """
    Optimized PostgreSQL writer with performance improvements and retry logic.
    
    Args:
        df: PySpark DataFrame to write
        dataset: Dataset name
        conn_params: Database connection parameters
        batch_size: Number of rows per batch (auto-calculated if None)
        num_partitions: Number of partitions (auto-calculated if None)
        target_table: Target table name (uses global dbtable_name if None)
        max_retries: Maximum number of write attempts (default: 5)
    """
    # Determine target table (staging or production)
    table_name = target_table if target_table else dbtable_name
    
    logger.info(f"_write_to_postgres_optimized: Starting optimized PostgreSQL write to table '{table_name}'")
    
    # Only create table if writing to production (not staging)
    if not target_table:
        # Ensure production table exists (has its own retry logic)
        create_table(conn_params, dataset)
    else:
        logger.info(f"_write_to_postgres_optimized: Writing to staging table '{target_table}', skipping create_table")

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
    
    # Retry logic with exponential backoff for busy Aurora instances
    for attempt in range(max_retries):
        try:
            logger.info(f"_write_to_postgres_optimized: Write attempt {attempt + 1}/{max_retries}")
            logger.info(f"_write_to_postgres_optimized: Writing {processed_df.count()} rows to PostgreSQL with batch size {batch_size}")
            logger.info(f"_write_to_postgres_optimized: Using {num_partitions} partitions for parallel writes")
            
            # Use optimized JDBC write with error handling
            processed_df.write \
                .mode("append") \
                .option("numPartitions", num_partitions) \
                .jdbc(url=url, table=table_name, properties=properties)
            
            logger.info(f"_write_to_postgres_optimized: Successfully wrote data to {table_name} using optimized JDBC writer")
            return  # Success - exit function
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if this is a retryable error (connection issues, timeouts, busy database)
            is_retryable = any(keyword in error_str for keyword in [
                'timeout', 'connection', 'busy', 'locked', 'deadlock', 
                'could not connect', 'refused', 'network', 'temporarily unavailable',
                'too many connections', 'cannot acquire'
            ])
            
            if is_retryable and attempt < max_retries - 1:
                # Exponential backoff: 5s, 10s, 20s, 40s, 60s (capped at 60s)
                retry_delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"_write_to_postgres_optimized: Write failed (attempt {attempt + 1}/{max_retries}): {e}")
                logger.warning(f"_write_to_postgres_optimized: Aurora may be busy - retrying in {retry_delay} seconds...")
                logger.warning(f"_write_to_postgres_optimized: Error type: {type(e).__name__}")
                time.sleep(retry_delay)
            else:
                # Non-retryable error or final attempt - log and raise
                logger.error(f"_write_to_postgres_optimized: Failed to write to PostgreSQL after {attempt + 1} attempts: {e}")
                logger.error("_write_to_postgres_optimized: Troubleshooting steps:")
                logger.error("_write_to_postgres_optimized: 1. Check if PostgreSQL JDBC driver is available")
                logger.error("_write_to_postgres_optimized: 2. Verify network connectivity to database")
                logger.error("_write_to_postgres_optimized: 3. Check database permissions for the user")
                logger.error("_write_to_postgres_optimized: 4. Verify table schema matches DataFrame columns")
                logger.error("_write_to_postgres_optimized: 5. Check if Aurora instance is busy or at capacity")
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


