# -------------------- Postgres table creation --------------------

##writing to postgres db
import json
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)
import pg8000
from jobs.utils.aws_secrets_manager import get_secret_emr_compatible

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
            "timeout": 30,  # Connection timeout in seconds
            "tcp_keepalives_idle": 600,  # Keep connection alive
            "tcp_keepalives_interval": 30,
            "tcp_keepalives_count": 3
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

# -------------------- PostgreSQL Writer --------------------

##writing to postgres db
def write_to_postgres(df, config):
    
    config = {
        "TABLE_NAME": "entity",
        "PG_JDBC": "jdbc",
        "PG_URL": "jdbc:postgresql://localhost:5432/test_db",
        "USER_NAME": "test_user",
        "PASSWORD": "test_password",
        "DRIVER": "org.postgresql.Driver"
    }
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

