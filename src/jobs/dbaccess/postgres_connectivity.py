# -------------------- Postgres table creation --------------------

##writing to postgres db
from venv import logger
import psycopg2
from psycopg2 import sql
from jobs.utils.aws_secrets_manager import get_secret

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
    aws_secrets_json = get_secret("dev/pyspark/postgres")
    
    # Parse the JSON string
    secrets = json.loads(aws_secrets_json)

    # Extract required fields
    username = secrets.get("username")
    password = secrets.get("password")
    dbName = secrets.get("dbName")
    host = secrets.get("host")
    port = secrets.get("port")
    print(f"get_aws_secret:Retrieved secrets for {dbName} at {host}:{port} with user {username}")
    conn_params ={
        "dbname": dbName,
        "host": host,
        "port": port,
        "user": username,
        "password": password
    }
    return conn_params


# Create table if not exists
def create_table(conn_params):
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        # Build CREATE TABLE SQL dynamically
        column_defs = ", ".join([f"{col} {dtype}" for col, dtype in pyspark_entity_columns.items()])
        create_query = sql.SQL(
            f"CREATE TABLE IF NOT EXISTS {dbtable_name} ({column_defs});"
        )
        logger.info(f"create_table:Creating table {dbtable_name} with columns: {pyspark_entity_columns.keys()}")

        cur.execute(create_query)
        conn.commit()
        logger.info(f"create_table:Table '{dbtable_name}' created successfully (if it didn't exist).")

    except Exception as e:
        logger.error(f"create_table:Error creating table: {e}", exc_info=True)
    finally:
        if conn:
            cur.close()
            conn.close()

# Run the function
create_table(get_aws_secret())

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

