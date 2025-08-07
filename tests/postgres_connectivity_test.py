# -------------------- Postgres table creation --------------------

##writing to postgres db
from venv import logger
import psycopg2
from psycopg2 import sql
#from jobs.dbaccess.setting_secrets import get_secret
#from jobs import dbaccess

import sys
print(sys.executable)

# Define your table schema
# https://github.com/digital-land/digital-land.info/blob/main/application/db/models.py - refered from here
table_name = "entity"
columns = {   
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

conn_params = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres"
}


# Create table if not exists
def create_table():
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        # Build CREATE TABLE SQL dynamically
        column_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        create_query = sql.SQL(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs});"
        )
        logger.info(f"create_table:Creating table {table_name} with columns: {columns.keys()}")

        cur.execute(create_query)
        conn.commit()
        logger.info(f"create_table:Table '{table_name}' created successfully (if it didn't exist).")

    except Exception as e:
        
        print(f"Connection failed: {e}")
        logger.error(f"create_table:Error creating table: {e}", exc_info=True)
    finally:
        if conn:
            cur.close()
            conn.close()

# Run the function
create_table()

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
#insert records into the table
import psycopg2
import json
from datetime import date

# Sample data to insert
sample_data = [
    {
        "dataset": "planning-applications",
        "end_date": date(2025, 12, 31),
        "entity": "entity-001",
        "entry_date": date(2025, 8, 6),
        "geojson": json.dumps({"type": "Feature", "geometry": None}),
        "geometry": None,
        "json": json.dumps({"status": "approved"}),
        "name": "Application A",
        "organisation_entity": 1001,
        "point": None,
        "prefix": "PA",
        "reference": "REF001",
        "start_date": date(2025, 1, 1),
        "typology": "residential"
    },
    {
        "dataset": "planning-applications",
        "end_date": date(2025, 11, 30),
        "entity": "entity-002",
        "entry_date": date(2025, 8, 6),
        "geojson": json.dumps({"type": "Feature", "geometry": None}),
        "geometry": None,
        "json": json.dumps({"status": "pending"}),
        "name": "Application B",
        "organisation_entity": 1002,
        "point": None,
        "prefix": "PA",
        "reference": "REF002",
        "start_date": date(2025, 2, 1),
        "typology": "commercial"
    },
    {
        "dataset": "planning-applications",
        "end_date": date(2025, 10, 15),
        "entity": "entity-003",
        "entry_date": date(2025, 8, 6),
        "geojson": json.dumps({"type": "Feature", "geometry": None}),
        "geometry": None,
        "json": json.dumps({"status": "rejected"}),
        "name": "Application C",
        "organisation_entity": 1003,
        "point": None,
        "prefix": "PA",
        "reference": "REF003",
        "start_date": date(2025, 3, 1),
        "typology": "industrial"
    },
    {
        "dataset": "planning-applications",
        "end_date": date(2025, 9, 30),
        "entity": "entity-004",
        "entry_date": date(2025, 8, 6),
        "geojson": json.dumps({"type": "Feature", "geometry": None}),
        "geometry": None,
        "json": json.dumps({"status": "under review"}),
        "name": "Application D",
        "organisation_entity": 1004,
        "point": None,
        "prefix": "PA",
        "reference": "REF004",
        "start_date": date(2025, 4, 1),
        "typology": "mixed-use"
    }
]

def insert_sample_data():
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Prepare insert query
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns.keys())})
        VALUES ({', '.join(['%s'] * len(columns))})
        """

        # Convert dicts to tuples in correct column order
        for record in sample_data:
            values = tuple(record[col] for col in columns.keys())
            cur.execute(insert_query, values)

        conn.commit()
        print("✅ Sample records inserted successfully.")
    except Exception as e:
        print(f"❌ Error inserting records: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

# Run the function
insert_sample_data()

