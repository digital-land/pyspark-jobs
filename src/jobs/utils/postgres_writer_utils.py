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
from jobs.utils.df_utils import show_df
from pyspark.sql.functions import col, lit, to_json

logger = get_logger(__name__)

def _ensure_required_columns(df, required_cols, defaults=None, logger=None):
    """
    Ensures all required columns exist in df. Adds missing columns as NULL (or default values).
    Args:
        df: Spark DataFrame
        required_cols: list[str] of column names expected downstream
        defaults: dict[str, any] optional default values per missing column
        logger: optional logger
    Returns:
        DataFrame with all required columns present
    """
    defaults = defaults or {}
    existing = set(df.columns)

    missing = [c for c in required_cols if c not in existing]
    extra   = [c for c in df.columns if c not in set(required_cols)]

    if logger:
        if missing:
            logger.warning(f"PG writer: Missing columns will be set to NULL/defaults: {missing}")
        if extra:
            logger.info(f"PG writer: Extra columns present (ignored by select): {extra}")

    for c in missing:
        default_val = defaults.get(c, None)
       
        # Safe type casting for JDBC
        if c in ["entity", "organisation_entity"]:
            df = df.withColumn(c, lit(default_val).cast("bigint"))
        elif c in ["json", "geojson", "geometry", "point", "quality", "name", "prefix", "reference", "typology", "dataset"]:
            df = df.withColumn(c, lit(default_val).cast("string"))
        elif c in ["entry_date", "start_date", "end_date"]:
            df = df.withColumn(c, lit(default_val).cast("date"))
        else:
            df = df.withColumn(c, lit(default_val).cast("string"))

    return df


def write_dataframe_to_postgres_jdbc(df, table_name, data_set, env):
    """
    Write DataFrame to PostgreSQL using staging table and atomic transaction with retry logic.
    JDBC write and transaction block are retried on transient failures.
    """
    import time
    import hashlib
    from datetime import datetime
    import pg8000
    from pyspark.sql.functions import col
    from pyspark.sql.types import LongType

    logger.info("write_dataframe_to_postgres_jdbc: Show df:")
    show_df(df, 5, env)

    conn_params = get_aws_secret(env)
    row_count = df.count()
    logger.info(f"write_dataframe_to_postgres_jdbc: Writing {row_count:,} rows for {data_set}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_hash = hashlib.md5(data_set.encode()).hexdigest()[:8]
    staging_table = f"entity_staging_{dataset_hash}_{timestamp}"

    # Step 1: Create staging table
    try:
        conn_params["timeout"] = 1800  # 30 minutes
        conn = pg8000.connect(**conn_params)

        cur = conn.cursor()
        cur.execute(f"""
        CREATE TABLE {staging_table} (
            entity BIGINT, name TEXT, entry_date DATE, start_date DATE, end_date DATE,
            dataset TEXT, json JSONB, organisation_entity BIGINT, prefix TEXT,
            reference TEXT, typology TEXT, geojson JSONB,
            geometry GEOMETRY(MULTIPOLYGON, 4326), point GEOMETRY(POINT, 4326), quality TEXT
        );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"write_dataframe_to_postgres_jdbc: Created {staging_table}")
    except Exception as e:
        logger.error(f"Failed to create staging table: {e}")
        raise

    # Step 2: Prepare DataFrame
    required_cols = [
        "entity", "name", "entry_date", "start_date", "end_date", "dataset",
        "json", "organisation_entity", "prefix", "reference", "typology",
        "geojson", "geometry", "point", "quality"
    ]

    df_typed = _ensure_required_columns(df, required_cols, defaults=None, logger=logger)
    df_typed = df_typed.select(*required_cols) \
        .withColumn("entity", col("entity").cast(LongType())) \
        .withColumn("organisation_entity", col("organisation_entity").cast(LongType()))

    url = f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    num_partitions = max(1, min(20, row_count // 50000))

    # Retry logic for JDBC write
    max_attempts = 3
    attempt = 0
    while attempt < max_attempts:
        try:
            df_typed.repartition(num_partitions).write.jdbc(
                url=url,
                table=staging_table,
                mode="append",
                properties={
                    "user": conn_params["user"],
                    "password": conn_params["password"],
                    "driver": "org.postgresql.Driver",
                    "stringtype": "unspecified",
                    "batchsize": "5000",
                    "reWriteBatchedInserts": "true",
                    "numPartitions": str(num_partitions),
                },
            )
            logger.info(f"write_dataframe_to_postgres_jdbc: JDBC write successful on attempt {attempt+1}")
            break
        except Exception as e:
            attempt += 1
            logger.warning(f"JDBC write failed (attempt {attempt}): {e}")
            if attempt < max_attempts:
                sleep_time = 5 * attempt
                logger.info(f"Retrying JDBC write in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                logger.error("Max retries reached for JDBC write. Failing job.")
                raise

    # Retry logic for atomic transaction
    attempt = 0
    while attempt < max_attempts:
        try:
            conn_params["timeout"] = 1800  # 30 minutes
            conn = pg8000.connect(**conn_params)

            cur = conn.cursor()
            cur.execute("SET statement_timeout = '1800000';")
            cur.execute("BEGIN;")
            cur.execute("DELETE FROM entity WHERE dataset = %s;", (data_set,))
            deleted = cur.rowcount
            cur.execute(f"INSERT INTO entity SELECT * FROM {staging_table};")
            inserted = cur.rowcount
            cur.execute(f"DROP TABLE {staging_table};")
            cur.execute("COMMIT;")
            logger.info(f"Atomic commit successful on attempt {attempt+1} - Deleted {deleted:,}, Inserted {inserted:,} rows")
            break
        except Exception as e:
            cur.execute("ROLLBACK;")
            attempt += 1
            logger.warning(f"Atomic commit failed (attempt {attempt}): {e}")
            if attempt < max_attempts:
                sleep_time = 5 * attempt
                logger.info(f"Retrying atomic commit in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                logger.error("Max retries reached for atomic commit. Failing job.")
                raise
        finally:
            logger.info(f"write_dataframe_to_postgres_jdbc: Finally drop staging table if it still exists")
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
            conn.commit()
            cur.close()
            conn.close()