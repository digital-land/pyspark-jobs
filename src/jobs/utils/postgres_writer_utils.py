import time
from datetime import datetime

from pyspark.sql.functions import col, lit, to_json

from jobs.csv_s3_writer import (
    cleanup_temp_csv_files,
    import_csv_to_aurora,
    write_dataframe_to_csv_s3,
)
from jobs.dbaccess.postgres_connectivity import (
    ENTITY_TABLE_NAME,
    calculate_centroid_wkt,
    commit_staging_to_production,
    create_and_prepare_staging_table,
    get_aws_secret,
    write_to_postgres,
)
from jobs.utils.df_utils import show_df
from jobs.utils.logger_config import get_logger, log_execution_time

logger = get_logger(__name__)


def _ensure_required_columns(df, required_cols, defaults=None, logger=None):
    """
    Ensures all required columns exist in df. Adds missing columns as NULL (or default values).
    Also normalises types for existing columns to expected types where applicable.

    Args:
        df: Spark DataFrame
        required_cols: list[str] of column names expected downstream
        defaults: dict[str, any] optional default values per missing column
        logger: optional logger
    Returns:
        DataFrame with all required columns present and types normalised
    """
    defaults = defaults or {}
    existing = set(df.columns)
    missing = [c for c in required_cols if c not in existing]
    extra = [c for c in df.columns if c not in set(required_cols)]

    if logger:
        if missing:
            logger.warning(
                f"PG writer: Missing columns will be set to NULL/defaults: {missing}"
            )
        if extra:
            logger.info(
                f"PG writer: Extra columns present (ignored by select): {extra}"
            )

    # Add missing columns with appropriate types
    for c in missing:
        default_val = defaults.get(c, None)
        if c in ["entity", "organisation_entity"]:
            df = df.withColumn(c, lit(default_val).cast("bigint"))
        elif c in [
            "json",
            "geojson",
            "geometry",
            "point",
            "quality",
            "name",
            "prefix",
            "reference",
            "typology",
            "dataset",
        ]:
            df = df.withColumn(c, lit(default_val).cast("string"))
        elif c in ["entry_date", "start_date", "end_date"]:
            df = df.withColumn(c, lit(default_val).cast("date"))
        else:
            df = df.withColumn(c, lit(default_val).cast("string"))

    # Normalise types for existing columns (not only missing ones)
    from pyspark.sql.types import DateType, LongType

    if "entity" in existing:
        df = df.withColumn("entity", col("entity").cast(LongType()))
    if "organisation_entity" in existing:
        df = df.withColumn(
            "organisation_entity", col("organisation_entity").cast(LongType())
        )

    for c in ["entry_date", "start_date", "end_date"]:
        if c in existing:
            df = df.withColumn(c, col(c).cast(DateType()))

    # If json/geojson are structs, serialise to JSON strings for JDBC; later cast to JSONB in SQL
    for c in ["json", "geojson"]:
        if c in existing:
            df = df.withColumn(c, to_json(col(c)))

    # Ensure text - like fields are strings
    for c in ["name", "dataset", "prefix", "reference", "typology", "quality"]:
        if c in existing:
            df = df.withColumn(c, col(c).cast("string"))

    return df


def write_dataframe_to_postgres_jdbc(df, table_name, data_set, env):
    """
    Write DataFrame to PostgreSQL using staging table and atomic transaction with retry logic.
    JDBC write and transaction block are retried on transient failures.
    """
    import hashlib

    import pg8000
    from pyspark.sql.types import LongType

    logger.info("write_dataframe_to_postgres_jdbc: Show df:")
    show_df(df, 5, env)

    conn_params = get_aws_secret(env)
    row_count = df.count()
    logger.info(
        f"write_dataframe_to_postgres_jdbc: Writing {row_count:,} rows for {data_set}"
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_hash = hashlib.md5(data_set.encode()).hexdigest()[:8]
    staging_table = f"entity_staging_{dataset_hash}_{timestamp}"

    # Step 1: Create staging table with explicit types
    try:
        conn_params["timeout"] = 1800  # 30 minutes
        conn = pg8000.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE {staging_table} (
            entity BIGINT,
            name TEXT,
            entry_date DATE,
            start_date DATE,
            end_date DATE,
            dataset TEXT,
            json JSONB,
            organisation_entity BIGINT,
            prefix TEXT,
            reference TEXT,
            typology TEXT,
            geojson JSONB,
            geometry GEOMETRY(MULTIPOLYGON, 4326),
            point GEOMETRY(POINT, 4326),
            quality TEXT
        );
        """
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"write_dataframe_to_postgres_jdbc: Created {staging_table}")
    except Exception as e:
        logger.error(f"Failed to create staging table: {e}")
        raise

    # Step 2: Prepare DataFrame (enforce required cols and normalise)
    required_cols = [
        "entity",
        "name",
        "entry_date",
        "start_date",
        "end_date",
        "dataset",
        "json",
        "organisation_entity",
        "prefix",
        "reference",
        "typology",
        "geojson",
        "geometry",
        "point",
        "quality",
    ]

    df_typed = _ensure_required_columns(df, required_cols, defaults=None, logger=logger)
    df_typed = (
        df_typed.select(*required_cols)
        .withColumn("entity", col("entity").cast(LongType()))
        .withColumn("organisation_entity", col("organisation_entity").cast(LongType()))
    )

    # Step 3: JDBC write with retry
    url = f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    num_partitions = max(1, min(20, row_count // 50000))

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
            logger.info(
                f"write_dataframe_to_postgres_jdbc: JDBC write successful on attempt {attempt + 1}"
            )
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

    # Step 4: Atomic transaction with explicit column list and casts
    attempt = 0
    while attempt < max_attempts:
        try:
            conn_params["timeout"] = 1800  # 30 minutes
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            cur.execute("SET statement_timeout = '1800000';")
            cur.execute("BEGIN;")

            # Delete existing dataset rows
            cur.execute("DELETE FROM entity WHERE dataset = %s;", (data_set,))
            deleted = cur.rowcount

            # Insert using explicit column mapping to avoid positional mismatches
            cur.execute(
                """
            INSERT INTO entity (
                entity, name, entry_date, start_date, end_date,
                dataset, json, organisation_entity, prefix, reference,
                typology, geojson, geometry, point, quality
            )
            SELECT
                entity,
                name,
                entry_date::date,
                start_date::date,
                end_date::date,
                dataset::text,
                json::jsonb,
                organisation_entity::bigint,
                prefix::text,
                reference::text,
                typology::text,
                geojson::jsonb,
                geometry,
                point,
                quality::text
            FROM {staging_table};
            """
            )
            inserted = cur.rowcount

            # Drop staging and commit
            cur.execute(f"DROP TABLE {staging_table};")
            cur.execute("COMMIT;")
            logger.info(
                f"Atomic commit successful on attempt {attempt + 1} - Deleted {deleted:,}, Inserted {inserted:,} rows"
            )
            break
        except Exception as e:
            try:
                cur.execute("ROLLBACK;")
            except Exception:
                pass
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
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # Step 5: Safety cleanup - drop staging if it still exists
    try:
        logger.info(
            "write_dataframe_to_postgres_jdbc: Finally drop staging table if it still exists"
        )
        conn = pg8000.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Cleanup drop staging table failed or not needed: {e}")
