from pyspark.sql.functions import col, lit, to_json
from pyspark.sql.types import LongType


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

    for c in missing:
        default_val = defaults.get(c, None)
        df = df.withColumn(c, lit(default_val))

    return df


def write_dataframe_to_postgres_jdbc(df, table_name, data_set, env):
    logger.info("write_dataframe_to_postgres_jdbc: Show df:")
    show_df(df, 5, env)

    import hashlib

    import pg8000
    from pyspark.sql.types import LongType

    conn_params = get_aws_secret(env)
    row_count = df.count()
    logger.info(
        f"write_dataframe_to_postgres_jdbc: Writing {row_count:,} rows for {data_set}"
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_hash = hashlib.md5(data_set.encode()).hexdigest()[:8]
    staging_table = f"entity_staging_{dataset_hash}_{timestamp}"

    try:
        # --- Create staging table (unchanged) ---
        conn = pg8000.connect(**conn_params)
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE {staging_table} (
            entity BIGINT, name TEXT, entry_date DATE, start_date DATE, end_date DATE,
            dataset TEXT, json JSONB, organisation_entity BIGINT, prefix TEXT,
            reference TEXT, typology TEXT, geojson JSONB,
            geometry GEOMETRY(MULTIPOLYGON, 4326), point GEOMETRY(POINT, 4326), quality TEXT
        );
        """
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"write_dataframe_to_postgres_jdbc: Created {staging_table}")

        # --- Required columns as per staging schema ---
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

        # Optional defaults (you can keep it empty to insert pure NULLs)
        defaults = {
            # "quality": "unknown",  # example default instead of NULL
            # "json": "{}",          # if you prefer empty JSON rather than NULL
            # "geojson": None
        }

        # --- Ensure columns exist; add missing as NULL/defaults ---
        df = _ensure_required_columns(
            df, required_cols, defaults=defaults, logger=logger
        )

        # --- SAFE select + type normalisation ---
        df_typed = (
            df.select(*required_cols)
            .withColumn("entity", col("entity").cast(LongType()))
            .withColumn(
                "organisation_entity", col("organisation_entity").cast(LongType())
            )
        )

        # If your upstream provides complex (struct/map) types for json/geojson,
        # convert them to JSON strings prior to JDBC. If they're already strings, this is harmless.
        # (Postgres JSONB will accept text input; you're using `stringtype=unspecified`.)
        if "json" in df_typed.columns:
            df_typed = df_typed.withColumn("json", to_json(col("json")))
        if "geojson" in df_typed.columns:
            df_typed = df_typed.withColumn("geojson", to_json(col("geojson")))

        # NOTE: For 'geometry'/'point':
        # - If they are WKT strings, you may prefer inserting to text columns and
        #   casting on server side via ST_GeomFromText.
        # - If they are already suitable for PostGIS via the JDBC driver, leave as is.
        # If they are missing (NULL), PostGIS will accept NULL in the geometry columns.

        url = f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
        num_partitions = max(1, min(20, row_count // 50000))

        df_typed.repartition(num_partitions).write.jdbc(
            url=url,
            table=staging_table,
            mode="append",
            properties={
                "user": conn_params["user"],
                "password": conn_params["password"],
                "driver": "org.postgresql.Driver",
                "stringtype": "unspecified",  # lets text be coerced into JSONB server-side
                "batchsize": "5000",
                "reWriteBatchedInserts": "true",
                "numPartitions": str(num_partitions),
            },
        )
        logger.info(f"write_dataframe_to_postgres_jdbc: Wrote {row_count:,} rows")

        # --- Commit to production (unchanged) ---
        conn = pg8000.connect(**conn_params)
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '1800000';")
        cur.execute("DELETE FROM entity WHERE dataset = %s;", (data_set,))
        deleted = cur.rowcount
        cur.execute(f"INSERT INTO entity SELECT * FROM {staging_table};")
        inserted = cur.rowcount
        conn.commit()
        logger.info(
            f"write_dataframe_to_postgres_jdbc: Deleted {deleted:,}, inserted {inserted:,} rows"
        )
        cur.execute(f"DROP TABLE {staging_table};")
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"write_dataframe_to_postgres_jdbc: Failed: {e}")
        try:
            conn = pg8000.connect(**conn_params)
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
            conn.commit()
            cur.close()
            conn.close()
        except Exception:
            pass
        raise
