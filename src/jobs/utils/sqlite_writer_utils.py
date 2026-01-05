from jobs.utils.logger_config import get_logger, log_execution_time

logger = get_logger(__name__)


# -------------------- SQLite Writer --------------------
@log_execution_time
def generate_sqlite(df):
    # Step 4: Write to SQLite
    # Write to SQLite using JDBC
    try:
        # Output SQLite DB to Desktop
        # Note: This is commented out as it's not used in production
        # from utils.path_utils import resolve_desktop_path
        # sqlite_path = resolve_desktop_path("../MHCLG/tgt - data/sqlite - output/transport_access_node.db")
        sqlite_path = "/tmp/transport_access_node.db"  # Temporary path for development

        df.write.format("jdbc").option("url", f"jdbc:sqlite:{sqlite_path}").option(
            "dbtable", "fact_resource"
        ).option("driver", "org.sqlite.JDBC").mode("overwrite").save()
        logger.info("sqlite data inserted successfully")

    except Exception as e:
        logger.error(f"Failed to write to SQLite: {e}", exc_info=True)
        raise
