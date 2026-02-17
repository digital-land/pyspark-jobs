import logging

from pyspark.sql import SparkSession

from jobs.utils.logger_config import log_execution_time

logger = logging.getLogger(__name__)


@log_execution_time
def create_spark_session(app_name="EMR Transform Job"):
    try:
        logger.info(f"Creating Spark session with app name: {app_name}")

        # Configure Spark for EMR Serverless 7.9.0 (Spark 3.5.x, Java 17)
        # PostgreSQL JDBC driver is configured via --jars in Airflow DAG sparkSubmitParameters
        spark_session = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        # Set Spark logging level to reduce verbosity
        # set_spark_log_level("WARN")

        return spark_session

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}", exc_info=True)
        return None
