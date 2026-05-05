"""Column field transformer."""

from datetime import datetime

from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

from jobs.config.schema import get_schema
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


def transform_column_field(df, dataset):
    logger.info("transform_column_field: Transforming data for column_field table")

    df = df.withColumn("dataset", lit(dataset))
    df = get_schema("column_field").enforce(df)

    df = df.withColumn(
        "processed_timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
    )

    logger.info(f"transform_column_field: Complete, columns: {df.columns}")
    return df
