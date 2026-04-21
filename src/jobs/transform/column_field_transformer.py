"""Column field transformer."""

from datetime import datetime

from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

from jobs.config.schema import get_schema
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


class ColumnFieldTransformer:
    """Transform column field records by selecting required columns."""

    @staticmethod
    def transform(df, dataset):
        """Transform column field data."""
        logger.info("ColumnFieldTransformer: Transforming data for column_field table")

        df = df.withColumn("dataset", lit(dataset))
        df = get_schema("column_field").enforce(df)

        df = df.withColumn(
            "processed_timestamp",
            lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
        )

        logger.info(
            f"ColumnFieldTransformer: Transformation complete, columns: {df.columns}"
        )
        return df
