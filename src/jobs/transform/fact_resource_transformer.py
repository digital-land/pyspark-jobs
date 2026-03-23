"""Fact resource transformer for selecting and ordering fact resource columns."""

from datetime import datetime

from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


class FactResourceTransformer:
    """Transform fact resource records by selecting required columns."""

    @staticmethod
    def transform(df, dataset):
        """
        Transform fact resource data.

        Selects and orders the standard fact resource columns.
        """
        try:
            logger.info(
                "FactResourceTransformer: Transforming data for Fact Resource table"
            )

            # Select required columns in correct order
            df = df.select(
                "end_date",
                "fact",
                "entry_date",
                "entry_number",
                "priority",
                "resource",
                "start_date",
            )

            # add dataset column
            df = df.withColumn("dataset", lit(dataset))

            df = df.withColumn(
                "processed_timestamp",
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
            )

            logger.info(
                f"FactResourceTransformer: Transformation complete, columns: {df.columns}"
            )
            return df

        except Exception as e:
            logger.error(f"FactResourceTransformer: Error occurred - {e}")
            raise
