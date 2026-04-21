"""Dataset resource transformer."""

from datetime import datetime

from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

from jobs.config.schema import get_schema
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


class DatasetResourceTransformer:
    """Transform dataset resource records by selecting required columns."""

    @staticmethod
    def transform(df, dataset):
        """Transform dataset resource data."""
        logger.info(
            "DatasetResourceTransformer: Transforming data for dataset_resource table"
        )

        df = df.withColumn("dataset", lit(dataset))
        df = get_schema("dataset_resource").enforce(df)

        df = df.withColumn(
            "processed_timestamp",
            lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
        )

        logger.info(
            f"DatasetResourceTransformer: Transformation complete, columns: {df.columns}"
        )
        return df
