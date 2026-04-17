"""Issue transformer for adding date columns and selecting issue fields."""

from datetime import datetime

from pyspark.sql.functions import col, lit
from pyspark.sql.types import TimestampType

from jobs.config.schema import get_schema
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


class IssueTransformer:
    """Transform issue records by adding date columns and selecting fields."""

    @staticmethod
    def transform(df, dataset):
        """
        Transform issue data.

        Adds empty date columns (start_date, entry_date, end_date) and
        enforces the issue schema, adding missing fields as typed nulls.
        """
        try:
            logger.info("IssueTransformer: Transforming data for Issue table")

            # Add empty date columns
            transf_df = (
                df.withColumn("start_date", lit("").cast("string"))
                .withColumn("entry_date", lit("").cast("string"))
                .withColumn("end_date", lit("").cast("string"))
            )

            # Remove rows with no resource — these cannot be attributed to a source file
            transf_df = transf_df.filter(col("resource").isNotNull())

            # Enforce schema: adds missing fields as typed nulls, selects in order
            transf_df = get_schema("issue").enforce(transf_df)

            logger.info(
                f"IssueTransformer: Transformation complete, columns: {transf_df.columns}"
            )

            transf_df = transf_df.withColumn(
                "processed_timestamp",
                lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
            )

            return transf_df

        except Exception as e:
            logger.error(f"IssueTransformer: Error occurred - {e}")
            raise
