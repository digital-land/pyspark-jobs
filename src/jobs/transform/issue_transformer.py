"""Issue transformer."""

from datetime import datetime

from pyspark.sql.functions import col, lit
from pyspark.sql.types import TimestampType

from jobs.config.schema import get_schema
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


def transform_issue(df, dataset):
    logger.info("transform_issue: Transforming data for Issue table")

    transf_df = (
        df.withColumn("start_date", lit("").cast("string"))
        .withColumn("entry_date", lit("").cast("string"))
        .withColumn("end_date", lit("").cast("string"))
    )

    transf_df = transf_df.filter(col("resource").isNotNull())
    transf_df = get_schema("issue").enforce(transf_df)

    transf_df = transf_df.withColumn(
        "processed_timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
    )

    logger.info(f"transform_issue: Complete, columns: {transf_df.columns}")
    return transf_df
