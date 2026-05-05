"""Fact transformer."""

from datetime import datetime

from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

from jobs.config.schema import get_schema
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


def transform_fact(df, dataset):
    logger.info("transform_fact: Transforming data for Fact table")

    window_spec = (
        Window.partitionBy("fact")
        .orderBy("priority", "entry_date", "entry_number")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    transf_df = (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    transf_df = transf_df.withColumn("dataset", lit(dataset))
    transf_df = get_schema("fact").enforce(transf_df)

    transf_df = transf_df.withColumn(
        "processed_timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
    )

    logger.info(f"transform_fact: Complete, columns: {transf_df.columns}")
    return transf_df
