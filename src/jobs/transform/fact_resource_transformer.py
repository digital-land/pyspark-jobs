"""Fact resource transformer."""

import logging
from datetime import datetime

from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

from jobs.config.schema import get_schema

logger = logging.getLogger(__name__)


def transform_fact_resource(df, dataset):
    logger.info("transform_fact_resource: Transforming data for Fact Resource table")

    df = df.withColumn("dataset", lit(dataset))
    df = get_schema("fact_resource").enforce(df)

    df = df.withColumn(
        "processed_timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
    )

    logger.info(f"transform_fact_resource: Complete, columns: {df.columns}")
    return df
