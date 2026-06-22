import logging

from pyspark.sql import DataFrame, SparkSession

from jobs.utils.df_utils import normalise_column_names

logger = logging.getLogger(__name__)


def read_old_resources(spark: SparkSession, path: str) -> DataFrame:
    """
    Read the old-resource CSV file into a DataFrame.

    Expected columns: resource, status (and any others present in the file).

    Args:
        spark: Active Spark session
        path: Path to the old-resource.csv file (local or S3)

    Returns:
        DataFrame with old resource records
    """
    logger.info(f"read_old_resources: Reading old resources from {path}")
    df = spark.read.option("header", "true").csv(path)
    return normalise_column_names(df)
