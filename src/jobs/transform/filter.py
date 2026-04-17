from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


def filter_old_resources(df: DataFrame, old_resources_df: DataFrame) -> DataFrame:
    """
    Remove rows from df whose resource matches a gone (status 410) old resource,
    and remove rows with a null resource.

    Args:
        df: DataFrame to filter (must contain a 'resource' column)
        old_resources_df: DataFrame from read_old_resources (must contain 'resource'
                          and 'status' columns)

    Returns:
        Filtered DataFrame
    """
    gone_hashes = [
        row[0]
        for row in old_resources_df.filter(col("status") == "410")
        .select("resource")
        .collect()
    ]

    if gone_hashes:
        logger.info(
            f"filter_old_resources: Filtering out {len(gone_hashes)} resources with status 410"
        )

    return df.filter(~col("resource").isin(gone_hashes))
