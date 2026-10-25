import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col, split

logger = logging.getLogger(__name__)


def filter_old_resources(df: DataFrame, old_resources_df: DataFrame) -> DataFrame:
    """
    Remove rows from df whose resource matches a gone (status 410) old resource.

    Args:
        df: DataFrame to filter (must contain a 'resource' column)
        old_resources_df: DataFrame from read_old_resources (must contain 'old_resource'
                          and 'status' columns)

    Returns:
        Filtered DataFrame
    """
    gone_hashes = [
        row[0]
        for row in old_resources_df.filter(col("status") == "410")
        .select("old_resource")
        .collect()
    ]

    if gone_hashes:
        logger.info(
            f"filter_old_resources: Filtering out {len(gone_hashes)} resources with status 410"
        )

    return df.filter(col("resource").isNull() | ~col("resource").isin(gone_hashes))


def filter_dataset_resources(
    df: DataFrame, resources_df: DataFrame, dataset: str
) -> DataFrame:
    """
    Keep only rows whose resource is listed for dataset in resource.csv.

    transformed/{dataset}/ can still hold files for resources the collection no
    longer assembles into dataset, such as a resource whose endpoint has moved to
    another dataset in source.csv. resource.csv's `datasets` column is rebuilt
    from source.csv on every collection run, so this keeps the same resources as
    digital-land-python's Collection.dataset_resource_map.

    Args:
        df: DataFrame to filter (must contain a 'resource' column)
        resources_df: DataFrame from read_resources (must contain 'resource' and
                      'datasets' columns)
        dataset: Dataset being assembled

    Returns:
        Filtered DataFrame
    """
    dataset_resources = resources_df.filter(
        array_contains(split(col("datasets"), ";"), dataset)
    ).select("resource")

    unlisted = [
        row[0]
        for row in df.select("resource")
        .distinct()
        .join(dataset_resources, on="resource", how="left_anti")
        .collect()
    ]
    if unlisted:
        logger.warning(
            f"filter_dataset_resources: Filtering out {len(unlisted)} resources "
            f"not listed for {dataset} in resource.csv: {sorted(unlisted)}"
        )

    return df.join(dataset_resources, on="resource", how="left_semi")
