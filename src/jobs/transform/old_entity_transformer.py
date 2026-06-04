"""Old entity transformer."""

import io
from datetime import datetime

import pandas as pd
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import TimestampType

from jobs.config.schema import get_schema
from jobs.utils.df_utils import normalise_column_names
from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)

_DATASET_SPEC_URL = "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv"


def fetch_dataset_df(spark: SparkSession) -> DataFrame:
    """
    Fetch the dataset specification CSV from the digital-land specification repository.

    Returns a Spark DataFrame with columns: dataset, collection, entity_minimum, entity_maximum.
    Cache the result when calling multiple times in the same job to avoid repeated HTTP requests.
    """
    logger.info(f"fetch_dataset_df: Fetching from {_DATASET_SPEC_URL}")
    response = requests.get(_DATASET_SPEC_URL, timeout=30)
    response.raise_for_status()
    dataset_df = spark.createDataFrame(pd.read_csv(io.StringIO(response.text)))
    dataset_df = normalise_column_names(dataset_df)
    return dataset_df.select(
        "dataset", "collection", "entity_minimum", "entity_maximum"
    )


def transform_old_entity(old_entity_df: DataFrame, dataset_df: DataFrame) -> DataFrame:
    """
    Join old entity records with the dataset spec to derive the dataset column,
    then filter to keep only records where the collection matches the spec.

    Args:
        old_entity_df: Unioned DataFrame of old-entity.csv records with a collection column.
            Column names may use hyphens (old-entity, end-date) — they are normalised internally.
        dataset_df: DataFrame from fetch_dataset_df.
    """
    # Normalise column names from CSV kebab-case to snake_case (old-entity → old_entity, etc.)
    old_entity_df = normalise_column_names(old_entity_df)

    # Rename spec collection to avoid collision with old_entity_df's collection column
    spec_df = dataset_df.withColumnRenamed("collection", "spec_collection")

    # Range join: find the dataset whose entity range contains the old_entity value
    result = old_entity_df.join(
        spec_df,
        (col("old_entity").cast("long") >= col("entity_minimum").cast("long"))
        & (col("old_entity").cast("long") <= col("entity_maximum").cast("long")),
        "left",
    )

    # Keep only rows where the collection from the path matches the spec collection
    result = result.filter(col("collection") == col("spec_collection"))

    # Redirects can only be within the same dataset — the target entity must fall in the same
    # entity range as old_entity. Null entity (e.g. status=410 "gone") is always kept.
    result = result.filter(
        col("entity").isNull()
        | (
            (col("entity").cast("long") >= col("entity_minimum").cast("long"))
            & (col("entity").cast("long") <= col("entity_maximum").cast("long"))
        )
    )

    result = get_schema("old_entity").enforce(result)

    result = result.withColumn(
        "processed_timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()),
    )

    logger.info(f"transform_old_entity: Complete, columns: {result.columns}")
    return result
