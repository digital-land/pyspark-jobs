"""Task transformer — generates task rows from log and issue DataFrames."""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array_distinct,
    coalesce,
    col,
    concat_ws,
    count,
    explode,
    first,
    lit,
    sha2,
    split,
    struct,
    substring,
    to_json,
)

from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


# NOTE: This module mirrors some of the task transform logic in digital-land-python:
# digital_land/pipeline/task.py (_transform_log_to_tasks, _transform_issues_to_tasks).
# transform_issues_to_tasks now intentionally diverges: it filters severity in
# (error, warning, notice) with no responsibility filter, explodes ';'-separated
# organisation values to one task row per organisation, and includes organisation
# in the reference hash. digital-land-python's task.py still uses
# severity=error/responsibility=external and a hash without organisation —
# bring these back in sync if/when that implementation is updated to match.


def transform_log_to_tasks(df: DataFrame, entry_date: str = None) -> DataFrame:
    """
    Transform a log DataFrame into task rows.

    Expects df to already be joined with resource metadata so it has a
    dataset column. Only rows where status != 200 become tasks.
    """
    entry_date = entry_date or str(date.today())
    logger.info("transform_log_to_tasks: Starting")

    df = (
        df.filter(col("status") != "200")
        .select(
            "dataset", "organisation", "endpoint", "resource", "status", "exception"
        )
        .distinct()
    )

    if df.rdd.isEmpty():
        logger.info("transform_log_to_tasks: No failed log rows found")
        return None

    df = (
        df.withColumn(
            "details",
            to_json(
                struct(
                    col("status").cast("int").alias("status"),
                    coalesce(col("exception"), lit("")).alias("exception"),
                )
            ),
        )
        .withColumn("severity", lit("error"))
        .withColumn("responsibility", lit("external"))
        .withColumn("task_source", lit("log"))
        .withColumn("entry_date", lit(entry_date))
    )

    df = _add_reference(df)

    return df.select(
        col("dataset"),
        col("organisation"),
        col("endpoint"),
        col("resource"),
        col("details"),
        col("severity"),
        col("responsibility"),
        col("task_source"),
        col("entry_date"),
        col("reference"),
    )


def transform_issues_to_tasks(df: DataFrame, entry_date: str = None) -> DataFrame:
    """
    Transform an issue DataFrame into task rows.

    Expects df to already be filtered to active resources.
    """
    entry_date = entry_date or str(date.today())
    logger.info("transform_issues_to_tasks: Starting")

    df = df.filter(col("severity").isin("error", "warning", "notice"))

    if df.rdd.isEmpty():
        logger.info("transform_issues_to_tasks: No matching issue rows found")
        return None

    # array_distinct guards against an org appearing twice in the ';'-list,
    # which would otherwise double-count this issue for that org.
    df = df.withColumn(
        "organisation", explode(array_distinct(split(col("organisation"), ";")))
    )

    grouped = df.groupBy(
        "dataset", "resource", "field", "issue_type", "organisation"
    ).agg(
        count("*").alias("count"),
        first("severity").alias("severity"),
        first("responsibility").alias("responsibility"),
        first("endpoint").alias("endpoint"),
    )

    grouped = (
        grouped.withColumn(
            "details",
            to_json(
                struct(
                    coalesce(col("issue_type"), lit("")).alias("issue_type"),
                    col("count").cast("int").alias("count"),
                    coalesce(col("field"), lit("")).alias("field"),
                )
            ),
        )
        .withColumn("task_source", lit("issue"))
        .withColumn("entry_date", lit(entry_date))
    )

    grouped = _add_reference(grouped)

    return grouped.select(
        col("dataset"),
        col("organisation"),
        col("endpoint"),
        col("resource"),
        col("details"),
        col("severity"),
        col("responsibility"),
        col("task_source"),
        col("entry_date"),
        col("reference"),
    )


def _add_reference(df: DataFrame) -> DataFrame:
    """
    Adds a `reference` column: a 16-char hex digest of dataset, organisation,
    endpoint, resource, task_source and details. organisation is included so
    that exploded per-organisation issue task rows.
    """
    return df.withColumn(
        "reference",
        substring(
            sha2(
                concat_ws(
                    "|",
                    coalesce(col("dataset"), lit("")),
                    coalesce(col("organisation"), lit("")),
                    coalesce(col("endpoint"), lit("")),
                    coalesce(col("resource"), lit("")),
                    col("task_source"),
                    col("details"),
                ),
                256,
            ),
            1,
            16,
        ),
    )
