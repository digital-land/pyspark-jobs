"""Task transformer — generates task rows from log and issue DataFrames."""

import json
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    first,
    lit,
    sha2,
    substring,
    udf,
)
from pyspark.sql.types import StringType

from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


# NOTE: This module mirrors the task transform logic in digital-land-python:
# digital_land/pipeline/task.py (_transform_log_to_tasks, _transform_issues_to_tasks).
# The two implementations use different frameworks (PySpark vs Polars) but must
# produce identical output schemas and reference hashes for the same input data.
# If you change filtering logic, grouping, details JSON structure, or the reference
# hash inputs here, make the equivalent change there too (and vice versa).


def transform_log_to_tasks(df: DataFrame, entry_date: str = None) -> DataFrame:
    """
    Transform a log DataFrame into task rows.

    Expects df to already be joined with resource metadata so it has a
    dataset column. Only rows where status != 200 become tasks.
    """
    entry_date = entry_date or str(date.today())
    logger.info("transform_log_to_tasks: Starting")

    df = df.filter(col("status") != "200")

    if df.rdd.isEmpty():
        logger.info("transform_log_to_tasks: No failed log rows found")
        return None

    df = (
        df.withColumn("details", _log_details(col("status"), col("exception")))
        .withColumn("organisation", lit(""))
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
        col("task_source").alias("task-source"),
        col("entry_date").alias("entry-date"),
        col("reference"),
    )


def transform_issues_to_tasks(df: DataFrame, entry_date: str = None) -> DataFrame:
    """
    Transform an issue DataFrame into task rows.

    Expects df to already be filtered to active resources.
    Filters to severity=error and responsibility=external, then groups
    by dataset/resource/field/issue-type and counts rows.
    """
    entry_date = entry_date or str(date.today())
    logger.info("transform_issues_to_tasks: Starting")

    df = df.filter((col("severity") == "error") & (col("responsibility") == "external"))

    if df.rdd.isEmpty():
        logger.info("transform_issues_to_tasks: No matching issue rows found")
        return None

    grouped = df.groupBy("dataset", "resource", "field", "issue_type").agg(
        count("*").alias("count"),
        first("severity").alias("severity"),
        first("responsibility").alias("responsibility"),
    )

    grouped = (
        grouped.withColumn(
            "details",
            _issue_details(col("issue_type"), col("count"), col("field")),
        )
        .withColumn("organisation", lit(""))
        .withColumn("endpoint", lit(""))
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
        col("task_source").alias("task-source"),
        col("entry_date").alias("entry-date"),
        col("reference"),
    )


def _log_details_udf(status, exception):
    """Build JSON details string for a log task."""
    if status and str(status).isdigit():
        status_val = int(status)
    else:
        status_val = status
    return json.dumps({"status": status_val, "exception": exception or ""})


_log_details = udf(_log_details_udf, StringType())


def _issue_details_udf(issue_type, count_val, field):
    """Build JSON details string for an issue task."""
    return json.dumps(
        {
            "issue_type": issue_type or "",
            "count": int(count_val) if count_val else 0,
            "field": field or "",
        }
    )


_issue_details = udf(_issue_details_udf, StringType())


def _add_reference(df: DataFrame) -> DataFrame:
    """Add reference column: first 16 chars of SHA-256 hash of key fields."""
    return df.withColumn(
        "reference",
        substring(
            sha2(
                concat_ws(
                    "|",
                    col("dataset"),
                    col("endpoint"),
                    col("resource"),
                    col("task_source"),
                    col("details"),
                ),
                256,
            ),
            1,
            16,
        ),
    )
