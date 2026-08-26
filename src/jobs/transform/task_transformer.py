"""Task transformer — generates task rows from log and issue DataFrames."""

import logging
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
    from_json,
    lit,
    lower,
    sha2,
    size,
    split,
    struct,
    substring,
    sum as spark_sum,
    to_json,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logger = logging.getLogger(__name__)


# NOTE: This module mirrors some of the task transform logic in digital-land-python:
# digital_land/pipeline/task.py (_transform_log_to_tasks, _transform_issues_to_tasks).
# transform_issues_to_tasks now intentionally diverges: it filters severity in
# (error, warning, notice) with no responsibility filter, explodes ';'-separated
# organisation values to one task row per organisation, and includes organisation
# in the reference hash. digital-land-python's task.py still uses
# severity=error/responsibility=external and a hash without organisation —
# bring these back in sync if/when that implementation is updated to match.


# Only the keys the bridge needs. from_json ignores everything else in the blob,
# so checks can keep their own extra keys without this having to change.
_EXPECTATION_DETAILS_SCHEMA = StructType(
    [
        StructField("error", StringType(), True),
        StructField("field", StringType(), True),
        StructField(
            "failures",
            ArrayType(
                StructType(
                    [
                        StructField("organisation_entity", StringType(), True),
                        StructField("count", LongType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)


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
        logger.warning(
            "transform_issues_to_tasks: no issue rows survived the severity "
            "filter — no issue tasks will be produced"
        )
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


def transform_expectations_to_tasks(
    df: DataFrame, org_df: DataFrame, entry_date: str = None
) -> DataFrame:
    """
    Transform an expectation DataFrame into task rows.

    Expects df to be the expectation parquet — one row per (dataset, check),
    with `details` as a JSON string. org_df maps organisation_entity to the
    organisation curie the task schema uses, and must carry both columns.

    Unlike issues, expectations are computed against the assembled dataset, so
    there is no resource or endpoint to attribute them to.
    """
    entry_date = entry_date or str(date.today())
    logger.info("transform_expectations_to_tasks: Starting")

    # `passed` is written as the string "True"/"False", not a boolean. Casting
    # and comparing as a string works for either shape, so this does not depend
    # on the writer's choice of type. (Spark coerces a bare `== False` correctly
    # too, but this avoids the E712 lint suppression that would need.)
    df = df.filter(lower(col("passed").cast("string")) == "false").filter(
        col("severity").isin("error", "warning", "notice")
    )

    parsed = df.withColumn(
        "_details", from_json(col("details"), _EXPECTATION_DETAILS_SCHEMA)
    )

    # A check that errored found nothing. It used to report an empty details
    # dict; since digital-land-python#587 it reports {"error": ...}, so both
    # shapes have to be dropped here.
    parsed = parsed.filter(col("_details").isNotNull() & col("_details.error").isNull())

    # No failures means nothing attributable to an organisation: either a check
    # predating the details standardisation, or duplicate_geometry_check where an
    # organisation's polygons only touch each other. Neither is a task.
    parsed = parsed.filter(
        col("_details.failures").isNotNull() & (size(col("_details.failures")) > 0)
    )

    if parsed.rdd.isEmpty():
        logger.warning(
            "transform_expectations_to_tasks: no expectation rows survived the "
            "filters — no expectation tasks will be produced"
        )
        return None

    exploded = (
        parsed.withColumn("_failure", explode(col("_details.failures")))
        .withColumn(
            "organisation_entity", col("_failure.organisation_entity").cast("string")
        )
        .withColumn("_failure_count", col("_failure.count"))
        .withColumn("field", coalesce(col("_details.field"), lit("")))
    )

    grouped = exploded.groupBy("dataset", "organisation_entity", "operation").agg(
        # Where a check supplies its own count, sum it — a duplicate-name failure
        # carries the number of entities sharing that name. Where it does not,
        # each failure is one offending entity, hence the 1.
        spark_sum(coalesce(col("_failure_count"), lit(1))).alias("count"),
        # How many failure records the organisation has, e.g. how many distinct
        # names are duplicated as against how many entities that affects.
        count("*").alias("groups"),
        first("severity").alias("severity"),
        first("responsibility").alias("responsibility"),
        first("field").alias("field"),
    )

    # organisation_entity is an entity number; the task schema wants the curie.
    # Inner join so a failure naming an unknown organisation is dropped rather
    # than becoming an unattributed task no one can see.
    org_df = org_df.select(
        col("organisation_entity").cast("string").alias("organisation_entity"),
        col("organisation"),
    )
    grouped = grouped.join(org_df, on="organisation_entity", how="inner")

    grouped = (
        grouped.withColumn(
            "details",
            to_json(
                struct(
                    # `operation`, deliberately not `issue_type`: submit's task
                    # list only renders tasks whose details carry issue_type and
                    # field, so this keeps expectation tasks out of that path.
                    col("operation"),
                    col("count").cast("int").alias("count"),
                    col("groups").cast("int").alias("groups"),
                    col("field"),
                )
            ),
        )
        .withColumn("endpoint", lit(""))
        .withColumn("resource", lit(""))
        .withColumn("task_source", lit("expectation"))
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
