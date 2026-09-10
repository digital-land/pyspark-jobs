"""TaskPipeline: cross-collection task generation from log and issue files."""

import csv
import logging
import urllib.request
from functools import reduce

from cloudpathlib import AnyPath
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    count,
    explode,
    lit,
    lower,
    row_number,
    split,
    when,
)

from jobs.config.authoritativeness_task import RESTRICT_TO_ACTIVE_ORGS
from jobs.pipeline.authority import load_entity_quality, non_authoritative_providers
from jobs.pipeline.base import BasePipeline
from jobs.read import read_csvs_by_name, read_issue_csvs
from jobs.transform.task_transformer import (
    TASK_SEVERITIES,
    transform_authority_to_tasks,
    transform_expectations_to_tasks,
    transform_issues_to_tasks,
    transform_log_to_tasks,
)
from jobs.utils.collection_paths import (
    collection_files,
    collection_names,
    expectation_files,
    issue_files_for_resources,
)
from jobs.utils.df_utils import normalise_column_names
from jobs.utils.postgres_writer_utils import write_task_to_postgres

logger = logging.getLogger(__name__)

ISSUE_TYPE_URL = "https://raw.githubusercontent.com/digital-land/specification/main/content/issue-type.csv"


def _load_issue_type_df(spark):
    with urllib.request.urlopen(ISSUE_TYPE_URL) as response:
        lines = [line.decode("utf-8") for line in response.readlines()]
        reader = csv.DictReader(lines)
        # .get on quality_dimension: it is fetched from the specification at runtime,
        # so a column rename upstream should degrade to unmapped tasks and a loud log
        # line rather than killing the nightly task build outright.
        rows = [
            (
                row["issue-type"],
                row["severity"],
                row["responsibility"],
                row.get("quality_dimension") or "",
            )
            for row in reader
        ]
    return spark.createDataFrame(
        rows, ["issue_type", "severity", "responsibility", "quality_dimension"]
    )


def _backfill_dataset_from_source(log_df, endpoint_dataset_df):
    """
    Fill in missing dataset values for failed log entries using the endpoint → source lookup.

    Rows with dataset already set are left unchanged. Rows with dataset="" are joined
    against endpoint_dataset_df; if an endpoint serves multiple datasets the row expands
    to one row per dataset.
    """
    missing_df = log_df.filter(col("dataset") == "")
    resolved_df = (
        missing_df.drop("dataset")
        .join(endpoint_dataset_df, on="endpoint", how="left")
        .fillna("", subset=["dataset"])
    )
    return log_df.filter(col("dataset") != "").unionByName(resolved_df)


def _backfill_organisation_from_source(log_df, endpoint_organisation_df):
    """
    Fill in missing organisation values for failed log entries using the endpoint → source lookup.

    Mirrors _backfill_dataset_from_source: rows with organisation already set are left
    unchanged, rows with organisation="" are joined against endpoint_organisation_df.
    """
    missing_df = log_df.filter(col("organisation") == "")
    resolved_df = (
        missing_df.drop("organisation")
        .join(endpoint_organisation_df, on="endpoint", how="left")
        .fillna("", subset=["organisation"])
    )
    return log_df.filter(col("organisation") != "").unionByName(resolved_df)


def _active_resources_from_log(log_df, endpoint_attrs_df):
    """The resource each endpoint is currently serving, derived from the log.

    dataset/organisation are attributed from source.csv (endpoint_attrs_df),
    keyed on that endpoint, rather than from resource.csv's flattened sets.
    """
    latest_per_endpoint = Window.partitionBy("endpoint").orderBy(
        col("entry_date").desc(), col("resource").asc()
    )
    current = (
        log_df.filter(
            (col("status") == "200")
            & col("resource").isNotNull()
            & (col("resource") != "")
        )
        .withColumn("_rank", row_number().over(latest_per_endpoint))
        .filter(col("_rank") == 1)
        .select("endpoint", "resource")
    )

    if endpoint_attrs_df is not None:
        current = current.join(endpoint_attrs_df, on="endpoint", how="left")
    else:
        current = current.withColumn("dataset", lit("")).withColumn(
            "organisation", lit("")
        )

    return (
        current.select("resource", "dataset", "organisation", "endpoint")
        .fillna("", subset=["dataset", "organisation"])
        .distinct()
    )


class TaskPipeline(BasePipeline):
    """
    Cross-collection pipeline for generating task data from log and issue files.

    Unlike other pipelines, this reads across all collections at once using
    wildcard S3 paths rather than processing a single dataset/collection.
    Writes a Delta Lake table — full overwrite each run since the table is
    regenerated from scratch nightly.
    """

    def execute(self, entity_data_path):
        spark = self.config.spark
        base = AnyPath(self.config.collection_data_path)

        # -- Resolve file paths ---------------------------------------------------
        # Targeted listings rather than base.glob("*-collection/..."): a glob with
        # a '/' in the pattern lists the WHOLE bucket and filters client-side.
        collections = collection_names(base)
        logger.info(f"TaskPipeline: Found {len(collections)} collections")

        log_files = collection_files(base, collections, "log.csv")
        logger.info(f"TaskPipeline: Found {len(log_files)} log files")

        source_files = collection_files(base, collections, "source.csv")
        logger.info(f"TaskPipeline: Found {len(source_files)} source files")

        log_df = spark.read.option("header", "true").csv(log_files)
        log_df = normalise_column_names(log_df)

        # -- Endpoint → dataset/organisation lookups (from source.csv) ----------
        # source.csv maps endpoint hash → pipelines (dataset name) + organisation,
        # with ';' separating multiple datasets when one endpoint serves several.
        if source_files:
            source_df = spark.read.option("header", "true").csv(source_files)
            source_df = normalise_column_names(source_df)
            active_source_df = source_df.filter(
                col("end_date").isNull() | (col("end_date") == "")
            )

            # Exploded (one row per endpoint+dataset) — backfills failed log
            # entries, where a blank resource can't tell us the dataset.
            endpoint_dataset_df = active_source_df.select(
                col("endpoint"),
                explode(split(col("pipelines"), ";")).alias("dataset"),
            ).distinct()

            # dropDuplicates, not distinct because organisation isn't part of the reference hash
            endpoint_organisation_df = active_source_df.select(
                "endpoint", "organisation"
            ).dropDuplicates(["endpoint"])

            # One row per endpoint carrying its dataset(s) as the raw ';' string
            # and organisation — attributes the active resource below. Kept as a
            # single row per endpoint so the issue join stays one-to-one.
            endpoint_attrs_df = active_source_df.select(
                "endpoint",
                col("pipelines").alias("dataset"),
                "organisation",
            ).dropDuplicates(["endpoint"])
        else:
            endpoint_dataset_df = None
            endpoint_organisation_df = None
            endpoint_attrs_df = None

        # -- Organisation + designation reference (used by the authority leg
        # below, and by the expectation leg further down for organisation) ----
        org_path = str(
            base / "organisation-collection" / "dataset" / "organisation.csv"
        )
        org_df = normalise_column_names(
            spark.read.option("header", "true").csv(org_path)
        ).select(
            col("organisation"),
            col("entity").alias("organisation_entity"),
            (col("end_date").isNull() | (col("end_date") == "")).alias("org_active"),
        )

        config_base = base / "config" / "pipeline"
        eo_files = [str(p) for p in config_base.glob("*/entity-organisation.csv")]
        entity_org_df = read_csvs_by_name(
            spark, eo_files, ["dataset", "organisation"]
        ).distinct()  # designated (dataset, org)

        # -- Active resources (current resource per endpoint, from the log) ------
        active_df = _active_resources_from_log(log_df, endpoint_attrs_df)
        active_df.cache()
        logger.info(
            "TaskPipeline: Active resources loaded (current resource per endpoint from log)"
        )

        # -- Issue files (only for resources that survive the join below) --------
        # The issue join is an inner join on active_df, so files for any other
        # resource are read and then thrown away — ~26,450 files read to use
        # ~3,000. Restricting discovery here also keeps most legacy-layout issue
        # CSVs out of the read; read_issue_csvs handles any that remain.
        active_resources = {
            row["resource"] for row in active_df.select("resource").distinct().collect()
        }
        issue_files = issue_files_for_resources(base, collections, active_resources)
        logger.info(
            f"TaskPipeline: Found {len(issue_files)} issue files for "
            f"{len(active_resources)} active resources"
        )

        # -- Log tasks --------------------------------------------------------
        log_df = log_df.join(
            active_df.select("resource", "dataset", "organisation"),
            on="resource",
            how="left",
        )
        log_df = log_df.fillna("", subset=["dataset", "organisation"])

        if endpoint_dataset_df is not None:
            log_df = _backfill_dataset_from_source(log_df, endpoint_dataset_df)
        if endpoint_organisation_df is not None:
            log_df = _backfill_organisation_from_source(
                log_df, endpoint_organisation_df
            )

        log_tasks = transform_log_to_tasks(log_df)

        # -- Issue tasks ------------------------------------------------------
        if not issue_files:
            logger.warning("TaskPipeline: No issue files found — skipping issue tasks")
            issue_tasks = None
        else:
            issue_df = read_issue_csvs(spark, issue_files)
            issue_df = issue_df.join(
                active_df.select("resource", "organisation", "endpoint"),
                on="resource",
                how="inner",
            )
            issue_df = issue_df.fillna("", subset=["organisation", "endpoint"])

            issue_type_df = _load_issue_type_df(spark)
            issue_df = issue_df.join(issue_type_df, on="issue_type", how="left")

            # One pass, logged at INFO: this pipeline's failure mode was every
            # issue row being dropped here silently, which DEBUG-only counts hid.
            stats = issue_df.agg(
                count("*").alias("rows"),
                count(when(col("severity").isNotNull(), True)).alias("matched"),
                count(when(col("severity").isin(*TASK_SEVERITIES), True)).alias(
                    "surviving"
                ),
            ).collect()[0]
            logger.info(
                f"TaskPipeline: {stats['rows']} issue rows for active resources, "
                f"{stats['matched']} matched an issue-type, "
                f"{stats['surviving']} survive the severity filter"
            )
            if stats["rows"] and not stats["surviving"]:
                logger.error(
                    "TaskPipeline: every issue row was dropped by the severity "
                    "filter — issue CSVs are likely being misparsed"
                )

            issue_tasks = transform_issues_to_tasks(issue_df)

        # -- Expectation tasks -------------------------------------------------
        # Expectations run against the assembled dataset, so unlike issues they
        # have no resource to join on and are not filtered by active resources.
        exp_files = expectation_files(base)
        logger.info(f"TaskPipeline: Found {len(exp_files)} expectation files")

        if not exp_files:
            logger.warning(
                "TaskPipeline: No expectation files found — skipping expectation tasks"
            )
            expectation_tasks = None
        else:
            expectation_df = spark.read.parquet(*exp_files)

            # Same shape of guard as the issue path above: the failure mode
            # here is every row being dropped, which produces no tasks and no
            # error, so the counts are logged rather than left to DEBUG.
            stats = expectation_df.agg(
                count("*").alias("rows"),
                count(when(lower(col("passed").cast("string")) == "false", True)).alias(
                    "failed"
                ),
                count(
                    when(
                        (lower(col("passed").cast("string")) == "false")
                        & col("severity").isin(*TASK_SEVERITIES),
                        True,
                    )
                ).alias("surviving"),
            ).collect()[0]
            logger.info(
                f"TaskPipeline: {stats['rows']} expectation rows, "
                f"{stats['failed']} failed, "
                f"{stats['surviving']} survive the severity filter"
            )
            if stats["rows"] and not stats["surviving"]:
                logger.error(
                    "TaskPipeline: every expectation row was dropped by the "
                    "filters — check `passed` and `severity` values"
                )

            expectation_tasks = transform_expectations_to_tasks(expectation_df, org_df)

        # -- Authority (provide authoritative data) tasks ---------------------
        # Independent of the active-resource / issue-file legs above: a
        # statement about the whole dataset-organisation provision, not any
        # single resource. Uses org_df / entity_org_df read above.
        entity_quality_df = load_entity_quality(spark, entity_data_path)
        authority_population = non_authoritative_providers(
            entity_quality_df, org_df, entity_org_df, RESTRICT_TO_ACTIVE_ORGS
        )
        authority_tasks = transform_authority_to_tasks(authority_population)

        # -- Union and write --------------------------------------------------
        frames = [
            df
            for df in [log_tasks, issue_tasks, expectation_tasks, authority_tasks]
            if df is not None
        ]

        if not frames:
            logger.warning("TaskPipeline: No tasks generated — nothing to write")
            return

        tasks_df = (
            frames[0]
            if len(frames) == 1
            else reduce(lambda a, b: a.unionByName(b), frames)
        )

        # Dimension coverage, one pass logged at INFO in the same spirit as the
        # severity guards above. Some tasks legitimately have no dimension — every
        # untagged issue type in issue-type.csv is responsibility=internal — so a
        # non-zero unmapped count is normal. Every task being unmapped is not, and
        # means the issue-type fetch or a mapping has broken.
        dimension_counts = {
            row["quality_dimension"]: row["count"]
            for row in tasks_df.groupBy("quality_dimension").count().collect()
        }
        logger.info(f"TaskPipeline: tasks by quality dimension: {dimension_counts}")
        if not any(dimension for dimension in dimension_counts if dimension):
            logger.error(
                "TaskPipeline: no task resolved to a quality dimension — check the "
                "issue-type.csv fetch and the quality_dimensions mappings"
            )

        output_path = str(AnyPath(self.config.parquet_datasets_path) / "task")
        logger.info(f"TaskPipeline: Writing tasks to {output_path}...")
        (
            tasks_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )
        logger.info(f"TaskPipeline: Delta table written to {output_path}")

        if self.config.database_url:
            logger.info("TaskPipeline: Writing tasks to Postgres...")
            self._write_postgres(tasks_df)

    def _write_postgres(self, tasks_df):
        # quality_dimension is not in the Postgres task table yet. The staging DDL and
        # the INSERT column list in write_task_to_postgres are both explicit, and Spark's
        # JDBC writer rejects a DataFrame column the target table does not have. Delta
        # carries it — which is what provision quality reads — and Postgres gets it when
        # the digital-land.info migration lands.
        write_task_to_postgres(
            tasks_df.drop("quality_dimension"), self.config.database_url
        )
