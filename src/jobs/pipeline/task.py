"""TaskPipeline: cross-collection task generation from log and issue files."""

import csv
import io
import logging
import re
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
from jobs.config.quality_dimensions import known_dimensions
from jobs.pipeline.authority import (
    live_datasets,
    load_entity_quality,
    non_authoritative_providers,
)
from jobs.pipeline.base import BasePipeline
from jobs.read import read_csvs_by_name, read_issue_csvs
from jobs.transform.task_transformer import (
    EMPTY,
    TASK_SEVERITIES,
    UNREADABLE,
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
    resource_log_files_for_resources,
)
from jobs.utils.df_utils import normalise_column_names
from jobs.utils.postgres_writer_utils import write_task_to_postgres
from jobs.utils.s3_writer_utils import current_dataset_fields, write_single_csv

logger = logging.getLogger(__name__)

# As digital-land-python's MapPhase normalises a header before matching a field.
_NORMALISE_PATTERN = re.compile(r"[^a-z0-9-_]")

# Enough of a raw resource to see its first line, or that it is a FeatureCollection.
_HEAD_BYTES = 64 * 1024

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
                row.get("quality-dimension") or row.get("quality_dimension") or "",
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


def _latest_log_entry_per_endpoint(log_df):
    """The most recent log entry for each endpoint, whatever its status.

    A log task should say how an endpoint is behaving NOW. Without this, every
    distinct failure status an endpoint has ever returned becomes a task and stays
    one for ever: a 404 from 2021 on an endpoint that has collected cleanly every
    night since is already fixed, but still produces a task, and still pins its
    provision to the bottom rung in provision_quality. Keeping only the latest
    entry makes the task self-healing — the next successful collection clears it.

    Mirrors the window in _active_resources_from_log, but keeps the latest entry
    whatever its status rather than the latest successful one; transform_log_to_tasks
    then decides whether that entry was a failure. On a date tie the lowest status
    wins, so an endpoint logged both 200 and 404 on its last day counts as working,
    and nulls sort last so a real status beats a connection error.
    """
    latest_per_endpoint = Window.partitionBy("endpoint").orderBy(
        col("entry_date").desc(), col("status").asc_nulls_last()
    )
    return (
        log_df.withColumn("_rank", row_number().over(latest_per_endpoint))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )


def _failed_conversions(spark, files):
    """Resources whose converted-resource log records a failed conversion.

    The endpoint returned a 200 for these, but what came back — a WFS
    ExceptionReport, an auth-token error, a PDF — could not be turned into rows.

    Every converted-resource log has had the same columns since it was added,
    so one positional multi-file read is safe here, unlike issue CSVs. The
    exception holds ogr2ogr's multi-line output inside quotes, which needs
    multiLine or each line of it is read as a row of its own.
    """
    df = normalise_column_names(
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(files)
    )
    return df.filter(col("status") == "failed").select("resource").distinct()


def _empty_resources(spark, files):
    """{resource: datasets} for resources the pipeline found no data in.

    Empty means every dataset-resource log for the resource records no entities
    from at most one line: a header, a one-line error message, or nothing. A
    resource with entities in any dataset is not empty.

    The dataset-resource log's columns have changed over the years (entity-count
    arrived in 2026), so a positional multi-file read would misalign them. The
    files are a couple of lines each, so each is read whole and parsed against
    its own header. A log without entity-count cannot say, and is left alone.
    """
    seen = {}
    for row in spark.read.text(files, wholetext=True).collect():
        for record in csv.DictReader(io.StringIO(row["value"])):
            resource = record.get("resource")
            if not resource:
                continue
            empty = record.get("entity-count") == "0" and record.get("line-count") in (
                "0",
                "1",
            )
            datasets, all_empty = seen.get(resource, (set(), True))
            seen[resource] = (datasets | {record["dataset"]}, all_empty and empty)
    return {resource: datasets for resource, (datasets, empty) in seen.items() if empty}


def _normalise_column(name):
    return _NORMALISE_PATTERN.sub("", name.replace("_", "-").lower())


def _dataset_columns(base, dataset):
    """Normalised header names that would map onto the dataset: its
    specification fields, and the columns pipeline config maps to them."""
    names = set(current_dataset_fields(base / "specification", dataset))
    column_path = base / "config" / "pipeline" / dataset / "column.csv"
    if column_path.exists():
        with column_path.open(newline="") as f:
            names |= {
                row["column"]
                for row in csv.DictReader(f)
                if row.get("field") and row["field"] != "IGNORE"
            }
    return {_normalise_column(name) for name in names}


def _is_valid_empty_response(head, columns):
    """Whether the start of a response with no rows is a valid file for the
    dataset, rather than an error message sent with a 200.

    A FeatureCollection (WFS or GeoJSON) with no features is a valid answer;
    a WFS error is an ExceptionReport instead. Anything else is valid if its
    first line is a header with at least one column that maps to the dataset.
    An HTML error page or a one-line message shares none.
    """
    lowered = head.lower()
    if "featurecollection" in lowered and "exception" not in lowered:
        return True
    header = next(csv.reader(io.StringIO(head)), [])
    return any(_normalise_column(column) in columns for column in header)


def _unusable_responses(spark, base, collections, resources):
    """{resource: UNREADABLE | EMPTY} for current resources that hold no data.

    UNREADABLE: the resource could not be converted, or it converted to
    nothing and is not a valid file for the dataset (an error page).
    EMPTY: it is a valid file for the dataset with no rows in it.
    """
    unusable = {}

    converted_files = resource_log_files_for_resources(
        base, collections, "converted-resource", resources
    )
    logger.info(f"TaskPipeline: Found {len(converted_files)} converted-resource files")
    if converted_files:
        for row in _failed_conversions(spark, converted_files).collect():
            unusable[row["resource"]] = UNREADABLE

    dataset_resource_files = resource_log_files_for_resources(
        base, collections, "dataset-resource", resources
    )
    logger.info(
        f"TaskPipeline: Found {len(dataset_resource_files)} dataset-resource files"
    )
    if not dataset_resource_files:
        return unusable

    # The raw resource sits in the collection the log came from
    collection_dirs = {
        AnyPath(f).stem: AnyPath(f.split("/var/dataset-resource/")[0])
        for f in dataset_resource_files
    }
    columns_by_dataset = {}
    for resource, datasets in _empty_resources(spark, dataset_resource_files).items():
        if resource in unusable:
            continue
        raw_path = collection_dirs[resource] / "collection" / "resource" / resource
        if not raw_path.exists():
            logger.warning(f"TaskPipeline: no raw resource at {raw_path}, skipping")
            continue
        with raw_path.open("rb") as f:
            head = f.read(_HEAD_BYTES).decode("utf-8-sig", errors="replace")
        columns = set()
        for dataset in datasets:
            if dataset not in columns_by_dataset:
                columns_by_dataset[dataset] = _dataset_columns(base, dataset)
            columns |= columns_by_dataset[dataset]
        unusable[resource] = (
            EMPTY if _is_valid_empty_response(head, columns) else UNREADABLE
        )

    logger.info(
        f"TaskPipeline: {sum(v == UNREADABLE for v in unusable.values())} unreadable "
        f"and {sum(v == EMPTY for v in unusable.values())} empty current resources"
    )
    return unusable


class TaskPipeline(BasePipeline):
    """
    Cross-collection pipeline for generating task data from log and issue files.

    Unlike other pipelines, this reads across all collections at once using
    wildcard S3 paths rather than processing a single dataset/collection.
    Writes three ways: Delta to parquet_datasets_path (canonical), serving
    Postgres, and a CSV to output_path for download — full overwrite each run
    since the table is regenerated from scratch nightly.
    """

    def execute(self, entity_data_path, output_path):
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

            active_endpoints_df = active_source_df.select("endpoint").distinct()
        else:
            endpoint_dataset_df = None
            endpoint_organisation_df = None
            endpoint_attrs_df = None
            active_endpoints_df = None

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

        # -- Drop endpoints that have been retired ----------------------------
        if active_endpoints_df is not None:
            log_df = log_df.join(active_endpoints_df, on="endpoint", how="left_semi")

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
        # Narrow to each endpoint's current state before attribution
        log_df = _latest_log_entry_per_endpoint(log_df)

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

        # A 200 with no data in it is a failure too: the endpoint answered, but
        # not with data. Only the current resources are looked up, and a 200 is
        # always its endpoint's current resource.
        unusable = _unusable_responses(spark, base, collections, active_resources)
        if unusable:
            unusable_df = spark.createDataFrame(
                list(unusable.items()), "resource string, unusable_response string"
            )
            log_df = log_df.join(unusable_df, on="resource", how="left")

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
        # single resource. Uses org_df / entity_org_df read above. The live
        # datasets read here also filter every other task below the union.
        dataset_path = str(base / "specification" / "dataset.csv")
        dataset_df = normalise_column_names(
            spark.read.option("header", "true").csv(dataset_path)
        )
        live_datasets_df = live_datasets(dataset_df, self.config.env)

        entity_quality_df = load_entity_quality(spark, entity_data_path)
        authority_population = non_authoritative_providers(
            entity_quality_df,
            org_df,
            entity_org_df,
            live_datasets_df,
            RESTRICT_TO_ACTIVE_ORGS,
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

        # -- Live datasets only -------------------------------------------------
        # Switching a dataset off for this environment in the specification stops
        # it being built, but its frozen log and issue files stay in the bucket and
        # would keep producing tasks every night. So we apply this live-dataset
        # filter to every task. Tasks with a blank dataset are deliberately kept:
        # blank means the task couldn't be attributed, not that the dataset is off.

        live_dataset_names = [row["dataset"] for row in live_datasets_df.collect()]
        logger.info(
            f"TaskPipeline: keeping tasks for {len(live_dataset_names)} live datasets"
        )
        tasks_df = tasks_df.filter(
            col("dataset").isNull()
            | (col("dataset") == "")
            | col("dataset").isin(live_dataset_names)
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
                "issue-type.csv fetch and the specification's quality_dimension column"
            )

        # Values now arrive from issue-type.csv unmapped, so a new or renamed value
        # upstream would flow into the task table silently. Visible, not blocking —
        # the specification is allowed to grow a dimension before we know about it.
        unknown = {
            dimension
            for dimension in dimension_counts
            if dimension and dimension not in known_dimensions()
        }
        if unknown:
            logger.warning(
                f"TaskPipeline: quality dimensions outside the agreed vocabulary: "
                f"{sorted(unknown)} — issue-type.csv may have gained a value"
            )

        delta_path = str(AnyPath(self.config.parquet_datasets_path) / "task")
        logger.info(f"TaskPipeline: Writing tasks to {delta_path}...")
        (
            tasks_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(delta_path)
        )
        logger.info(f"TaskPipeline: Delta table written to {delta_path}")

        if self.config.database_url:
            logger.info("TaskPipeline: Writing tasks to Postgres...")
            self._write_postgres(tasks_df)

        write_single_csv(
            tasks_df.orderBy("dataset", "organisation"), output_path, "task"
        )

    def _write_postgres(self, tasks_df):
        write_task_to_postgres(tasks_df, self.config.database_url)
