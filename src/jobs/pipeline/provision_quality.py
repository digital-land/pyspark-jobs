"""ProvisionQualityPipeline: provider/organisation quality classification."""

import logging
from itertools import chain

from cloudpathlib import AnyPath
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    create_map,
    explode,
    first,
    lit,
    lower,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import (
    split,
    substring,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (
    to_date,
    when,
)

from jobs.config.quality_dimensions import AUTHORITATIVENESS
from jobs.pipeline.authority import (
    join_entity_quality_to_org,
    live_datasets,
    load_entity_quality,
    owner_side_classification,
)
from jobs.pipeline.base import BasePipeline
from jobs.read import read_csvs_by_name
from jobs.utils.collection_paths import collection_files, collection_names
from jobs.utils.df_utils import normalise_column_names
from jobs.utils.postgres_writer_utils import write_table_to_postgres
from jobs.utils.s3_writer_utils import write_single_csv
from jobs.utils.specification import (
    load_declared_provisions,
    load_quality_priorities,
    load_severity_priorities,
)

logger = logging.getLogger(__name__)


def _seeder_alt_sources(eq, lookup_df, entity_org_df, active_orgs):
    """Seeder (alt-source) detection. An active org that seeded 'some'-quality
    entities it does NOT own and is NOT designated for counts as a 'some'
    contributor. LA-type orgs must have seeded for >1 distinct owner to
    count (stale-lookup guard); other org types need >=1."""
    some_ent = eq.filter(col("quality") == "some").select(
        "dataset", "entity", col("organisation").alias("owner_org")
    )
    some_owner_orgs = some_ent.select(
        "dataset", col("owner_org").alias("organisation")
    ).distinct()

    lkp = lookup_df.select("entity", col("organisation").alias("seeder"))
    candidates = some_ent.join(lkp, on="entity", how="inner")

    # seeder must not itself own 'some' entities in this dataset ...
    candidates = candidates.join(
        some_owner_orgs.select("dataset", col("organisation").alias("seeder")),
        on=["dataset", "seeder"],
        how="left_anti",
    )
    # ... must not be designated for this dataset ...
    candidates = candidates.join(
        entity_org_df.select("dataset", col("organisation").alias("seeder")),
        on=["dataset", "seeder"],
        how="left_anti",
    )
    # ... and must be active.
    candidates = candidates.join(
        active_orgs.select(col("organisation").alias("seeder")),
        on="seeder",
        how="left_semi",
    )

    coverage = candidates.groupBy("dataset", "seeder").agg(
        countDistinct("owner_org").alias("owner_coverage"),
        countDistinct("entity").alias("seeded_count"),
    )
    is_la = (
        col("seeder").startswith("local-authority:")
        | col("seeder").startswith("national-park-authority:")
        | col("seeder").startswith("development-corporation:")
    )
    alt = coverage.filter(
        (is_la & (col("owner_coverage") > 1)) | (~is_la & (col("owner_coverage") >= 1))
    )
    return alt.select(
        "dataset",
        col("seeder").alias("organisation"),
        lit("some").alias("seeder_quality"),
        col("seeded_count").alias("seeder_entity_count"),
    )


def load_tasks(spark, parquet_datasets_path):
    """SWAPPABLE SEAM, same discipline as load_entity_quality: the task table the
    task pipeline wrote earlier in this DAG run."""
    task_path = str(AnyPath(parquet_datasets_path) / "task")
    logger.info(f"ProvisionQuality: reading tasks from {task_path}")
    return (
        spark.read.format("delta")
        .load(task_path)
        .select(
            "dataset", "organisation", "severity", "responsibility", "quality_dimension"
        )
    )


def _task_state(tasks_df, severity_priorities, error_priority, scoring_priority):
    """Per (dataset, organisation): is there an error-or-worse task, and any task at all?

    Authoritativeness tasks are EXCLUDED. They decide which band a provision sits
    in, not where it sits inside that band — and because the
    provide-authoritative-data task is always severity=error, counting them would
    put every non-authoritative provision on the bottom rung and make `indicative`
    and `verifiable` unreachable.

    Internal responsibility is excluded: those are our processing problems and must
    not mark a publisher down. That also removes every task with no dimension,
    since all the untagged issue types in issue-type.csv are internal.

    Severities below `scoring_priority` are excluded. The task table carries
    `notice` rows so we can work out which checks belong at which severity, but a
    notice must not move a provision: the rung comes from `task_count > 0`, so
    without this a notice-only provision would silently drop off the top rung. An
    unrecognised severity is excluded for the same reason — we cannot score what
    we cannot rank.
    """
    severity_map = create_map([lit(x) for x in chain(*severity_priorities.items())])
    relevant = (
        tasks_df.filter(~col("quality_dimension").eqNullSafe(lit(AUTHORITATIVENESS)))
        .filter(~col("responsibility").eqNullSafe(lit("internal")))
        .filter(col("organisation").isNotNull() & (col("organisation") != ""))
        .withColumn("severity_priority", severity_map[col("severity")])
        .filter(col("severity_priority") <= lit(scoring_priority))
    )
    return relevant.groupBy("dataset", "organisation").agg(
        spark_max(
            when(col("severity_priority") <= lit(error_priority), lit(1)).otherwise(
                lit(0)
            )
        ).alias("has_error_task"),
        count(lit(1)).alias("task_count"),
    )


def _provision_start_dates(dated_source_df):
    """Earliest entry-date per (dataset, organisation) — when an endpoint was first
    added for that provision.

    Ended source rows are kept deliberately. A provider whose URL changed got a new
    source row and the old one was end-dated, so counting only live rows would report
    a provider active since 2021 as having started in 2025. That differs for 42% of
    provisions, which is the decision this function exists to hold.

    entry-date is an ISO timestamp ("2023-07-07T13:13:02Z"). The date part is taken by
    substring rather than by cast, because casting a Z-suffixed timestamp is
    version-dependent and can silently return null.
    """
    return (
        dated_source_df.filter(col("endpoint").isNotNull() & (col("endpoint") != ""))
        .select(
            explode(split(col("pipelines"), ";")).alias("dataset"),
            col("organisation"),
            to_date(substring(col("entry_date"), 1, 10)).alias("start_date"),
        )
        .filter(col("start_date").isNotNull())
        .groupBy("dataset", "organisation")
        .agg(spark_min("start_date").alias("start_date"))
    )


def _build_provision_quality(
    providers_df,
    org_df,
    entity_org_df,
    lookup_df,
    entity_quality_df,
    declared_df,
    start_dates_df,
):
    """Base table: one row per (dataset, organisation) that has an active endpoint
    OR owns entities OR is a detected seeder. Nothing dropped; flags distinguish
    the cases. Owner/provider classification + seeder detection."""
    # map each owned entity to its owner organisation reference
    eq = join_entity_quality_to_org(entity_quality_df, org_df)

    owner_side = owner_side_classification(eq)
    active_orgs = org_df.filter(col("org_active")).select("organisation").distinct()
    seeder_side = _seeder_alt_sources(eq, lookup_df, entity_org_df, active_orgs)

    providers = providers_df.select("dataset", "organisation").distinct()
    designated = entity_org_df.select("dataset", "organisation").distinct()

    keys = (
        providers.unionByName(owner_side.select("dataset", "organisation"))
        .unionByName(seeder_side.select("dataset", "organisation"))
        # the specification's declared universe, so a provision that has supplied
        # nothing still gets a row and can score "no data"
        .unionByName(declared_df.select("dataset", "organisation"))
        .distinct()
    )

    pq = (
        keys.join(
            providers_df.withColumn("has_active_endpoint", lit(True)),
            on=["dataset", "organisation"],
            how="left",
        )
        .join(owner_side, on=["dataset", "organisation"], how="left")
        .join(seeder_side, on=["dataset", "organisation"], how="left")
        .join(
            designated.withColumn("is_designated_provider", lit(True)),
            on=["dataset", "organisation"],
            how="left",
        )
        .join(
            org_df.select("organisation", "organisation_name").distinct(),
            on="organisation",
            how="left",
        )
        .join(start_dates_df, on=["dataset", "organisation"], how="left")
    )

    return pq.select(
        "dataset",
        "organisation",
        "organisation_name",
        coalesce(col("has_active_endpoint"), lit(False)).alias("has_active_endpoint"),
        coalesce(col("has_active_resource"), lit(False)).alias("has_active_resource"),
        coalesce(col("owns_entities"), lit(False)).alias("owns_entities"),
        coalesce(col("is_designated_provider"), lit(False)).alias(
            "is_designated_provider"
        ),
        coalesce(col("owner_quality"), col("seeder_quality")).alias("entity_quality"),
        coalesce(col("owned_entity_count"), col("seeder_entity_count"), lit(0)).alias(
            "entity_count"
        ),
        "start_date",
    )


def _assign_quality(provision_quality, task_state, quality_priorities):
    """The seven-level ladder: authoritativeness picks the band, task state picks
    the rung within it.

        base   = none          owns no entities
                 some          owns only alternative-source entities
                 authoritative owns authoritative entities
        offset = 0 any error-or-worse task, 1 warnings only, 2 no tasks

    A pure function of (entity_quality, task state), so the ladder can move into
    reference data later without the logic changing. Priorities are read from
    quality.csv; only the three band anchors are named here, never their values.
    """
    none_priority = quality_priorities["none"]
    some_priority = quality_priorities["some"]
    authoritative_priority = quality_priorities["authoritative"]
    word = create_map(
        [lit(x) for x in chain(*{p: q for q, p in quality_priorities.items()}.items())]
    )

    base = (
        when(col("entity_quality") == "authoritative", lit(authoritative_priority))
        .when(col("entity_quality") == "some", lit(some_priority))
        .otherwise(lit(none_priority))
    )
    offset = (
        when(col("has_error_task") == 1, lit(0))
        .when(col("task_count") > 0, lit(1))
        .otherwise(lit(2))
    )

    scored = provision_quality.join(
        task_state, on=["dataset", "organisation"], how="left"
    ).withColumn(
        # a provision with no data stays at the bottom whatever its task state
        "priority",
        when(base == lit(none_priority), lit(none_priority)).otherwise(base + offset),
    )
    return scored.select(
        "dataset",
        "organisation",
        "organisation_name",
        "has_active_endpoint",
        "has_active_resource",
        "owns_entities",
        "is_designated_provider",
        word[col("priority")].alias("quality"),
        "entity_count",
        col("priority").cast("double").alias("quality_score"),
        "start_date",
    )


def _build_dataset_quality(provision_quality, authoritative_priority):
    """Rollup per dataset. Organisations are counted by BAND, not by exact quality:
    `usable` and `trustworthy` are authoritative provisions that happen to be
    cleaner, so an equality test on "authoritative" would count only the ones with
    outstanding errors."""
    return (
        provision_quality.groupBy("dataset")
        .agg(
            countDistinct(
                when(
                    col("quality_score") >= authoritative_priority, col("organisation")
                )
            ).alias("authoritative_organisations"),
            countDistinct(
                when(col("quality_score") < authoritative_priority, col("organisation"))
            ).alias("some_organisations"),
            countDistinct("organisation").alias("total_organisations"),
            spark_sum(
                when(col("owns_entities"), col("entity_count")).otherwise(0)
            ).alias("total_entities"),
        )
        .withColumn("quality_score", lit(None).cast("double"))
    )


def _build_organisation_quality(provision_quality, authoritative_priority):
    """Rollup per organisation across datasets. Datasets are counted by BAND rather
    than by exact quality — see _build_dataset_quality."""
    return (
        provision_quality.groupBy("organisation")
        .agg(
            first("organisation_name", ignorenulls=True).alias("organisation_name"),
            countDistinct(
                when(col("quality_score") >= authoritative_priority, col("dataset"))
            ).alias("authoritative_datasets"),
            countDistinct(
                when(col("quality_score") < authoritative_priority, col("dataset"))
            ).alias("some_datasets"),
            countDistinct("dataset").alias("total_datasets"),
            spark_sum(
                when(col("owns_entities"), col("entity_count")).otherwise(0)
            ).alias("total_entities_owned"),
        )
        .withColumn("quality_score", lit(None).cast("double"))
    )


def _drop_blank_organisations(df):
    """Drop rows with no organisation — a blank org can't key a table
    (organisation is a NOT NULL primary key). Rollups already exclude them."""
    return df.filter(col("organisation").isNotNull() & (col("organisation") != ""))


PROVISION_QUALITY_PG_TYPES = [
    ("dataset", "TEXT"),
    ("organisation", "TEXT"),
    ("organisation_name", "TEXT"),
    ("has_active_endpoint", "BOOLEAN"),
    ("has_active_resource", "BOOLEAN"),
    ("owns_entities", "BOOLEAN"),
    ("is_designated_provider", "BOOLEAN"),
    ("quality", "TEXT"),
    ("entity_count", "BIGINT"),
    ("quality_score", "DOUBLE PRECISION"),
    ("start_date", "DATE"),
]

DATASET_QUALITY_PG_TYPES = [
    ("dataset", "TEXT"),
    ("authoritative_organisations", "INTEGER"),
    ("some_organisations", "INTEGER"),
    ("total_organisations", "INTEGER"),
    ("total_entities", "BIGINT"),
    ("quality_score", "DOUBLE PRECISION"),
]

ORGANISATION_QUALITY_PG_TYPES = [
    ("organisation", "TEXT"),
    ("organisation_name", "TEXT"),
    ("authoritative_datasets", "INTEGER"),
    ("some_datasets", "INTEGER"),
    ("total_datasets", "INTEGER"),
    ("total_entities_owned", "BIGINT"),
    ("quality_score", "DOUBLE PRECISION"),
]


class ProvisionQualityPipeline(BasePipeline):
    """
    Cross-collection pipeline computing provider/organisation quality per
    (dataset, organisation). Reads across all collections at once (wildcard S3
    paths) like TaskPipeline. Phase 1 writes three CSVs; phase 2 will add Delta
    + Postgres. Classification follows the agreed provider/organisation
    quality definitions (see the Provision Quality technical documentation).
    """

    def execute(self, entity_data_path, output_path):
        spark = self.config.spark
        base = AnyPath(self.config.collection_data_path)

        # -- Active providers (source.csv → who submits) ------------------------
        # Non-empty endpoint, empty end_date; `pipelines` (';'-split) = dataset(s).
        collections = collection_names(base)
        source_files = collection_files(base, collections, "source.csv")
        logger.info(f"ProvisionQuality: Found {len(source_files)} source files")
        source_df = read_csvs_by_name(
            spark, source_files, ["endpoint", "end_date", "organisation", "pipelines"]
        )
        active_sources = (
            source_df.filter(
                (col("endpoint").isNotNull() & (col("endpoint") != ""))
                & (col("end_date").isNull() | (col("end_date") == ""))
            )
            .select(
                explode(split(col("pipelines"), ";")).alias("dataset"),
                col("organisation"),
                col("endpoint"),
            )
            .distinct()
        )

        # -- Provision start date (source.csv → when the endpoint was added) ----
        # Read separately from the active-source read above: read_csvs_by_name
        # SKIPS any file missing a requested column, so folding entry_date into
        # that select would silently drop a whole collection's providers if one
        # source.csv predates the column.
        dated_source_df = read_csvs_by_name(
            spark, source_files, ["endpoint", "organisation", "pipelines", "entry_date"]
        )
        start_dates = _provision_start_dates(dated_source_df)

        # -- Active resources (resource.csv → is data still arriving) -----------
        # A resource's end-date is the last date the collector saw it, so blank
        # means it was fetched today. An endpoint can be configured and active
        # while nothing actually arrives. `endpoints` is ';'-joined where
        # several endpoints produced identical content.
        resource_files = collection_files(base, collections, "resource.csv")
        logger.info(f"ProvisionQuality: Found {len(resource_files)} resource files")
        resource_df = read_csvs_by_name(
            spark, resource_files, ["endpoints", "end_date"]
        )
        delivering_endpoints = (
            resource_df.filter(col("end_date").isNull() | (col("end_date") == ""))
            .select(explode(split(col("endpoints"), ";")).alias("endpoint"))
            .distinct()
        )

        # an organisation is still delivering a dataset if ANY of its active
        # endpoints for it still has a resource arriving
        delivering = (
            active_sources.join(delivering_endpoints, on="endpoint", how="left_semi")
            .select("dataset", "organisation")
            .distinct()
        )
        providers_df = (
            active_sources.select("dataset", "organisation")
            .distinct()
            .join(
                delivering.withColumn("has_active_resource", lit(True)),
                on=["dataset", "organisation"],
                how="left",
            )
        )

        # -- Organisation reference (organisation.csv) --------------------------
        org_path = str(
            base / "organisation-collection" / "dataset" / "organisation.csv"
        )
        org_df = normalise_column_names(
            spark.read.option("header", "true").csv(org_path)
        )
        # org<->entity id, human name, active flag (empty end_date)
        org_df = org_df.select(
            col("organisation"),
            col("entity").alias("organisation_entity"),
            col("name").alias("organisation_name"),
            (col("end_date").isNull() | (col("end_date") == "")).alias("org_active"),
        )

        # -- Config: designated provisions + seeding lookup ---------------------
        config_base = base / "config" / "pipeline"
        eo_files = [str(p) for p in config_base.glob("*/entity-organisation.csv")]
        entity_org_df = read_csvs_by_name(
            spark, eo_files, ["dataset", "organisation"]
        ).distinct()  # designated (dataset, org)

        lookup_files = [str(p) for p in config_base.glob("*/lookup.csv")]
        lookup_df = read_csvs_by_name(
            spark, lookup_files, ["entity", "organisation"]
        )  # who seeded each entity

        # -- Live datasets (specification/dataset.csv) --------------------------
        # Restrict to datasets the platform still builds in this environment;
        # retired ones linger in the CSVs (e.g. local-plan-timetable) in s3.
        dataset_path = str(base / "specification" / "dataset.csv")
        dataset_df = normalise_column_names(
            spark.read.option("header", "true").csv(dataset_path)
        )
        live_datasets_df = live_datasets(dataset_df, self.config.env)

        # -- Entity + quality (SWAPPABLE SEAM) ----------------------------------
        entity_quality_df = load_entity_quality(spark, entity_data_path)

        # -- Specification: the ladder, the severity scale, the declared universe -
        quality_priorities = load_quality_priorities()
        severity_priorities = load_severity_priorities()
        declared_df = load_declared_provisions(spark)

        # -- Tasks (written by assemble-tasks earlier in this DAG run) ----------
        task_state = _task_state(
            load_tasks(spark, self.config.parquet_datasets_path),
            severity_priorities,
            severity_priorities["error"],
            severity_priorities["warning"],
        )

        # -- Classification + rollups ------------------------------------------
        provision_quality = _build_provision_quality(
            providers_df,
            org_df,
            entity_org_df,
            lookup_df,
            entity_quality_df,
            declared_df,
            start_dates,
        ).localCheckpoint(
            eager=True
        )  # materialise once AND truncate the huge plan

        # Log what the specification filter removes before applying it, so a
        # dataset disappearing from the output is never silent.
        dropped = (
            provision_quality.select("dataset")
            .distinct()
            .join(live_datasets_df, on="dataset", how="left_anti")
        )
        dropped_names = sorted(row["dataset"] for row in dropped.collect())
        if dropped_names:
            logger.info(
                f"ProvisionQuality: excluding {len(dropped_names)} dataset(s) not live "
                f"in {self.config.env}: {', '.join(dropped_names)}"
            )
        provision_quality = provision_quality.join(
            live_datasets_df, on="dataset", how="left_semi"
        )

        provision_quality = _drop_blank_organisations(provision_quality)

        # Score after the filters so the task join only sees rows we keep, and
        # checkpoint again: provision_quality now has five actions on it (two
        # rollups, CSV, Delta, Postgres) and the task join is a shuffle.
        provision_quality = _assign_quality(
            provision_quality, task_state, quality_priorities
        ).localCheckpoint(eager=True)

        authoritative_priority = quality_priorities["authoritative"]
        dataset_quality = _build_dataset_quality(
            provision_quality, authoritative_priority
        )
        organisation_quality = _build_organisation_quality(
            provision_quality, authoritative_priority
        )

        # -- Write (phase 1: CSV) ----------------------------------------------
        write_single_csv(
            provision_quality.orderBy(
                col("dataset"),
                lower(col("organisation_name")).asc_nulls_last(),
            ),
            output_path,
            "provision-quality",
        )
        write_single_csv(
            dataset_quality.orderBy("dataset"), output_path, "dataset-quality"
        )
        write_single_csv(
            organisation_quality.orderBy(lower(col("organisation_name"))),
            output_path,
            "organisation-quality",
        )

        # -- Write Delta (canonical) + Postgres (serving) ----------------------
        outputs = [
            ("provision_quality", provision_quality, PROVISION_QUALITY_PG_TYPES),
            ("dataset_quality", dataset_quality, DATASET_QUALITY_PG_TYPES),
            (
                "organisation_quality",
                organisation_quality,
                ORGANISATION_QUALITY_PG_TYPES,
            ),
        ]
        for name, frame, _ in outputs:
            delta_path = str(AnyPath(self.config.parquet_datasets_path) / name)
            logger.info(f"ProvisionQuality: Writing Delta table to {delta_path}")
            frame.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).save(delta_path)

        if self.config.database_url:
            for name, frame, pg_types in outputs:
                logger.info(f"ProvisionQuality: Writing {name} to Postgres")
                write_table_to_postgres(frame, name, pg_types, self.config.database_url)
        else:
            logger.info(
                "ProvisionQuality: No database_url provided — skipping Postgres writes"
            )
