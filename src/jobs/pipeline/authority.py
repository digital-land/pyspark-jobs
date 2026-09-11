"""Shared authoritativeness classification and dataset scoping.

TaskPipeline (the provide-authoritative-data task) and ProvisionQualityPipeline
(the owner-side of the quality rollup) both need to answer the same question —
does this organisation own an authoritative-quality entity for this dataset? —
and must answer it the same way. This module is the one place that decides it;
neither pipeline re-derives the classification itself. The same applies to which
datasets are in scope at all, hence live_datasets() living here too.
"""

import logging
from functools import reduce

from cloudpathlib import AnyPath
from pyspark.sql.functions import coalesce, col, countDistinct, lit, when
from pyspark.sql.functions import sum as spark_sum

logger = logging.getLogger(__name__)

# Cross-collection pipeline outputs are written into the same folder as the
# flattened per-dataset entity CSVs, so a plain *.csv glob picks them up. They are
# excluded by name rather than left to the column check in load_entity_quality:
# provision-quality.csv already carries a `quality` column, so it is a single
# `organisation-entity` column away from being read as entity data instead of skipped.
PIPELINE_OUTPUT_CSVS = {
    "provision-quality",
    "dataset-quality",
    "organisation-quality",
    "task",
}


def load_entity_quality(spark, entity_data_path):
    """SWAPPABLE SEAM. Phase 1: read the flattened per-dataset entity CSVs
    (one {dataset}.csv each) and return (dataset, organisation_entity,
    quality, entity). Read per file + union because each dataset's flattened
    CSV has its own column set — a single multi-file read would misalign
    headers. Future: swap the body to read the per-dataset Delta tables."""
    entity_files = [
        str(p)
        for p in AnyPath(entity_data_path).glob("*.csv")
        if p.stem not in PIPELINE_OUTPUT_CSVS
    ]
    logger.info(f"authority: Found {len(entity_files)} entity CSVs")
    frames = []
    for f in entity_files:
        dataset = AnyPath(f).stem
        df = spark.read.option("header", "true").csv(f)
        if "organisation-entity" not in df.columns or "quality" not in df.columns:
            logger.warning(
                f"authority: {dataset} flattened CSV missing "
                "organisation-entity/quality — skipping"
            )
            continue
        frames.append(
            df.select(
                col("entity"),
                col("`organisation-entity`").alias("organisation_entity"),
                col("quality"),
            ).withColumn("dataset", lit(dataset))
        )
    if not frames:
        raise ValueError(f"No usable entity CSVs found under {entity_data_path}")
    return reduce(lambda a, b: a.unionByName(b), frames)


def live_datasets(dataset_df, env):
    """The datasets the platform builds in `env` and has not retired.

    Mirrors is_dataset_available in airflow-dags (dags/utils.py): a
    `production` dataset is built in every environment, `staging` only in
    staging and development, `development` only in development, and a blank
    environment is not built anywhere. An end-dated dataset is retired
    whatever its environment (e.g. development-plan-document, which is
    production but was end-dated in February).
    """
    available = col("environment") == "production"
    if env in ("staging", "development"):
        available = available | (col("environment") == "staging")
    if env == "development":
        available = available | (col("environment") == "development")

    return (
        dataset_df.filter(available)
        .filter(col("end_date").isNull() | (col("end_date") == ""))
        .select("dataset")
        .distinct()
    )


def join_entity_quality_to_org(entity_quality_df, org_df):
    """Map each owned entity to its owner organisation curie. org_df must
    carry organisation + organisation_entity."""
    return entity_quality_df.join(
        org_df.select("organisation", "organisation_entity"),
        on="organisation_entity",
        how="inner",
    )


def owner_side_classification(eq):
    """Owner lens: per (dataset, organisation) that owns entities, its quality
    (authoritative if it owns any authoritative entity, else some) and the count
    of entities it owns. `eq` is join_entity_quality_to_org()'s output."""
    agg = eq.groupBy("dataset", "organisation").agg(
        spark_sum(when(col("quality") == "authoritative", 1).otherwise(0)).alias(
            "auth_owned"
        ),
        countDistinct("entity").alias("owned_entity_count"),
    )
    return agg.select(
        "dataset",
        "organisation",
        lit(True).alias("owns_entities"),
        when(col("auth_owned") > 0, lit("authoritative"))
        .otherwise(lit("some"))
        .alias("owner_quality"),
        col("owned_entity_count"),
    )


def non_authoritative_providers(
    entity_quality_df, org_df, entity_org_df, live_datasets_df, restrict_to_active_orgs
):
    """Designated (dataset, organisation) providers that do not own an
    authoritative-quality entity — the population for the provide-authoritative-
    data task. Covers both cases: owns nothing at all (quality "none") and owns
    only 'some'-quality entities (quality "some"). org_df must carry
    organisation, organisation_entity and org_active.

    live_datasets_df restricts the population to datasets this environment
    actually builds. Designation config outlives the dataset, so without it a
    retired or never-enabled dataset still produces tasks nobody can act on.
    """
    eq = join_entity_quality_to_org(entity_quality_df, org_df)
    owner = owner_side_classification(eq)

    designated = entity_org_df.select("dataset", "organisation").distinct()

    # Logged before the filter is applied, so a dataset dropping out of the task
    # population is never silent
    dropped = (
        designated.select("dataset")
        .distinct()
        .join(live_datasets_df, on="dataset", how="left_anti")
    )
    dropped_names = sorted(row["dataset"] for row in dropped.collect())
    if dropped_names:
        logger.info(
            f"authority: excluding {len(dropped_names)} dataset(s) not live: "
            f"{', '.join(dropped_names)}"
        )
    designated = designated.join(live_datasets_df, on="dataset", how="left_semi")

    if restrict_to_active_orgs:
        active_orgs = org_df.filter(col("org_active")).select("organisation").distinct()
        designated = designated.join(active_orgs, on="organisation", how="left_semi")

    population = designated.join(owner, on=["dataset", "organisation"], how="left")
    population = population.filter(
        col("owner_quality").isNull() | (col("owner_quality") == "some")
    )

    return population.select(
        "dataset",
        "organisation",
        coalesce(col("owner_quality"), lit("none")).alias("quality"),
        coalesce(col("owned_entity_count"), lit(0)).alias("owned_entity_count"),
    )
