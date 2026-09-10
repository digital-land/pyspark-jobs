"""Shared authoritativeness classification.

TaskPipeline (the provide-authoritative-data task) and ProvisionQualityPipeline
(the owner-side of the quality rollup) both need to answer the same question —
does this organisation own an authoritative-quality entity for this dataset? —
and must answer it the same way. This module is the one place that decides it;
neither pipeline re-derives the classification itself.
"""

import logging
from functools import reduce

from cloudpathlib import AnyPath
from pyspark.sql.functions import coalesce, col, countDistinct, lit, when
from pyspark.sql.functions import sum as spark_sum

logger = logging.getLogger(__name__)


def load_entity_quality(spark, entity_data_path):
    """SWAPPABLE SEAM. Phase 1: read the flattened per-dataset entity CSVs
    (one {dataset}.csv each) and return (dataset, organisation_entity,
    quality, entity). Read per file + union because each dataset's flattened
    CSV has its own column set — a single multi-file read would misalign
    headers. Future: swap the body to read the per-dataset Delta tables."""
    entity_files = [str(p) for p in AnyPath(entity_data_path).glob("*.csv")]
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
    entity_quality_df, org_df, entity_org_df, restrict_to_active_orgs
):
    """Designated (dataset, organisation) providers that do not own an
    authoritative-quality entity — the population for the provide-authoritative-
    data task. Covers both cases: owns nothing at all (quality "none") and owns
    only 'some'-quality entities (quality "some"). org_df must carry
    organisation, organisation_entity and org_active."""
    eq = join_entity_quality_to_org(entity_quality_df, org_df)
    owner = owner_side_classification(eq)

    designated = entity_org_df.select("dataset", "organisation").distinct()
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
