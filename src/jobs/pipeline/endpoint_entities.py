"""Which active endpoints give their organisation no entities (config#3002).

Raised per endpoint and dataset, from the endpoint's current resource: the facts
that resource produced (fact_resource) tell us its entities, and the flattened
entity CSVs tell us who owns each one. An organisation can own entities through
one endpoint while another of its endpoints gives it nothing, so this is not the
same as provision_quality's owns_entities, which is per (dataset, organisation).
"""

import logging

from cloudpathlib import AnyPath
from pyspark.sql.functions import (
    col,
    collect_set,
    countDistinct,
    explode,
    lit,
    sort_array,
    split,
    trim,
    when,
)

from jobs.config.endpoint_entities_task import (
    NO_ENTITIES,
    NO_RESOURCE,
    OTHER_ORGANISATION,
)
from jobs.pipeline.authority import join_entity_quality_to_org

logger = logging.getLogger(__name__)


def endpoint_datasets(active_source_df):
    """One row per (endpoint, dataset, organisation) from the live source rows.

    Taken per source row rather than per endpoint, so an endpoint two
    organisations both collect from is checked for each of them.
    """
    return (
        active_source_df.select(
            "endpoint",
            "organisation",
            explode(split(col("pipelines"), ";")).alias("dataset"),
        )
        .withColumn("dataset", trim(col("dataset")))
        .filter(col("endpoint").isNotNull() & (col("endpoint") != ""))
        .filter(col("organisation").isNotNull() & (col("organisation") != ""))
        .filter(col("dataset").isNotNull() & (col("dataset") != ""))
        .distinct()
    )


def load_fact_resource(spark, parquet_datasets_path):
    """SWAPPABLE SEAM: (dataset, resource, entity) for every fact a resource
    produced, from the fact_resource Delta table the entity pipeline writes.
    None when the table does not exist (a fresh environment, or a local run)."""
    path = AnyPath(parquet_datasets_path) / "fact_resource"
    if not path.exists():
        logger.warning(f"endpoint_entities: no fact_resource table at {path}")
        return None
    return (
        spark.read.format("delta")
        .load(str(path))
        .select("dataset", "resource", col("entity").cast("string").alias("entity"))
    )


def endpoints_without_entities(
    endpoint_datasets_df,
    current_resources_df,
    fact_resource_df,
    entity_quality_df,
    org_df,
    live_datasets_df,
    rule,
    restrict_to_active_orgs,
):
    """Active (endpoint, dataset, organisation) whose current resource gives the
    organisation no entities, with the reason and who the entities went to.

    current_resources_df: endpoint, resource — one row per endpoint that has
    collected anything (the task pipeline's active resources). An endpoint not in
    it has never been collected from.
    fact_resource_df: dataset, resource, entity.
    org_df: organisation, organisation_entity, org_active.
    """
    population = endpoint_datasets_df.join(
        live_datasets_df, on="dataset", how="left_semi"
    ).filter(~col("organisation").isin(rule["exempt_organisations"]))
    if restrict_to_active_orgs:
        active_orgs = org_df.filter(col("org_active")).select("organisation")
        population = population.join(active_orgs, on="organisation", how="left_semi")

    population = population.join(
        current_resources_df.select("endpoint", "resource"), on="endpoint", how="left"
    )

    # Only the facts of the resources being checked, and their entities' owners
    produced = (
        fact_resource_df.join(
            population.select("dataset", "resource").distinct(),
            on=["dataset", "resource"],
            how="left_semi",
        )
        .select("dataset", "resource", "entity")
        .distinct()
    )
    owners = join_entity_quality_to_org(entity_quality_df, org_df).select(
        "dataset",
        col("entity").cast("string").alias("entity"),
        col("organisation").alias("owner"),
    )
    produced = produced.join(owners, on=["dataset", "entity"], how="left")

    counted = (
        population.join(produced, on=["dataset", "resource"], how="left")
        .groupBy("endpoint", "dataset", "organisation", "resource")
        .agg(
            countDistinct("entity").alias("entity_count"),
            countDistinct(
                when(col("owner") == col("organisation"), col("entity"))
            ).alias("owned_entity_count"),
            sort_array(
                collect_set(when(col("owner") != col("organisation"), col("owner")))
            ).alias("credited_to"),
        )
    )

    return counted.filter(col("owned_entity_count") == 0).select(
        "endpoint",
        "dataset",
        "organisation",
        "resource",
        when(col("resource").isNull(), lit(NO_RESOURCE))
        .when(col("entity_count") == 0, lit(NO_ENTITIES))
        .otherwise(lit(OTHER_ORGANISATION))
        .alias("reason"),
        "entity_count",
        "credited_to",
    )
