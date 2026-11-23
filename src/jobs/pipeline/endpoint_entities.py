"""Which active endpoints give their organisation no entities (config#3002).

Raised per endpoint and dataset, from the endpoint's current resource: the facts
that resource produced (fact_resource) tell us its entities, and the flattened
entity CSVs tell us who owns each one. An organisation can own entities through
one endpoint while another of its endpoints gives it nothing, so this is not the
same as provision_quality's owns_entities, which is per (dataset, organisation).

A reason is only given when there is evidence for it. A resource missing from
fact_resource has not necessarily produced nothing: its dataset may not have been
rebuilt since it was collected. So "no entities" needs the pipeline's own
dataset-resource log to say so, and "another organisation" needs at least one
owner to name. Anything else is UNCONFIRMED, counted in the job log and not
raised.
"""

import logging

from cloudpathlib import AnyPath
from pyspark.sql.functions import (
    col,
    collect_set,
    countDistinct,
    explode,
    lit,
    size,
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

# Not a task reason: an endpoint gave its organisation no entities, but nothing
# says why, so no task is raised for it.
UNCONFIRMED = "unconfirmed"


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
    zero_entity_resources_df,
    entity_quality_df,
    org_df,
    live_datasets_df,
    rule,
    restrict_to_active_orgs,
):
    """Active (endpoint, dataset, organisation) whose current resource gives the
    organisation no entities, with the reason and who the entities went to.
    Rows whose reason cannot be confirmed have reason UNCONFIRMED; the caller
    counts and drops them.

    current_resources_df: endpoint, resource — one row per endpoint that has
    collected anything (the task pipeline's active resources). An endpoint not in
    it has never been collected from.
    fact_resource_df: dataset, resource, entity.
    zero_entity_resources_df: dataset, resource — the resources whose
    dataset-resource log records an entity-count of 0 for that dataset.
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

    logged_zero = zero_entity_resources_df.select("dataset", "resource").distinct()
    logged_zero = logged_zero.withColumn("logged_zero", lit(True))

    return (
        counted.filter(col("owned_entity_count") == 0)
        .join(logged_zero, on=["dataset", "resource"], how="left")
        .select(
            "endpoint",
            "dataset",
            "organisation",
            "resource",
            when(col("resource").isNull(), lit(NO_RESOURCE))
            # No facts is only "no entities" when the pipeline logged a count of
            # 0; otherwise the resource may simply not have been processed yet
            .when(
                (col("entity_count") == 0) & col("logged_zero").isNotNull(),
                lit(NO_ENTITIES),
            )
            # Entities, none of them this organisation's: only when at least one
            # owner is known, so the task can say who they went to
            .when(
                (col("entity_count") > 0) & (size(col("credited_to")) > 0),
                lit(OTHER_ORGANISATION),
            )
            .otherwise(lit(UNCONFIRMED))
            .alias("reason"),
            "entity_count",
            "credited_to",
        )
    )
