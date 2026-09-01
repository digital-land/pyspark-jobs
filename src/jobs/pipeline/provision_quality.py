"""ProvisionQualityPipeline: provider/organisation quality classification."""

import logging
from functools import reduce

from cloudpathlib import AnyPath
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    explode,
    first,
    lit,
    lower,
    split,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when

from jobs.pipeline.base import BasePipeline
from jobs.read import read_csvs_by_name
from jobs.utils.collection_paths import collection_files, collection_names
from jobs.utils.df_utils import normalise_column_names
from jobs.utils.postgres_writer_utils import write_table_to_postgres

logger = logging.getLogger(__name__)


def _owner_side(eq):
    """Owner lens: per (dataset, organisation) that owns entities, its quality
    (authoritative if it owns any authoritative entity, else some) and the count
    of entities it owns."""
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


def _live_datasets(dataset_df, env):
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


def _build_provision_quality(
    providers_df, ever_provided, org_df, entity_org_df, lookup_df, entity_quality_df
):
    """Base table: one row per (dataset, organisation) that has ever had an
    endpoint OR owns entities OR is a detected seeder. Nothing dropped; flags
    distinguish the cases. Owner/provider classification + seeder detection.

    A flag joined on after `keys` cannot bring rows with it — see
    is_designated_provider, which is only ever true on rows that exist for some
    other reason. That is why has_endpoint widens `keys` rather than just
    joining a column.
    """

    # map each owned entity to its owner organisation reference
    eq = entity_quality_df.join(
        org_df.select("organisation", "organisation_entity"),
        on="organisation_entity",
        how="inner",
    )

    owner_side = _owner_side(eq)
    active_orgs = org_df.filter(col("org_active")).select("organisation").distinct()
    seeder_side = _seeder_alt_sources(eq, lookup_df, entity_org_df, active_orgs)

    designated = entity_org_df.select("dataset", "organisation").distinct()

    keys = (
        ever_provided.unionByName(owner_side.select("dataset", "organisation"))
        .unionByName(seeder_side.select("dataset", "organisation"))
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
            ever_provided.withColumn("has_endpoint", lit(True)),
            on=["dataset", "organisation"],
            how="left",
        )
        .join(
            org_df.select("organisation", "organisation_name").distinct(),
            on="organisation",
            how="left",
        )
    )

    return pq.select(
        "dataset",
        "organisation",
        "organisation_name",
        coalesce(col("has_endpoint"), lit(False)).alias("has_endpoint"),
        coalesce(col("has_active_endpoint"), lit(False)).alias("has_active_endpoint"),
        coalesce(col("has_active_resource"), lit(False)).alias("has_active_resource"),
        coalesce(col("owns_entities"), lit(False)).alias("owns_entities"),
        coalesce(col("is_designated_provider"), lit(False)).alias(
            "is_designated_provider"
        ),
        coalesce(col("owner_quality"), col("seeder_quality")).alias("quality"),
        coalesce(col("owned_entity_count"), col("seeder_entity_count"), lit(0)).alias(
            "entity_count"
        ),
        lit(None).cast("double").alias("quality_score"),
    )


def _build_dataset_quality(provision_quality):
    """Rollup per dataset. Only classified (auth/some) rows count; entity total is
    from owned counts so seeded rows don't double-count."""
    classified = provision_quality.filter(col("quality").isNotNull())
    return (
        classified.groupBy("dataset")
        .agg(
            countDistinct(
                when(col("quality") == "authoritative", col("organisation"))
            ).alias("authoritative_organisations"),
            countDistinct(when(col("quality") == "some", col("organisation"))).alias(
                "some_organisations"
            ),
            countDistinct("organisation").alias("total_organisations"),
            spark_sum(
                when(col("owns_entities"), col("entity_count")).otherwise(0)
            ).alias("total_entities"),
        )
        .withColumn("quality_score", lit(None).cast("double"))
    )


def _build_organisation_quality(provision_quality):
    """Rollup per organisation across datasets."""
    classified = provision_quality.filter(col("quality").isNotNull())
    return (
        classified.groupBy("organisation")
        .agg(
            first("organisation_name", ignorenulls=True).alias("organisation_name"),
            countDistinct(
                when(col("quality") == "authoritative", col("dataset"))
            ).alias("authoritative_datasets"),
            countDistinct(when(col("quality") == "some", col("dataset"))).alias(
                "some_datasets"
            ),
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
    ("has_endpoint", "BOOLEAN"),
    ("has_active_endpoint", "BOOLEAN"),
    ("has_active_resource", "BOOLEAN"),
    ("owns_entities", "BOOLEAN"),
    ("is_designated_provider", "BOOLEAN"),
    ("quality", "TEXT"),
    ("entity_count", "BIGINT"),
    ("quality_score", "DOUBLE PRECISION"),
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

        # -- Providers (source.csv → who submits) -------------------------------
        # `pipelines` (';'-split) = dataset(s). Read once, then split two ways:
        # everyone who has ever had an endpoint, and the subset still live.
        collections = collection_names(base)
        source_files = collection_files(base, collections, "source.csv")
        logger.info(f"ProvisionQuality: Found {len(source_files)} source files")
        source_df = read_csvs_by_name(
            spark, source_files, ["endpoint", "end_date", "organisation", "pipelines"]
        )
        # Every organisation that has ever registered an endpoint for the dataset.
        endpoint_sources = (
            source_df.filter(col("endpoint").isNotNull() & (col("endpoint") != ""))
            .select(
                explode(split(col("pipelines"), ";")).alias("dataset"),
                col("organisation"),
                col("endpoint"),
                col("end_date"),
            )
            .distinct()
        )

        ever_provided = endpoint_sources.select("dataset", "organisation").distinct()

        active_sources = (
            endpoint_sources.filter(col("end_date").isNull() | (col("end_date") == ""))
            .select("dataset", "organisation", "endpoint")
            .distinct()
        )

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
        live_datasets_df = _live_datasets(dataset_df, self.config.env)

        # -- Entity + quality (SWAPPABLE SEAM) ----------------------------------
        entity_quality_df = self.load_entity_quality(spark, entity_data_path)

        # -- Classification + rollups ------------------------------------------
        provision_quality = _build_provision_quality(
            providers_df,
            ever_provided,
            org_df,
            entity_org_df,
            lookup_df,
            entity_quality_df,
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

        dataset_quality = _build_dataset_quality(provision_quality)
        organisation_quality = _build_organisation_quality(provision_quality)

        # -- Write (phase 1: CSV) ----------------------------------------------
        self._write_single_csv(
            provision_quality.orderBy(
                col("dataset"),
                lower(col("organisation_name")).asc_nulls_last(),
            ),
            output_path,
            "provision-quality",
        )
        self._write_single_csv(
            dataset_quality.orderBy("dataset"), output_path, "dataset-quality"
        )
        self._write_single_csv(
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

    def load_entity_quality(self, spark, entity_data_path):
        """SWAPPABLE SEAM. Phase 1: read the flattened per-dataset entity CSVs
        (one {dataset}.csv each) and return (dataset, organisation_entity,
        quality, entity). Read per file + union because each dataset's flattened
        CSV has its own column set — a single multi-file read would misalign
        headers. Future: swap the body to read the per-dataset Delta tables."""
        entity_files = [str(p) for p in AnyPath(entity_data_path).glob("*.csv")]
        logger.info(f"ProvisionQuality: Found {len(entity_files)} entity CSVs")
        frames = []
        for f in entity_files:
            dataset = AnyPath(f).stem
            df = spark.read.option("header", "true").csv(f)
            if "organisation-entity" not in df.columns or "quality" not in df.columns:
                logger.warning(
                    f"ProvisionQuality: {dataset} flattened CSV missing "
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

    def _write_single_csv(self, df, output_path, name):
        """Write df as a single header CSV at output_path/name.csv.

        Spark writes a directory of part-files, so we coalesce(1), then move the
        one part-file to the target name. The outputs are small aggregates
        (hundreds/thousands of rows), so a driver-side move is fine.
        """
        tmp_dir = AnyPath(output_path) / f"_tmp_{name}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            str(tmp_dir)
        )
        part = next(p for p in tmp_dir.glob("part-*.csv"))
        target = AnyPath(output_path) / f"{name}.csv"
        target.write_bytes(part.read_bytes())
        for p in tmp_dir.glob("*"):
            p.unlink()
        logger.info(f"ProvisionQuality: Wrote {target}")
