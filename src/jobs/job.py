"""
Job orchestration module.

Creates Spark session, validates inputs, and runs pipelines.
Pipeline implementations live in jobs.pipeline.
"""

import logging
from functools import reduce

from cloudpathlib import AnyPath, S3Path
from delta.tables import DeltaTable
from pyspark.sql.functions import lit

from jobs.config.schema import _REGISTRY
from jobs.dbaccess.postgres_connectivity import get_aws_secret
from jobs.pipeline import (
    ColumnFieldPipeline,
    DatasetResourcePipeline,
    EntityPipeline,
    IssuePipeline,
    PipelineConfig,
    TaskPipeline,
)
from jobs.transform.old_entity_transformer import fetch_dataset_df, transform_old_entity
from jobs.utils.db_url import build_database_url
from jobs.utils.df_utils import normalise_column_names
from jobs.utils.logger_config import initialize_logging
from jobs.utils.postgres_writer_utils import write_old_entity_to_postgres
from jobs.utils.s3_utils import list_delta_table_paths, validate_s3_path
from jobs.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)


def assemble_and_load_entity(
    collection_data_path,
    parquet_datasets_path,
    env,
    dataset,
    collection,
    database_url=None,
):
    """
    Orchestrate the ETL pipelines for entity and issue data.

    Validates inputs, creates Spark session, runs EntityPipeline
    and IssuePipeline, and handles cleanup.
    """
    initialize_logging(env)

    # Validate environment
    allowed_envs = ["development", "staging", "production", "local"]
    if env not in allowed_envs:
        raise ValueError(f"Invalid env: {env}. Must be one of {allowed_envs}")

    if isinstance(AnyPath(collection_data_path), S3Path):
        validate_s3_path(collection_data_path)
    else:
        logger.info(f"Local collection_data_path: {collection_data_path}")

    logger.info(f"Starting ETL pipelines for dataset {dataset}")

    spark = None
    try:
        spark = create_spark_session()
        if spark is None:
            raise RuntimeError("Failed to create Spark session")

        # Resolve database URL: use provided URL or fall back to Secrets Manager
        if database_url is None:
            logger.info("No database_url provided, resolving from AWS Secrets Manager")
            conn_params = get_aws_secret(env)
            database_url = build_database_url(conn_params)
        else:
            logger.info("Using provided database_url")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env=env,
            collection_data_path=collection_data_path,
            parquet_datasets_path=parquet_datasets_path,
            database_url=database_url,
        )

        entity_pipeline = EntityPipeline(config)
        entity_pipeline.run(collection=collection)

        issue_pipeline = IssuePipeline(config)
        issue_pipeline.run(collection=collection)

        dataset_resource_pipeline = DatasetResourcePipeline(config)
        dataset_resource_pipeline.run(collection=collection)

        column_field_pipeline = ColumnFieldPipeline(config)
        column_field_pipeline.run(collection=collection)

        report = [
            entity_pipeline.result,
            issue_pipeline.result,
            dataset_resource_pipeline.result,
            column_field_pipeline.result,
        ]
        logger.info(f"Pipeline report: {report}")

    except (ValueError, AttributeError, KeyError) as e:
        logger.exception("An error occurred during the ETL process: %s", str(e))
        raise
    except Exception as e:
        logger.exception("Unexpected error during the ETL process: %s", str(e))
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")


def assemble_and_load_config(
    collection_data_path: str,
    parquet_datasets_path: str,
    database_url: str,
):
    """
    Build the old_entity config table from old-entity.csv files across all collections.

    Reads old-entity.csv from config/pipeline/<collection>/ for every collection found,
    derives the dataset from the digital-land specification entity ranges, filters records
    where the collection or redirect target entity don't match, and writes to the
    old_entity parquet table and the old_entity Postgres table.

    Args:
        collection_data_path: Path to the collection data root (local or S3).
        parquet_datasets_path: S3 path to the parquet datasets bucket.
        database_url: PostgreSQL connection URL (postgresql://user:pass@host:port/dbname).
    """
    base = AnyPath(collection_data_path)
    config_pipeline_path = base / "config" / "pipeline"

    logger.info(
        f"assemble_and_load_config: Reading old-entity config from {config_pipeline_path}"
    )

    spark = None
    try:
        spark = create_spark_session()
        if spark is None:
            raise RuntimeError("Failed to create Spark session")

        collection_dfs = []
        for collection_dir in config_pipeline_path.iterdir():
            if not collection_dir.is_dir():
                continue
            old_entity_path = collection_dir / "old-entity.csv"
            if not old_entity_path.exists():
                continue
            collection_name = collection_dir.name
            logger.info(
                f"assemble_and_load_config: Reading old-entity for '{collection_name}'"
            )
            df = spark.read.option("header", "true").csv(str(old_entity_path))
            df = df.withColumn("collection", lit(collection_name))
            collection_dfs.append(df)

        if not collection_dfs:
            logger.warning(
                "assemble_and_load_config: No old-entity.csv files found, nothing to write"
            )
            return

        logger.info(
            f"assemble_and_load_config: Found {len(collection_dfs)} collections with old-entity data"
        )

        old_entity_df = reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True), collection_dfs
        )

        dataset_df = fetch_dataset_df(spark)
        result_df = transform_old_entity(old_entity_df, dataset_df)

        output_path = str(AnyPath(parquet_datasets_path) / "old_entity")
        result_df.write.format("delta").mode("overwrite").save(output_path)
        logger.info(
            f"assemble_and_load_config: Wrote old_entity Delta table to {output_path}"
        )

        write_old_entity_to_postgres(result_df, database_url)
        logger.info("assemble_and_load_config: Wrote old_entity to Postgres")

    except Exception as e:
        logger.exception(f"assemble_and_load_config: Unexpected error — {e}")
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("assemble_and_load_config: Spark session stopped")
            except Exception as e:
                logger.warning(
                    f"assemble_and_load_config: Error stopping Spark session — {e}"
                )


def migrate_datasets(parquet_datasets_path: str, allow_destructive: bool = False):
    """
    Migrate all registered Delta tables to match their schemas in schema.py.

    Safe changes (adding columns) are always applied. Destructive changes
    (removing columns, type changes) require allow_destructive=True.
    Skipped destructive changes are logged as warnings.

    Args:
        parquet_datasets_path: S3 path to the parquet datasets bucket.
        allow_destructive: If True, applies column removals and type changes.
    """
    logger.info(
        f"migrate_datasets: Starting migration for {parquet_datasets_path} "
        f"(allow_destructive={allow_destructive})"
    )

    spark = None
    try:
        spark = create_spark_session()
        if spark is None:
            raise RuntimeError("Failed to create Spark session")

        parquet_base = AnyPath(parquet_datasets_path)
        results = []

        for schema_name, schema in _REGISTRY.items():
            table_path = str(parquet_base / schema_name)
            logger.info(f"migrate_datasets: Checking {schema_name} at {table_path}")
            summary = schema.migrate(
                spark, table_path, allow_destructive=allow_destructive
            )
            summary["table"] = schema_name
            results.append(summary)
            logger.info(f"migrate_datasets: {schema_name} — {summary}")

        pending = [r for r in results if r.get("skipped")]
        if pending:
            logger.warning(
                "migrate_datasets: The following tables have destructive changes "
                "pending — re-run with allow_destructive=True to apply:"
            )
            for r in pending:
                logger.warning(f"  {r['table']}: {r['skipped']}")

        logger.info("migrate_datasets: Complete")

    except Exception as e:
        logger.exception(f"migrate_datasets: Unexpected error — {e}")
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("migrate_datasets: Spark session stopped")
            except Exception as e:
                logger.warning(f"migrate_datasets: Error stopping Spark session — {e}")


def maintain_datasets(parquet_datasets_path: str, retention_hours: float = 24):
    """
    Run maintenance operations on all Delta tables in the parquet datasets bucket.

    For each Delta table:
    1. OPTIMIZE — compacts small files into larger ones for faster reads.
    2. VACUUM — physically deletes files no longer referenced by the Delta log.

    Args:
        parquet_datasets_path: S3 path to the parquet datasets bucket (e.g. s3://env-parquet-datasets).
        retention_hours: Hours of history to retain after vacuum. Default 24.
    """
    logger.info(
        f"maintain_datasets: Starting maintenance for {parquet_datasets_path} "
        f"with retention_hours={retention_hours}"
    )

    spark = None
    try:
        spark = create_spark_session()
        if spark is None:
            raise RuntimeError("Failed to create Spark session")

        # Disable the minimum retention check so short periods (e.g. 24h) work
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        table_paths = list_delta_table_paths(AnyPath(parquet_datasets_path))

        if not table_paths:
            logger.warning(
                f"maintain_datasets: No Delta tables found at {parquet_datasets_path}"
            )
            return

        logger.info(
            f"maintain_datasets: Found {len(table_paths)} Delta tables to maintain"
        )

        results = []
        for path in table_paths:
            try:
                logger.info(f"maintain_datasets: Optimising {path}")
                DeltaTable.forPath(spark, path).optimize().executeCompaction()
                logger.info(f"maintain_datasets: Vacuuming {path}")
                DeltaTable.forPath(spark, path).vacuum(retention_hours)
                results.append({"path": path, "status": "ok"})
            except Exception as e:
                logger.error(f"maintain_datasets: Failed to maintain {path} — {e}")
                results.append({"path": path, "status": "error", "error": str(e)})

        errors = [r for r in results if r["status"] == "error"]
        logger.info(
            f"maintain_datasets: Complete — {len(results) - len(errors)} succeeded, "
            f"{len(errors)} failed"
        )
        for r in errors:
            logger.error(f"  Failed: {r['path']} — {r['error']}")

    except Exception as e:
        logger.exception(f"maintain_datasets: Unexpected error — {e}")
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("maintain_datasets: Spark session stopped")
            except Exception as e:
                logger.warning(f"maintain_datasets: Error stopping Spark session — {e}")


def generate_tasks(
    collection_data_path: str,
    parquet_datasets_path: str,
    env: str,
):
    """
    Generate task data from log and issue files across all collections.

    Reads collection log and issue CSVs from S3, filters to active resources,
    and writes a plain Parquet task table to parquet_datasets_path/task/.

    Args:
        collection_data_path: Root path containing all *-collection directories.
        parquet_datasets_path: S3 path to the parquet datasets bucket.
        env: Environment name (development, staging, production, local).
    """
    initialize_logging(env)

    allowed_envs = ["development", "staging", "production", "local"]
    if env not in allowed_envs:
        raise ValueError(f"Invalid env: {env}. Must be one of {allowed_envs}")

    if isinstance(AnyPath(collection_data_path), S3Path):
        validate_s3_path(collection_data_path)
    else:
        logger.info(
            f"generate_tasks: Local collection_data_path: {collection_data_path}"
        )

    logger.info("generate_tasks: Starting cross-collection task generation")

    spark = None
    try:
        spark = create_spark_session()
        if spark is None:
            raise RuntimeError("Failed to create Spark session")

        config = PipelineConfig(
            spark=spark,
            dataset="",
            env=env,
            collection_data_path=collection_data_path,
            parquet_datasets_path=parquet_datasets_path,
        )

        task_pipeline = TaskPipeline(config)
        task_pipeline.run()

        logger.info(f"generate_tasks: Result — {task_pipeline.result}")

    except (ValueError, AttributeError, KeyError) as e:
        logger.exception("generate_tasks: Error during task generation: %s", str(e))
        raise
    except Exception as e:
        logger.exception("generate_tasks: Unexpected error: %s", str(e))
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("generate_tasks: Spark session stopped")
            except Exception as e:
                logger.warning(f"generate_tasks: Error stopping Spark session — {e}")
