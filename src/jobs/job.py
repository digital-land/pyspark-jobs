"""
Job orchestration module.

Creates Spark session, validates inputs, and runs pipelines.
Pipeline implementations live in jobs.pipeline.
"""

import logging

from cloudpathlib import AnyPath, S3Path
from delta.tables import DeltaTable

from jobs.dbaccess.postgres_connectivity import get_aws_secret
from jobs.pipeline import EntityPipeline, IssuePipeline, PipelineConfig
from jobs.utils.db_url import build_database_url
from jobs.utils.logger_config import initialize_logging
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

        report = [entity_pipeline.result, issue_pipeline.result]
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
