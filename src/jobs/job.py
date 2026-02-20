"""
Job orchestration module.

Creates Spark session, validates inputs, and runs pipelines.
Pipeline implementations live in jobs.pipeline.
"""

import logging

from jobs.dbaccess.postgres_connectivity import get_aws_secret
from jobs.pipeline import EntityPipeline, IssuePipeline, PipelineConfig
from jobs.utils.db_url import build_database_url
from jobs.utils.logger_config import initialize_logging
from jobs.utils.s3_utils import validate_s3_path
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

    validate_s3_path(collection_data_path)

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
