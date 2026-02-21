"""
run_main.py

This script serves as the entry point for submitting the assemble and load PySpark job to Amazon EMR Serverless.

It is intentionally minimal and designed to be uploaded to S3 and referenced as the `entryPoint`
in EMR Serverless job submissions. The actual job logic resides in the packaged `.whl` file,
which is included via the `--py-files` parameter.

How it works:
- Imports `assemble_and_load_entity` from `jobs.job` and calls it with the CLI arguments.
- Executes the full ETL pipeline (EntityPipeline + IssuePipeline).
- Keeps the entry script lightweight and decoupled from the job logic for modularity and reuse.

Usage:
1. Package code into a `.whl` using setup.py.
2. Upload both the `.whl` and this script to S3.
3. Submit the EMR Serverless job with:
   - `entryPoint`: S3 path to this script
   - `--py-files`: S3 path to the `.whl` file

This pattern ensures clean separation of concerns and supports scalable, maintainable job deployments.
"""

import sys
import click

from jobs import job
from jobs.utils.logger_config import get_logger, setup_logging

# Setup basic logging for the entry point
setup_logging(log_level="INFO", environment="production")
logger = get_logger(__name__)


@click.command()
@click.option(
    "--dataset",
    required=True,
    type=str,
    help="Name of the dataset to process",
)
@click.option(
    "--collection",
    required=True,
    type=str,
    help="Collection that the dataset belongs to",
)
@click.option(
    "--env",
    required=True,
    type=click.Choice(
        ["development", "staging", "production", "local"], case_sensitive=False
    ),
    help="Environment (e.g., development, staging, production, local)",
)
@click.option(
    "--collection-data-path",
    required=False,
    type=str,
    default=None,
    help="Output path for collection data (default: s3://{env}-collection-data/)",
)
@click.option(
    "--parquet-datasets-path",
    required=False,
    type=str,
    default=None,
    help="Output path for parquet datasets (default: s3://{env}-parquet-datasets/)",
)
@click.option(
    "--database-url",
    required=False,
    type=str,
    default=None,
    help="PostgreSQL database URL (default: resolved from AWS Secrets Manager)",
)
def run(
    dataset,
    collection,
    env,
    collection_data_path,
    parquet_datasets_path,
    database_url,
):
    """ETL Process for Collection Data."""
    job.assemble_and_load_entity(
        collection_data_path=collection_data_path or f"s3://{env}-collection-data/",
        parquet_datasets_path=parquet_datasets_path or f"s3://{env}-parquet-datasets/",
        env=env,
        dataset=dataset,
        collection=collection,
        database_url=database_url,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.error(
            f"__main__: An error occurred during execution - {str(e)}", exc_info=True
        )
        sys.exit(1)
