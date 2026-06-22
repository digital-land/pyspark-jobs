"""
run_tasks.py

Entry point for the cross-collection task generation job on Amazon EMR Serverless.

Reads collection log and issue CSVs across all collections, filters to active
resources, and writes a plain Parquet task table to the parquet datasets bucket.

Usage:
1. Package code into a .whl using setup.py.
2. Upload both the .whl and this script to S3.
3. Submit the EMR Serverless job with:
   - `entryPoint`: S3 path to this script
   - `--py-files`: S3 path to the .whl file
"""

import logging
import sys

import click

from jobs import job

logger = logging.getLogger(__name__)


@click.command()
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
    help="Root path containing all *-collection directories (default: s3://{env}-collection-data/)",
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
    help="PostgreSQL connection URL (default: resolved from AWS Secrets Manager)",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable DEBUG logging (default: INFO)",
)
def run(env, collection_data_path, parquet_datasets_path, database_url, debug):
    """Generate task data from log and issue files across all collections."""
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s",
    )
    for logger_name in ("boto3", "botocore", "urllib3", "py4j", "pyspark"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    job.generate_tasks(
        collection_data_path=collection_data_path or f"s3://{env}-collection-data/",
        parquet_datasets_path=parquet_datasets_path or f"s3://{env}-parquet-datasets/",
        env=env,
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
