"""
run_config.py

Entry point for building the old_entity config table from old-entity.csv files.

Reads old-entity.csv from config/pipeline/<collection>/ for every collection found
under collection_data_path, derives the dataset from the digital-land specification
entity ranges, filters invalid records, and writes to the old_entity parquet table
and Postgres table.

Usage:
1. Package code into a `.whl` using setup.py.
2. Upload both the `.whl` and this script to S3.
3. Submit the EMR Serverless job with:
   - `entryPoint`: S3 path to this script
   - `--py-files`: S3 path to the `.whl` file
"""

import logging
import sys

import click

from jobs import job
from jobs.dbaccess.postgres_connectivity import get_aws_secret
from jobs.utils.db_url import build_database_url

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--env",
    required=True,
    type=click.Choice(
        ["development", "staging", "production", "local"], case_sensitive=False
    ),
    help="Environment (development, staging, production, local)",
)
@click.option(
    "--collection-data-path",
    required=False,
    type=str,
    default=None,
    help="Path to the collection data root (default: s3://{env}-collection-data/)",
)
@click.option(
    "--parquet-datasets-path",
    required=False,
    type=str,
    default=None,
    help="S3 path to the parquet datasets bucket (default: s3://{env}-parquet-datasets/)",
)
@click.option(
    "--database-url",
    required=False,
    type=str,
    default=None,
    help="PostgreSQL database URL (default: resolved from AWS Secrets Manager)",
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable DEBUG logging (default: INFO)",
)
def run(env, collection_data_path, parquet_datasets_path, database_url, debug):
    """Build the old_entity config table from old-entity.csv files across all collections."""
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s",
    )
    for logger_name in ("boto3", "botocore", "urllib3", "py4j", "pyspark"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    if database_url is None:
        logger.info("No database_url provided, resolving from AWS Secrets Manager")
        conn_params = get_aws_secret(env)
        database_url = build_database_url(conn_params)

    job.assemble_and_load_config(
        collection_data_path=collection_data_path or f"s3://{env}-collection-data/",
        parquet_datasets_path=parquet_datasets_path or f"s3://{env}-parquet-datasets/",
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
