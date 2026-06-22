"""
run_migrate.py

Entry point for migrating Delta Lake tables to match their registered schemas.

Compares each registered schema in schema.py against the existing Delta table
and applies any differences. Safe changes (adding columns) are always applied.
Destructive changes (removing columns, type changes) require --allow-destructive.

Skipped destructive changes are logged as warnings so the operator knows
what is pending before re-running with --allow-destructive.

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

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--parquet-datasets-path",
    required=True,
    type=str,
    help="S3 path to the parquet datasets bucket (e.g. s3://production-parquet-datasets/)",
)
@click.option(
    "--allow-destructive",
    is_flag=True,
    default=False,
    help="Allow destructive changes: column removals and type changes. "
    "Without this flag, only safe changes (adding columns) are applied.",
)
def run(parquet_datasets_path, allow_destructive):
    """Migrate Delta tables to match registered schemas."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s",
    )
    for logger_name in ("boto3", "botocore", "urllib3", "py4j", "pyspark"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    job.migrate_datasets(
        parquet_datasets_path=parquet_datasets_path,
        allow_destructive=allow_destructive,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.error(
            f"__main__: An error occurred during execution - {str(e)}", exc_info=True
        )
        sys.exit(1)
