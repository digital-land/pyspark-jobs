"""
run_maintenance.py

Entry point for the Delta Lake maintenance job on Amazon EMR Serverless.

Runs OPTIMIZE and VACUUM on all Delta tables in the parquet datasets bucket.
Intended to be scheduled nightly via Airflow after all dataset pipelines complete.

Usage:
1. Package code into a `.whl` using setup.py.
2. Upload both the `.whl` and this script to S3.
3. Submit the EMR Serverless job with:
   - `entryPoint`: S3 path to this script
   - `--py-files`: S3 path to the `.whl` file
"""

import sys

import click

from jobs import job
from jobs.utils.logger_config import get_logger, setup_logging

setup_logging(log_level="INFO")
logger = get_logger(__name__)


@click.command()
@click.option(
    "--parquet-datasets-path",
    required=True,
    type=str,
    help="S3 path to the parquet datasets bucket (e.g. s3://production-parquet-datasets/)",
)
@click.option(
    "--retention-hours",
    required=False,
    type=float,
    default=24,
    help="Hours of Delta history to retain after vacuum (default: 24)",
)
def run(parquet_datasets_path, retention_hours):
    """Run Delta Lake maintenance (OPTIMIZE + VACUUM) on all parquet datasets."""
    job.maintain_datasets(
        parquet_datasets_path=parquet_datasets_path,
        retention_hours=retention_hours,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.error(
            f"__main__: An error occurred during execution - {str(e)}", exc_info=True
        )
        sys.exit(1)
