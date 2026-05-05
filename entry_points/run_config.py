"""
run_config.py

Entry point for building the old_entity config table from old-entity.csv files.

Reads old-entity.csv from config/pipeline/<collection>/ for every collection found
under collection_data_path, derives the dataset from the digital-land specification
entity ranges, filters records where the collection doesn't match, and writes a
single old_entity Delta table.

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

setup_logging(log_level="INFO", environment="production")
logger = get_logger(__name__)


@click.command()
@click.option(
    "--collection-data-path",
    required=True,
    type=str,
    help="Path to the collection data root (local or S3, e.g. s3://production-collection-data/)",
)
@click.option(
    "--parquet-datasets-path",
    required=True,
    type=str,
    help="S3 path to the parquet datasets bucket (e.g. s3://production-parquet-datasets/)",
)
def run(collection_data_path, parquet_datasets_path):
    """Build the old_entity config table from old-entity.csv files across all collections."""
    job.assemble_and_load_config(
        collection_data_path=collection_data_path,
        parquet_datasets_path=parquet_datasets_path,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.error(
            f"__main__: An error occurred during execution - {str(e)}", exc_info=True
        )
        sys.exit(1)
