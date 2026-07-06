"""
run_provision_quality.py

Entry point for the provision-quality job on Amazon EMR Serverless.

Computes provider/organisation quality per dataset across all collections and
writes three CSVs (provision-quality, dataset-quality, organisation-quality) to
the collection-data bucket. Phase 1: CSV only (for review). Phase 2 will add
Delta + serving Postgres writes.

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
    default=None,
    help="Root path with *-collection dirs + config (default: s3://{env}-collection-data/)",
)
@click.option(
    "--entity-data-path",
    default=None,
    help="Root path with flattened per-dataset entity CSVs "
    "(default: s3://digital-land-{env}-collection-dataset-hoisted/data/)",
)
@click.option(
    "--output-path",
    default=None,
    help="Where the CSVs are written (default: s3://{env}-collection-data/dataset/)",
)
@click.option("--debug", is_flag=True, default=False, help="Enable DEBUG logging")
def run(env, collection_data_path, entity_data_path, output_path, debug):
    """Generate provision-quality CSVs across all collections."""
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s",
    )
    for noisy in ("boto3", "botocore", "urllib3", "py4j", "pyspark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    job.generate_provision_quality(
        collection_data_path=collection_data_path or f"s3://{env}-collection-data/",
        entity_data_path=entity_data_path
        or f"s3://digital-land-{env}-collection-dataset-hoisted/data/",
        output_path=output_path or f"s3://{env}-collection-data/dataset/",
        env=env,
    )

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.error(f"__main__: error during execution - {e}", exc_info=True)
        sys.exit(1)
