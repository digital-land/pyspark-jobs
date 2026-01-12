"""
run_main.py

This script serves as the entry point for submitting PySpark jobs to Amazon EMR Serverless.

It is intentionally minimal and designed to be uploaded to S3 and referenced as the `entryPoint`
in EMR Serverless job submissions. The actual job logic resides in the packaged `.whl` file,
which is included via the `--py-files` parameter.

How it works:
- Imports the `main()` function from the packaged job module (e.g., jobs.main_collection_data).
- Calls `main()` to execute the PySpark job logic.
- Keeps the entry script lightweight and decoupled from the job logic for modularity and reuse.

Usage:
1. Package code into a `.whl` using setup.py.
2. Upload both the `.whl` and this script to S3.
3. Submit the EMR Serverless job with:
   - `entryPoint`: S3 path to this script
   - `--py-files`: S3 path to the `.whl` file

This pattern ensures clean separation of concerns and supports scalable, maintainable job deployments.
"""

from jobs.main_collection_data import main
from jobs.utils.logger_config import setup_logging, get_logger
import argparse
import sys

# Setup basic logging for the entry point
setup_logging(log_level="INFO", environment="production")
logger = get_logger(__name__)

# -------------------- Argument Parser --------------------
def parse_args():
    try:
        print("parse_args:Parsing command line arguments")
        parser = argparse.ArgumentParser(description="ETL Process for Collection Data with Enhanced Import Options")
        
        # Required arguments
        parser.add_argument("--load_type", type=str, required=True,
                            help="Type of load operation (e.g., full, incremental, sample)")
        parser.add_argument("--data_set", type=str, required=True,
                            help="Name of the dataset to process")
        parser.add_argument("--path", type=str, required=True,
                            help="Path to the dataset")
        parser.add_argument("--env", type=str, required=True,
                            help="Environment (e.g., development, staging, production, local)")
        
        # Import method control argument
        parser.add_argument("--use-jdbc", action="store_true",
                            help="Use JDBC import instead of Aurora S3 import (default: S3 import)")
        
        args = parser.parse_args()
        print(f"parse_args:Adding arguments for dataset: {args}")
        return args
    
    except argparse.ArgumentError as ae:
        logger.error(f"parse_args: Argument parsing error - {str(ae)}", exc_info=True)
        sys.exit(1)
    except SystemExit as se:
        logger.error(f"parse_args: SystemExit triggered - likely due to missing or invalid arguments", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"parse_args: Unexpected error - {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    try:
        args = parse_args()
        main(args)    
    except Exception as e:
        logger.error(f"__main__: An error occurred during execution - {str(e)}", exc_info=True)
        sys.exit(1)
