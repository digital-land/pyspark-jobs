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
import argparse

# -------------------- Argument Parser --------------------
def parse_args():
    print("parse_args:Parsing command line arguments")
    parser = argparse.ArgumentParser(description="ETL Process for Collection Data")
    parser.add_argument("--load_type", type=str, required=True,
                        help="Type of load operation (e.g., full, incremental)")
    parser.add_argument("--data_set", type=str, required=True,
                        help="Name of the dataset to process")
    args = parser.parse_args()
    print(f"parse_args:Adding arguments for dataset: {args}")
    return args

if __name__ == "__main__":
    args = parse_args()
    main(args)
