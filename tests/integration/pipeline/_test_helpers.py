"""Shared fixtures for jobs.pipeline integration tests.

Named so pytest's test-file collection (test_*.py / *_test.py) skips it.
"""

import csv
import os

from pyspark.sql.types import StringType, StructField, StructType


def write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_parquet(spark, path, fieldnames, rows):
    """Write rows as a parquet dataset at path (a directory), mirroring how
    EntityPipeline reads transformed data as {dataset}/*.parquet."""
    schema = StructType([StructField(f, StringType(), True) for f in fieldnames])
    data = [tuple(row.get(f, "") for f in fieldnames) for row in rows]
    spark.createDataFrame(data, schema=schema).write.mode("overwrite").parquet(path)
