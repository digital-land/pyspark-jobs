#!/usr/bin/env python3
"""Convert Parquet files to CSV using Polars."""

from pathlib import Path

import polars as pl

# Paths
parquet_dir = Path(
    "/Users/399182/MHCLG-githib/pyspark-jobs/src/analytics/data_volume_300/Polars/parquet_flattened_polars"
)
csv_dir = parquet_dir.parent / "csv_output"
csv_dir.mkdir(exist_ok=True)

# Convert all parquet files to CSV
for parquet_file in parquet_dir.glob("*.parquet"):
    csv_file = csv_dir / f"{parquet_file.stem}.csv"
    pl.read_parquet(parquet_file).write_csv(csv_file)
    print(f"Converted: {parquet_file.name} -> {csv_file.name}")

print(f"\nAll files converted to: {csv_dir}")
