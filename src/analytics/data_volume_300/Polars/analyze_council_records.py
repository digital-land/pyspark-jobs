#!/usr/bin/env python3
from pathlib import Path

import polars as pl


def analyze_parquet_data(parquet_dir):
    """Analyze parquet files and calculate record counts per council"""
    parquet_files = list(parquet_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in {parquet_dir}")
        return

    print(f"Reading {len(parquet_files)} parquet files...\n")

    # Lazy read all parquet files
    df = pl.scan_parquet(str(parquet_dir / "*.parquet"))

    # Calculate record count per council
    result = (
        df.group_by("council_name")
        .agg(pl.count().alias("record_count"))
        .sort("record_count", descending=True)
        .collect()
    )

    # Display results
    print("Record Count by Council:")
    print("=" * 60)
    print(result)

    # Summary statistics
    total_records = result["record_count"].sum()
    total_councils = len(result)
    avg_records = result["record_count"].mean()

    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total Councils: {total_councils}")
    print(f"Total Records: {total_records:,}")
    print(f"Average Records per Council: {avg_records:.2f}")
    print(
        f"Max Records: {result['record_count'].max():,} ({result.filter(pl.col('record_count') == result['record_count'].max())['council_name'][0]})"
    )
    print(
        f"Min Records: {result['record_count'].min():,} ({result.filter(pl.col('record_count') == result['record_count'].min())['council_name'][0]})"
    )

    # Save results
    output_csv = parquet_dir.parent / "council_record_counts.csv"
    output_parquet = parquet_dir.parent / "council_record_counts.parquet"

    result.write_csv(output_csv)
    result.write_parquet(output_parquet)

    print(f"\nResults saved to:")
    print(f"  CSV: {output_csv}")
    print(f"  Parquet: {output_parquet}")


if __name__ == "__main__":
    parquet_dir = Path(__file__).parent / "parquet_flattened_polars"
    analyze_parquet_data(parquet_dir)
