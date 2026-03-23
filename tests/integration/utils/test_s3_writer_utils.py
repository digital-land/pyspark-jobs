"""Integration tests for s3_writer_utils.write_parquet."""

import os

from pyspark.sql import Row

from jobs.utils.s3_writer_utils import write_parquet


def test_write_parquet_does_not_add_partition_columns_by_default(spark, tmp_path):
    """write_parquet with no partition_by does not inject year/month/day columns."""
    df = spark.createDataFrame([Row(id=1, value="test")])

    write_parquet(df, str(tmp_path / "output"))

    result = spark.read.parquet(str(tmp_path / "output"))
    assert "year" not in result.columns
    assert "month" not in result.columns
    assert "day" not in result.columns


def test_write_parquet_with_no_partition_by_writes_all_rows(spark, tmp_path):
    """write_parquet with no partition_by writes all rows to output path."""
    rows = [Row(id=i, value=f"val-{i}") for i in range(5)]
    df = spark.createDataFrame(rows)

    write_parquet(df, str(tmp_path / "output"))

    result = spark.read.parquet(str(tmp_path / "output"))
    assert result.count() == 5


def test_write_parquet_with_partition_by_creates_partition_directories(spark, tmp_path):
    """write_parquet with partition_by creates Hive-style partition directories."""
    df = spark.createDataFrame(
        [Row(dataset="my-dataset", value="x"), Row(dataset="other-dataset", value="y")]
    )

    write_parquet(df, str(tmp_path / "output"), partition_by=["dataset"])

    dirs = os.listdir(str(tmp_path / "output"))
    assert any(d.startswith("dataset=") for d in dirs)


def test_write_parquet_with_partition_by_preserves_row_count(spark, tmp_path):
    """write_parquet with partition_by writes all rows."""
    rows = [Row(dataset="ds-a", value=f"val-{i}") for i in range(3)]
    rows += [Row(dataset="ds-b", value=f"val-{i}") for i in range(2)]
    df = spark.createDataFrame(rows)

    write_parquet(df, str(tmp_path / "output"), partition_by=["dataset"])

    result = spark.read.parquet(str(tmp_path / "output"))
    assert result.count() == 5
