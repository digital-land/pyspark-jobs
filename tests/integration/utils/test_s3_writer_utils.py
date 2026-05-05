"""Integration tests for s3_writer_utils.write_delta."""

import os

import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from jobs.utils.s3_writer_utils import write_delta


def _read_delta(spark, path):
    return spark.read.format("delta").load(path)


def test_write_delta_writes_all_rows(spark, tmp_path):
    """write_delta writes all rows to the output path."""
    rows = [Row(dataset="ds-a", value=f"val-{i}") for i in range(5)]
    df = spark.createDataFrame(rows)

    write_delta(df, str(tmp_path / "output"), dataset="ds-a", partition_by=["dataset"])

    result = _read_delta(spark, str(tmp_path / "output"))
    assert result.count() == 5


def test_write_delta_creates_partition_directories(spark, tmp_path):
    """write_delta with partition_by creates Hive-style partition directories."""
    df = spark.createDataFrame(
        [Row(dataset="ds-a", value="x"), Row(dataset="ds-b", value="y")]
    )

    write_delta(df, str(tmp_path / "output"), dataset="ds-a", partition_by=["dataset"])

    dirs = os.listdir(str(tmp_path / "output"))
    assert any(d.startswith("dataset=") for d in dirs)


def test_write_delta_replace_where_replaces_only_matching_dataset(spark, tmp_path):
    """Second write replaces only the matching dataset partition, leaving others intact."""
    output = str(tmp_path / "output")

    df_v1 = spark.createDataFrame(
        [Row(dataset="ds-a", value="old"), Row(dataset="ds-b", value="keep")]
    )
    write_delta(df_v1, output, dataset="ds-a", partition_by=["dataset"])

    df_v2 = spark.createDataFrame([Row(dataset="ds-a", value="new")])
    write_delta(df_v2, output, dataset="ds-a", partition_by=["dataset"])

    result = _read_delta(spark, output)
    ds_a_rows = [r for r in result.collect() if r["dataset"] == "ds-a"]
    ds_b_rows = [r for r in result.collect() if r["dataset"] == "ds-b"]

    assert len(ds_a_rows) == 1
    assert ds_a_rows[0]["value"] == "new"
    assert len(ds_b_rows) == 1
    assert ds_b_rows[0]["value"] == "keep"


def test_write_delta_raises_on_schema_mismatch(spark, tmp_path):
    """write_delta raises ValueError if the incoming schema differs from the existing table."""
    output = str(tmp_path / "output")

    df_v1 = spark.createDataFrame([Row(dataset="ds-a", value="x")])
    write_delta(df_v1, output, dataset="ds-a", partition_by=["dataset"])

    df_v2 = spark.createDataFrame([Row(dataset="ds-a", value="x", extra="y")])
    with pytest.raises(ValueError, match="Schema mismatch"):
        write_delta(df_v2, output, dataset="ds-a", partition_by=["dataset"])


def test_write_delta_same_schema_twice_succeeds(spark, tmp_path):
    """Writing the same schema twice does not raise a schema mismatch error."""
    output = str(tmp_path / "output")

    schema = StructType(
        [
            StructField("dataset", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("fact", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("resource", StringType(), True),
            StructField("start_date", StringType(), True),
        ]
    )
    rows = [("ds-a", None, None, "1", "fact-1", 1, "res-1", None)]

    df_v1 = spark.createDataFrame(rows, schema=schema)
    write_delta(df_v1, output, dataset="ds-a", partition_by=["dataset"])

    df_v2 = spark.createDataFrame(rows, schema=schema)
    write_delta(df_v2, output, dataset="ds-a", partition_by=["dataset"])

    result = _read_delta(spark, output)
    assert result.count() == 1


def test_write_delta_raises_on_non_delta_existing_files(spark, tmp_path):
    """write_delta raises ValueError if the path contains non-Delta files."""
    output = tmp_path / "output"
    output.mkdir()
    (output / "stale.parquet").write_text("not a delta table")

    df = spark.createDataFrame([Row(dataset="ds-a", value="x")])
    with pytest.raises(ValueError, match="not a Delta table"):
        write_delta(df, str(output), dataset="ds-a", partition_by=["dataset"])
