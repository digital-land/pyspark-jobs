"""Integration tests for s3_writer_utils write functions that need a real Spark session."""

import os

import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from jobs.utils.s3_writer_utils import (
    _nonempty_partition_bounds,
    write_delta,
    write_json_entities_s3,
)


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


def test_nonempty_partition_bounds_covers_all_rows_when_evenly_distributed(spark):
    """The common case: bounds are simply (0, num_partitions - 1)."""
    df = spark.range(1000).toDF("id")
    partitioned = df.repartition(4)
    first_idx, last_idx = _nonempty_partition_bounds(partitioned.rdd)
    assert (first_idx, last_idx) == (0, 3)


def test_nonempty_partition_bounds_handles_partitions_left_empty_by_repartition(spark):
    """Regression case for the bug write_json_entities_s3 hit in practice:
    df.repartition(n) with n close to the row count can leave partitions --
    including the first and/or last -- empty, because Spark's round-robin
    partitioning starts from a random offset per *input* partition rather
    than cycling through output partitions across the whole dataset. This
    confirms _nonempty_partition_bounds finds the true bounds regardless."""
    df = spark.range(50).toDF("id")
    partitioned = df.repartition(50)
    counts = partitioned.rdd.mapPartitionsWithIndex(
        lambda idx, rows: [(idx, sum(1 for _ in rows))]
    ).collect()
    truly_nonempty = [idx for idx, count in counts if count > 0]

    first_idx, last_idx = _nonempty_partition_bounds(partitioned.rdd)

    assert first_idx == min(truly_nonempty)
    assert last_idx == max(truly_nonempty)


class FakeS3Client:
    """Records put_object calls; no other S3 methods are needed for the
    row_count == 0 path, which never reaches the distributed upload."""

    def __init__(self):
        self.put_calls = []

    def put_object(self, Bucket, Key, Body):
        self.put_calls.append({"Bucket": Bucket, "Key": Key, "Body": Body})


def test_write_json_entities_s3_empty_dataframe_writes_empty_entities_array(spark):
    """An empty DataFrame short-circuits to a plain put_object -- this is
    the only path of write_json_entities_s3 testable without a real S3
    endpoint, since the non-empty path uploads from Spark executor
    subprocesses where a mocked boto3 client can't be observed."""
    df = spark.createDataFrame([], "id: int")
    fake_client = FakeS3Client()

    write_json_entities_s3(df, fake_client, "test-bucket", "dataset/test.json")

    assert fake_client.put_calls == [
        {
            "Bucket": "test-bucket",
            "Key": "dataset/test.json",
            "Body": '{"entities":[]}',
        }
    ]
