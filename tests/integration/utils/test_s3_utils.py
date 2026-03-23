"""Integration tests for s3_utils.cleanup_dataset_data using moto mock S3."""

import boto3
import pytest
from moto import mock_aws

from jobs.utils.s3_utils import cleanup_dataset_data

BUCKET = "test-parquet-datasets"
OUTPUT_PATH = f"s3://{BUCKET}/entity"


@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3


def test_cleanup_dataset_data_deletes_matching_partition_files(s3_mock):
    """Deletes all files under dataset={name}/."""
    s3_mock.put_object(
        Bucket=BUCKET,
        Key="entity/dataset=test-dataset/year=2024/month=1/day=1/part-0.parquet",
        Body=b"data",
    )
    s3_mock.put_object(
        Bucket=BUCKET,
        Key="entity/dataset=test-dataset/year=2023/month=12/day=31/part-0.parquet",
        Body=b"data",
    )

    result = cleanup_dataset_data(OUTPUT_PATH, "test-dataset")

    assert result["objects_found"] == 2
    assert result["objects_deleted"] == 2

    response = s3_mock.list_objects_v2(
        Bucket=BUCKET, Prefix="entity/dataset=test-dataset/"
    )
    assert "Contents" not in response


def test_cleanup_dataset_data_leaves_other_dataset_partitions_intact(s3_mock):
    """Only deletes the specified dataset's files, not other datasets."""
    s3_mock.put_object(
        Bucket=BUCKET,
        Key="entity/dataset=test-dataset/part-0.parquet",
        Body=b"data",
    )
    s3_mock.put_object(
        Bucket=BUCKET,
        Key="entity/dataset=other-dataset/part-0.parquet",
        Body=b"data",
    )

    cleanup_dataset_data(OUTPUT_PATH, "test-dataset")

    response = s3_mock.list_objects_v2(
        Bucket=BUCKET, Prefix="entity/dataset=other-dataset/"
    )
    assert len(response.get("Contents", [])) == 1


def test_cleanup_dataset_data_returns_zero_when_no_files_exist(s3_mock):
    """Returns zero counts when no files exist for the dataset."""
    result = cleanup_dataset_data(OUTPUT_PATH, "test-dataset")

    assert result["objects_found"] == 0
    assert result["objects_deleted"] == 0


def test_cleanup_dataset_data_returns_summary_with_no_errors(s3_mock):
    """Returns summary dict with correct counts and empty errors list."""
    s3_mock.put_object(
        Bucket=BUCKET,
        Key="entity/dataset=my-dataset/file1.parquet",
        Body=b"x",
    )

    result = cleanup_dataset_data(OUTPUT_PATH, "my-dataset")

    assert result["objects_found"] == 1
    assert result["objects_deleted"] == 1
    assert result["errors"] == []


def test_cleanup_dataset_data_works_with_path_without_trailing_slash(s3_mock):
    """Works correctly when output_path has no trailing slash."""
    s3_mock.put_object(
        Bucket=BUCKET,
        Key="entity/dataset=my-dataset/part-0.parquet",
        Body=b"x",
    )

    result = cleanup_dataset_data(f"s3://{BUCKET}/entity", "my-dataset")

    assert result["objects_deleted"] == 1


def test_cleanup_dataset_data_works_with_path_with_trailing_slash(s3_mock):
    """Works correctly when output_path has a trailing slash."""
    s3_mock.put_object(
        Bucket=BUCKET,
        Key="entity/dataset=my-dataset/part-0.parquet",
        Body=b"x",
    )

    result = cleanup_dataset_data(f"s3://{BUCKET}/entity/", "my-dataset")

    assert result["objects_deleted"] == 1


def test_cleanup_dataset_data_deletes_all_files_across_sub_partitions(s3_mock):
    """Deletes all files across multiple year/month/day sub-partitions."""
    keys = [
        "entity/dataset=ds/year=2024/month=1/day=1/part-0.parquet",
        "entity/dataset=ds/year=2024/month=1/day=2/part-0.parquet",
        "entity/dataset=ds/year=2024/month=2/day=1/part-0.parquet",
        "entity/dataset=ds/year=2023/month=12/day=31/part-0.parquet",
    ]
    for key in keys:
        s3_mock.put_object(Bucket=BUCKET, Key=key, Body=b"x")

    result = cleanup_dataset_data(OUTPUT_PATH, "ds")

    assert result["objects_found"] == len(keys)
    assert result["objects_deleted"] == len(keys)
