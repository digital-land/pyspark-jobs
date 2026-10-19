"""
Integration tests for IssuePipeline.

Uses a real Spark session and local filesystem for reads/writes.
"""

import os

import pytest

from jobs.pipeline.base import PipelineConfig
from jobs.pipeline.issue import IssuePipeline

from ._test_helpers import write_csv

ISSUE_COLUMNS = [
    "dataset",
    "resource",
    "line-number",
    "entry-number",
    "field",
    "entity",
    "issue-type",
    "value",
    "message",
]

ISSUE_ROWS = [
    {
        "entity": "1001",
        "entry-number": "1",
        "field": "name",
        "issue-type": "warning",
        "line-number": "10",
        "dataset": "test-dataset",
        "resource": "res-001",
        "value": "Test Property A",
        "message": "Name format warning",
    },
]

# the oldest issue CSV layout, with no entity or message column
SEVEN_COLUMN_ISSUE_COLUMNS = [
    "dataset",
    "resource",
    "line-number",
    "entry-number",
    "field",
    "issue-type",
    "value",
]


def write_transformed(collection_dir, dataset, resource):
    """IssuePipeline only lists transformed files to learn which resources the
    dataset is assembled from, so an empty file is enough."""
    path = os.path.join(collection_dir, "transformed", dataset, f"{resource}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").close()


def make_config(spark, base, dataset):
    return PipelineConfig(
        spark=spark,
        dataset=dataset,
        env="local",
        collection_data_path=f"{base}/",
        parquet_datasets_path=os.path.join(base, "parquet-output/"),
        database_url="postgresql://user:pass@localhost:5432/testdb",
    )


class TestIssuePipeline:
    def test_execute_writes_correct_issue_row_count(self, spark, tmp_path, mocker):
        """execute() writes all issue rows to parquet."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        write_csv(
            os.path.join(collection_dir, "issue", dataset, "res-001.csv"),
            ISSUE_COLUMNS,
            ISSUE_ROWS,
        )
        write_transformed(collection_dir, dataset, "res-001")

        pipeline = IssuePipeline(make_config(spark, base, dataset))
        pipeline.run(collection=collection)

        assert pipeline.result["status"] == "success"

        issue_df = spark.read.format("delta").load(os.path.join(parquet_base, "issue"))
        assert issue_df.count() == len(ISSUE_ROWS)

    def test_execute_aligns_issue_csvs_with_different_layouts(self, spark, tmp_path):
        """A seven-column issue CSV read alongside a nine-column one keeps every
        value in its own column, whichever file Spark reads first."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")

        write_csv(
            os.path.join(collection_dir, "issue", dataset, "res-001.csv"),
            ISSUE_COLUMNS,
            ISSUE_ROWS,
        )
        write_csv(
            os.path.join(collection_dir, "issue", dataset, "res-000.csv"),
            SEVEN_COLUMN_ISSUE_COLUMNS,
            [
                {
                    "dataset": dataset,
                    "resource": "res-000",
                    "line-number": "2",
                    "entry-number": "1",
                    "field": "reference",
                    "issue-type": "missing reference",
                    "value": "",
                }
            ],
        )
        write_transformed(collection_dir, dataset, "res-001")
        write_transformed(collection_dir, dataset, "res-000")

        pipeline = IssuePipeline(make_config(spark, base, dataset))
        pipeline.run(collection=collection)

        assert pipeline.result["status"] == "success"
        rows = {
            r["resource"]: r
            for r in spark.read.format("delta")
            .load(os.path.join(base, "parquet-output", "issue"))
            .collect()
        }
        assert rows["res-000"]["issue_type"] == "missing reference"
        assert rows["res-000"]["field"] == "reference"
        assert rows["res-000"]["entity"] is None
        assert rows["res-001"]["issue_type"] == "warning"
        assert rows["res-001"]["entity"] == 1001
        assert rows["res-001"]["value"] == "Test Property A"
        assert rows["res-001"]["message"] == "Name format warning"

    def test_execute_skips_issues_of_resources_not_transformed(self, spark, tmp_path):
        """An issue CSV with no transformed file for the dataset belongs to a
        resource the dataset isn't assembled from, so its rows are left out."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")

        write_csv(
            os.path.join(collection_dir, "issue", dataset, "res-001.csv"),
            ISSUE_COLUMNS,
            ISSUE_ROWS,
        )
        write_csv(
            os.path.join(collection_dir, "issue", dataset, "res-retired.csv"),
            ISSUE_COLUMNS,
            [{**ISSUE_ROWS[0], "resource": "res-retired"}],
        )
        write_transformed(collection_dir, dataset, "res-001")

        pipeline = IssuePipeline(make_config(spark, base, dataset))
        pipeline.run(collection=collection)

        assert pipeline.result["status"] == "success"
        resources = {
            r["resource"]
            for r in spark.read.format("delta")
            .load(os.path.join(base, "parquet-output", "issue"))
            .collect()
        }
        assert resources == {"res-001"}

    def test_execute_raises_on_missing_input_path(self, spark, tmp_path):
        """execute() raises when input CSV path doesn't exist."""
        dataset = "test-dataset"
        base = str(tmp_path)

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=os.path.join(base, "parquet-output/"),
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        pipeline = IssuePipeline(config)
        with pytest.raises(Exception):
            pipeline.run(collection="test-dataset")

        assert pipeline.result["status"] == "failed"
