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
    "entity",
    "entry-number",
    "field",
    "issue-type",
    "line-number",
    "dataset",
    "resource",
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


class TestIssuePipeline:
    def test_execute_writes_correct_issue_row_count(self, spark, tmp_path, mocker):
        """execute() writes all issue rows to parquet."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        write_csv(
            os.path.join(collection_dir, "issue", dataset, "issue.csv"),
            ISSUE_COLUMNS,
            ISSUE_ROWS,
        )

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        pipeline = IssuePipeline(config)
        pipeline.run(collection=collection)

        assert pipeline.result["status"] == "success"

        issue_df = spark.read.format("delta").load(os.path.join(parquet_base, "issue"))
        assert issue_df.count() == len(ISSUE_ROWS)

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
