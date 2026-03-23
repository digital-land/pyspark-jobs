"""
Integration tests for EntityPipeline and IssuePipeline.

Uses a real Spark session and local filesystem for reads/writes.
Parquet I/O uses local disk; S3 (_write_consumer_formats) uses moto.
Postgres is mocked.
"""

import csv
import os

import pytest

from jobs.pipeline import EntityPipeline, IssuePipeline, PipelineConfig

# -- Test data ----------------------------------------------------------------

TRANSFORMED_COLUMNS = [
    "entity",
    "field",
    "value",
    "entry-date",
    "entry-number",
    "priority",
    "end-date",
    "start-date",
    "fact",
    "reference-entity",
    "resource",
]

TRANSFORMED_ROWS = [
    {
        "entity": "1001",
        "field": "name",
        "value": "Test Property A",
        "entry-date": "2024-01-15",
        "entry-number": "1",
        "priority": "2",
        "end-date": "",
        "start-date": "2024-01-01",
        "fact": "fact-001",
        "reference-entity": "",
        "resource": "res-001",
    },
    {
        "entity": "1001",
        "field": "reference",
        "value": "REF-001",
        "entry-date": "2024-01-15",
        "entry-number": "1",
        "priority": "1",
        "end-date": "",
        "start-date": "2024-01-01",
        "fact": "fact-002",
        "reference-entity": "1001",
        "resource": "res-001",
    },
    {
        "entity": "1001",
        "field": "prefix",
        "value": "test",
        "entry-date": "2024-01-15",
        "entry-number": "1",
        "priority": "1",
        "end-date": "",
        "start-date": "2024-01-01",
        "fact": "fact-003",
        "reference-entity": "",
        "resource": "res-002",
    },
    {
        "entity": "1001",
        "field": "organisation",
        "value": "local-authority:ABC",
        "entry-date": "2024-01-15",
        "entry-number": "1",
        "priority": "1",
        "end-date": "",
        "start-date": "2024-01-01",
        "fact": "fact-004",
        "reference-entity": "",
        "resource": "res-002",
    },
    {
        "entity": "1001",
        "field": "entry-date",
        "value": "2024-01-15",
        "entry-date": "2024-01-15",
        "entry-number": "1",
        "priority": "1",
        "end-date": "",
        "start-date": "2024-01-01",
        "fact": "fact-005",
        "reference-entity": "",
        "resource": "res-001",
    },
    {
        "entity": "1001",
        "field": "start-date",
        "value": "2024-01-01",
        "entry-date": "2024-01-15",
        "entry-number": "1",
        "priority": "1",
        "end-date": "",
        "start-date": "2024-01-01",
        "fact": "fact-006",
        "reference-entity": "",
        "resource": "res-001",
    },
]

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

ORGANISATION_ROWS = [
    {"organisation": "local-authority:ABC", "entity": "100"},
]


# -- Helpers ------------------------------------------------------------------


def _write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# -- EntityPipeline tests ----------------------------------------------------


class TestEntityPipeline:
    def test_execute_writes_correct_fact_resource_row_count(
        self, spark, tmp_path, mocker
    ):
        """execute() preserves all input rows in fact_resource parquet."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        _write_csv(
            os.path.join(collection_dir, "transformed", dataset, "data.csv"),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        _write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )

        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )
        mock_consumer_df = mocker.MagicMock()
        mock_consumer_df.columns = []
        mock_consumer_df.count.return_value = 0
        mock_consumer_df.toLocalIterator.return_value = iter([])
        mock_consumer_df.repartition.return_value.toLocalIterator.return_value = iter(
            []
        )
        mocker.patch(
            "jobs.pipeline.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mocker.patch("jobs.pipeline.write_dataframe_to_postgres_jdbc")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        fact_resource_df = spark.read.parquet(
            os.path.join(parquet_base, "fact_resource")
        )
        assert fact_resource_df.count() == len(TRANSFORMED_ROWS)

    def test_execute_writes_correct_fact_row_count(self, spark, tmp_path, mocker):
        """execute() deduplicates facts to one row per unique fact."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        _write_csv(
            os.path.join(collection_dir, "transformed", dataset, "data.csv"),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        _write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )

        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )
        mock_consumer_df = mocker.MagicMock()
        mock_consumer_df.columns = []
        mock_consumer_df.count.return_value = 0
        mock_consumer_df.toLocalIterator.return_value = iter([])
        mock_consumer_df.repartition.return_value.toLocalIterator.return_value = iter(
            []
        )
        mocker.patch(
            "jobs.pipeline.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mocker.patch("jobs.pipeline.write_dataframe_to_postgres_jdbc")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        fact_df = spark.read.parquet(os.path.join(parquet_base, "fact"))
        expected_unique_facts = len({r["fact"] for r in TRANSFORMED_ROWS})
        assert fact_df.count() == expected_unique_facts

    def test_execute_writes_correct_entity_row_count(self, spark, tmp_path, mocker):
        """execute() pivots EAV to one row per unique entity."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        _write_csv(
            os.path.join(collection_dir, "transformed", dataset, "data.csv"),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        _write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )

        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )
        mock_consumer_df = mocker.MagicMock()
        mock_consumer_df.columns = []
        mock_consumer_df.count.return_value = 0
        mock_consumer_df.toLocalIterator.return_value = iter([])
        mock_consumer_df.repartition.return_value.toLocalIterator.return_value = iter(
            []
        )
        mocker.patch(
            "jobs.pipeline.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mocker.patch("jobs.pipeline.write_dataframe_to_postgres_jdbc")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        entity_df = spark.read.parquet(os.path.join(parquet_base, "entity"))
        expected_unique_entities = len({r["entity"] for r in TRANSFORMED_ROWS})
        assert entity_df.count() == expected_unique_entities

    def test_execute_calls_postgres_write(self, spark, tmp_path, mocker):
        """execute() writes entity data to Postgres."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        _write_csv(
            os.path.join(collection_dir, "transformed", dataset, "data.csv"),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        _write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )

        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )
        mock_consumer_df = mocker.MagicMock()
        mock_consumer_df.columns = []
        mock_consumer_df.count.return_value = 0
        mock_consumer_df.toLocalIterator.return_value = iter([])
        mock_consumer_df.repartition.return_value.toLocalIterator.return_value = iter(
            []
        )
        mocker.patch(
            "jobs.pipeline.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mock_pg = mocker.patch("jobs.pipeline.write_dataframe_to_postgres_jdbc")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        assert mock_pg.call_count == 1

    def test_execute_raises_value_error_on_empty_input(self, spark, tmp_path, mocker):
        """execute() raises ValueError if transformed data is empty."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")

        _write_csv(
            os.path.join(collection_dir, "transformed", dataset, "data.csv"),
            TRANSFORMED_COLUMNS,
            [],
        )
        _write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=os.path.join(base, "parquet-output/"),
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        pipeline = EntityPipeline(config)
        with pytest.raises(ValueError, match="empty"):
            pipeline.run(collection=collection)

        assert pipeline.result["status"] == "failed"


# -- IssuePipeline tests ------------------------------------------------------


class TestIssuePipeline:
    def test_execute_writes_correct_issue_row_count(self, spark, tmp_path, mocker):
        """execute() writes all issue rows to parquet."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        _write_csv(
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

        issue_df = spark.read.parquet(os.path.join(parquet_base, "issue"))
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
