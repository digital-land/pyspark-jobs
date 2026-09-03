"""
Integration tests for EntityPipeline.

Uses a real Spark session and local filesystem for reads/writes.
Parquet I/O uses local disk; S3 (_write_consumer_formats) uses moto.
Postgres is mocked.
"""

import os

import pytest

from jobs.pipeline.base import PipelineConfig
from jobs.pipeline.entity import EntityPipeline
from ._test_helpers import write_csv, write_parquet

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

ORGANISATION_ROWS = [
    {"organisation": "local-authority:ABC", "entity": "100"},
]

RESOURCE_ROWS = [
    {"resource": "res-001", "start-date": "2024-01-01", "end-date": ""},
    {"resource": "res-002", "start-date": "2024-01-01", "end-date": ""},
]


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

        write_parquet(
            spark,
            os.path.join(collection_dir, "transformed", dataset),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )
        write_csv(
            os.path.join(
                base, f"{collection}-collection", "collection", "resource.csv"
            ),
            ["resource", "start-date", "end-date"],
            RESOURCE_ROWS,
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
            "jobs.pipeline.entity.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.entity.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mocker.patch("jobs.pipeline.entity.write_dataframe_to_postgres_jdbc")
        mocker.patch("jobs.pipeline.entity.EntityPipeline._write_single_parquet")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        fact_resource_df = spark.read.format("delta").load(
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

        write_parquet(
            spark,
            os.path.join(collection_dir, "transformed", dataset),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )
        write_csv(
            os.path.join(
                base, f"{collection}-collection", "collection", "resource.csv"
            ),
            ["resource", "start-date", "end-date"],
            RESOURCE_ROWS,
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
            "jobs.pipeline.entity.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.entity.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mocker.patch("jobs.pipeline.entity.write_dataframe_to_postgres_jdbc")
        mocker.patch("jobs.pipeline.entity.EntityPipeline._write_single_parquet")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        fact_df = spark.read.format("delta").load(os.path.join(parquet_base, "fact"))
        expected_unique_facts = len({r["fact"] for r in TRANSFORMED_ROWS})
        assert fact_df.count() == expected_unique_facts

    def test_execute_writes_correct_entity_row_count(self, spark, tmp_path, mocker):
        """execute() pivots EAV to one row per unique entity."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        write_parquet(
            spark,
            os.path.join(collection_dir, "transformed", dataset),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )
        write_csv(
            os.path.join(
                base, f"{collection}-collection", "collection", "resource.csv"
            ),
            ["resource", "start-date", "end-date"],
            RESOURCE_ROWS,
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
            "jobs.pipeline.entity.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.entity.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mocker.patch("jobs.pipeline.entity.write_dataframe_to_postgres_jdbc")
        mocker.patch("jobs.pipeline.entity.EntityPipeline._write_single_parquet")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        entity_df = spark.read.format("delta").load(
            os.path.join(parquet_base, "entity")
        )
        expected_unique_entities = len({r["entity"] for r in TRANSFORMED_ROWS})
        assert entity_df.count() == expected_unique_entities

    def test_execute_writes_entity_and_field_to_fact_resource(
        self, spark, tmp_path, mocker
    ):
        """fact_resource carries entity and field, agreeing with the fact table."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        write_parquet(
            spark,
            os.path.join(collection_dir, "transformed", dataset),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )
        write_csv(
            os.path.join(
                base, f"{collection}-collection", "collection", "resource.csv"
            ),
            ["resource", "start-date", "end-date"],
            RESOURCE_ROWS,
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
            "jobs.pipeline.entity.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.entity.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mocker.patch("jobs.pipeline.entity.write_dataframe_to_postgres_jdbc")
        mocker.patch("jobs.pipeline.entity.EntityPipeline._write_single_parquet")

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
            database_url="postgresql://user:pass@localhost:5432/testdb",
        )

        EntityPipeline(config).run(collection=collection)

        fact_resource_df = spark.read.format("delta").load(
            os.path.join(parquet_base, "fact_resource")
        )
        assert fact_resource_df.filter("entity is null or field is null").count() == 0

        # A fact hash is derived from (entity, field, value), so the two tables
        # must agree on entity and field for any given fact.
        fact_df = spark.read.format("delta").load(os.path.join(parquet_base, "fact"))
        joined = fact_resource_df.alias("fr").join(fact_df.alias("f"), "fact")
        assert (
            joined.filter("fr.entity <> f.entity or fr.field <> f.field").count() == 0
        )

        expected = {(r["fact"], int(r["entity"]), r["field"]) for r in TRANSFORMED_ROWS}
        actual = {
            (r["fact"], r["entity"], r["field"])
            for r in fact_resource_df.select("fact", "entity", "field").collect()
        }
        assert actual == expected

    def test_execute_calls_postgres_write(self, spark, tmp_path, mocker):
        """execute() writes entity data to Postgres."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        write_parquet(
            spark,
            os.path.join(collection_dir, "transformed", dataset),
            TRANSFORMED_COLUMNS,
            TRANSFORMED_ROWS,
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )
        write_csv(
            os.path.join(
                base, f"{collection}-collection", "collection", "resource.csv"
            ),
            ["resource", "start-date", "end-date"],
            RESOURCE_ROWS,
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
            "jobs.pipeline.entity.flatten_json_column",
            return_value=mock_consumer_df,
        )
        mocker.patch(
            "jobs.pipeline.entity.ensure_schema_fields",
            return_value=mock_consumer_df,
        )
        mock_pg = mocker.patch("jobs.pipeline.entity.write_dataframe_to_postgres_jdbc")
        mocker.patch("jobs.pipeline.entity.EntityPipeline._write_single_parquet")

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

        write_parquet(
            spark,
            os.path.join(collection_dir, "transformed", dataset),
            TRANSFORMED_COLUMNS,
            [],
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity"],
            ORGANISATION_ROWS,
        )
        write_csv(
            os.path.join(
                base, f"{collection}-collection", "collection", "resource.csv"
            ),
            ["resource", "start-date", "end-date"],
            RESOURCE_ROWS,
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

    def test_write_single_parquet_writes_one_file_with_correct_data(
        self, spark, tmp_path
    ):
        """_write_single_parquet writes exactly one parquet file, readable
        back with the same rows as the source DataFrame."""
        config = PipelineConfig(
            spark=spark,
            dataset="test-dataset",
            env="local",
            collection_data_path=str(tmp_path),
            parquet_datasets_path=os.path.join(str(tmp_path), "parquet-output/"),
        )
        pipeline = EntityPipeline(config)

        df = spark.createDataFrame(
            [{"entity": "1001", "name": "Test Property A"}],
        )
        output_path = os.path.join(str(tmp_path), "dataset")

        pipeline._write_single_parquet(df, output_path, "test-dataset")

        target = os.path.join(output_path, "test-dataset.parquet")
        assert os.path.isfile(target)

        result_df = spark.read.parquet(target)
        assert result_df.count() == 1
        assert result_df.collect()[0]["entity"] == "1001"

        tmp_dir = os.path.join(output_path, "_tmp_test-dataset_parquet")
        assert not os.path.exists(tmp_dir), "temp directory was not cleaned up"
