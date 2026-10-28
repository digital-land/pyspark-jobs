"""
Integration tests for EntityPipeline.

Uses a real Spark session and local filesystem for reads/writes.
Parquet I/O uses local disk; S3 (_write_consumer_formats) uses moto.
Postgres is mocked.
"""

import json
import os
from datetime import date, datetime

import pytest
from pyspark import StorageLevel

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
    {
        "resource": "res-001",
        "datasets": "test-dataset",
        "start-date": "2024-01-01",
        "end-date": "",
    },
    {
        "resource": "res-002",
        "datasets": "test-dataset",
        "start-date": "2024-01-01",
        "end-date": "",
    },
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
            ["resource", "datasets", "start-date", "end-date"],
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

    def test_execute_keeps_rows_of_redirected_resource_replacement(
        self, spark, tmp_path, mocker
    ):
        """Transform saves a redirected (301) resource's replacement under the
        redirected resource's name, so its rows carry the replacement's hash,
        which resource.csv doesn't list. Those rows are assembled, as
        digital-land-python does."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")
        parquet_base = os.path.join(base, "parquet-output/")

        write_parquet(
            spark,
            os.path.join(collection_dir, "transformed", dataset),
            TRANSFORMED_COLUMNS,
            [
                (
                    row
                    if row["resource"] == "res-001"
                    else {**row, "resource": "res-fixed"}
                )
                for row in TRANSFORMED_ROWS
            ],
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
            ["resource", "datasets", "start-date", "end-date"],
            RESOURCE_ROWS,
        )
        write_csv(
            os.path.join(base, "config", "collection", collection, "old-resource.csv"),
            ["old-resource", "status", "resource", "notes"],
            [
                {
                    "old-resource": "res-002",
                    "status": "301",
                    "resource": "res-fixed",
                    "notes": "Migration from fixed file",
                }
            ],
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
        resources = {
            row["resource"]
            for row in fact_resource_df.select("resource").distinct().collect()
        }
        assert resources == {"res-001", "res-fixed"}
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
            ["resource", "datasets", "start-date", "end-date"],
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
            ["resource", "datasets", "start-date", "end-date"],
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
            ["resource", "datasets", "start-date", "end-date"],
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
            ["resource", "datasets", "start-date", "end-date"],
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

    def test_entity_df_is_persisted_for_every_writer_then_released(
        self, spark, tmp_path, mocker
    ):
        """Every writer must get entity_df already persisted, so the pivot runs
        once rather than once per action. Recomputing it per action filled
        title-boundary's executor disks in production. It is released once the
        Postgres write has finished."""
        dataset = "test-dataset"
        collection = "test-dataset"
        base = str(tmp_path)
        collection_dir = os.path.join(base, f"{collection}-collection")

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
            ["resource", "datasets", "start-date", "end-date"],
            RESOURCE_ROWS,
        )

        mocker.patch(
            "jobs.transform.entity_transformer.get_dataset_typology",
            return_value="geography",
        )
        # Record the storage level each writer sees at the moment it is called
        seen = {}
        mocker.patch.object(
            EntityPipeline,
            "_write_consumer_formats",
            autospec=True,
            side_effect=lambda self, df: seen.update(consumer=df.storageLevel),
        )
        mocker.patch.object(
            EntityPipeline,
            "_write_postgres",
            autospec=True,
            side_effect=lambda self, df: seen.update(postgres=df.storageLevel, df=df),
        )

        config = PipelineConfig(
            spark=spark,
            dataset=dataset,
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=os.path.join(base, "parquet-output/"),
        )

        EntityPipeline(config).run(collection=collection)

        assert seen["consumer"] == StorageLevel.DISK_ONLY
        assert seen["postgres"] == StorageLevel.DISK_ONLY
        assert seen["df"].storageLevel == StorageLevel.NONE

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
            ["resource", "datasets", "start-date", "end-date"],
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


class TestWriteConsumerFormats:
    """_write_consumer_formats end to end against local files."""

    @staticmethod
    def _write_specification(base):
        specification = base / "specification"
        specification.mkdir()
        (specification / "dataset-schema.csv").write_text(
            "dataset,schema\ntest-dataset,test-dataset\n"
        )
        (specification / "schema-field.csv").write_text(
            "schema,field\n"
            "test-dataset,name\n"
            "test-dataset,notes\n"
            "test-dataset,documentation-url\n"
        )
        (specification / "field.csv").write_text(
            "field,end-date\nname,\nnotes,\ndocumentation-url,\n"
        )

    @staticmethod
    def _entity_df(spark):
        rows = [
            (
                1002,
                'The "Old" Mill, Bath',
                '{"notes":"has a quote"}',
                "POINT (1 2)",
                None,
            ),
            (
                1001,
                None,
                '{"documentation_url":"https://example.com/1001"}',
                None,
                "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))",
            ),
            (1003, " No geometry ", None, None, None),
        ]
        return spark.createDataFrame(
            [
                (
                    "test-dataset",
                    None,
                    entity,
                    date(2024, 1, 2),
                    geometry,
                    json_value,
                    name,
                    600001,
                    point,
                    "test-dataset",
                    "authoritative",
                    f"REF{entity}",
                    None,
                    "geography",
                    datetime(2026, 10, 15, 10, 0),
                )
                for entity, name, json_value, point, geometry in rows
            ],
            "dataset string, end_date date, entity bigint, entry_date date, "
            "geometry string, json string, name string, organisation_entity bigint, "
            "point string, prefix string, quality string, reference string, "
            "start_date date, typology string, processed_timestamp timestamp",
        )

    def test_writes_download_files_in_the_published_formats(self, spark, tmp_path):
        self._write_specification(tmp_path)
        config = PipelineConfig(
            spark=spark,
            dataset="test-dataset",
            env="local",
            collection_data_path=f"{tmp_path}/",
            parquet_datasets_path=str(tmp_path / "parquet"),
        )

        EntityPipeline(config)._write_consumer_formats(self._entity_df(spark))

        dataset_dir = tmp_path / "dataset"
        csv_part = next((dataset_dir / "temp" / "test-dataset").glob("part-*.csv"))
        assert csv_part.read_text().splitlines() == [
            "dataset,end-date,entity,entry-date,geometry,name,"
            "organisation-entity,point,prefix,quality,reference,start-date,"
            "typology,documentation-url,notes",
            'test-dataset,,1001,2024-01-02,"MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))",,'
            "600001,,test-dataset,authoritative,REF1001,,geography,"
            "https://example.com/1001,",
            'test-dataset,,1002,2024-01-02,,"The ""Old"" Mill, Bath",600001,'
            "POINT (1 2),test-dataset,authoritative,REF1002,,geography,,has a quote",
            "test-dataset,,1003,2024-01-02,, No geometry ,600001,,test-dataset,"
            "authoritative,REF1003,,geography,,",
        ]

        parquet_df = spark.read.parquet(str(dataset_dir / "test-dataset.parquet"))
        assert dict(parquet_df.dtypes)["entity"] == "bigint"
        assert {t for c, t in parquet_df.dtypes if c != "entity"} == {"string"}
        assert [r["entity"] for r in parquet_df.collect()] == [1001, 1002, 1003]

        entities = json.loads((dataset_dir / "test-dataset.json").read_text())[
            "entities"
        ]
        assert [e["entity"] for e in entities] == ["1001", "1002", "1003"]
        assert entities[0]["documentation-url"] == "https://example.com/1001"
        assert "notes" not in entities[0]
        assert entities[1]["notes"] == "has a quote"
        assert "documentation-url" not in entities[1]
        assert all("geojson" not in e for e in entities)
        assert entities[2]["end-date"] == ""

        geojson = json.loads((dataset_dir / "test-dataset.geojson").read_text())
        assert [f["properties"]["entity"] for f in geojson["features"]] == [
            "1001",
            "1002",
        ]
