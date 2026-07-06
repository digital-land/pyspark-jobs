"""
Integration tests for EntityPipeline, IssuePipeline and TaskPipeline

Uses a real Spark session and local filesystem for reads/writes.
Parquet I/O uses local disk; S3 (_write_consumer_formats) uses moto.
Postgres is mocked.
"""

import csv
import os

import pytest

from jobs.pipeline import (
    EntityPipeline,
    IssuePipeline,
    PipelineConfig,
    TaskPipeline,
    _active_resources_from_log,
    _backfill_dataset_from_source,
    _backfill_organisation_from_source,
    _build_dataset_quality,
    _build_organisation_quality,
    _build_provision_quality,
)

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

        entity_df = spark.read.format("delta").load(
            os.path.join(parquet_base, "entity")
        )
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


# -- TaskPipeline tests -------------------------------------------------------


class TestTaskPipeline:

    def test_no_duplicate_references_in_output(self, spark, tmp_path, mocker):
        """TaskPipeline produces no duplicate references even when the same
        endpoint fails on multiple collection days — realistic log.csv scenario
        where extra columns (entry-date, bytes, elapsed) would previously prevent
        .distinct() from deduplicating repeated failures."""
        base = str(tmp_path)
        parquet_base = os.path.join(base, "parquet-output/")

        # Same endpoint failing on two different dates — the key scenario.
        # entry-date and elapsed differ, which previously caused .distinct()
        # to keep both rows and produce duplicate reference hashes.
        _write_csv(
            os.path.join(base, "test-collection", "collection", "log.csv"),
            [
                "endpoint",
                "resource",
                "status",
                "exception",
                "entry-date",
                "bytes",
                "elapsed",
            ],
            [
                {
                    "endpoint": "http://endpoint-a",
                    "resource": "resource-aaa",
                    "status": "404",
                    "exception": "",
                    "entry-date": "2026-01-01",
                    "bytes": "200",
                    "elapsed": "1.2",
                },
                {
                    "endpoint": "http://endpoint-a",
                    "resource": "resource-aaa",
                    "status": "404",
                    "exception": "",
                    "entry-date": "2026-01-02",
                    "bytes": "200",
                    "elapsed": "1.1",
                },
                {
                    "endpoint": "http://endpoint-a",
                    "resource": "resource-aaa",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-03",
                    "bytes": "200",
                    "elapsed": "1.0",
                },
            ],
        )

        _write_csv(
            os.path.join(
                base, "test-collection", "issue", "dataset-a", "resource-aaa.csv"
            ),
            ["resource", "issue_type", "field", "value", "dataset"],
            [
                {
                    "resource": "resource-aaa",
                    "issue_type": "invalid-geometry",
                    "field": "geometry",
                    "value": "POLYGON((0 0))",
                    "dataset": "dataset-a",
                }
            ],
        )

        mocker.patch(
            "jobs.pipeline._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("invalid-geometry", "error", "external")],
                ["issue_type", "severity", "responsibility"],
            ),
        )

        config = PipelineConfig(
            spark=spark,
            dataset="",
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
        )

        TaskPipeline(config).run()

        tasks_df = spark.read.format("delta").load(os.path.join(parquet_base, "task"))
        references = [row["reference"] for row in tasks_df.collect()]
        assert len(references) == len(
            set(references)
        ), f"{len(references) - len(set(references))} duplicate references found"


class TestBackfillDatasetFromSource:

    def _make_log_df(self, spark, rows):
        return spark.createDataFrame(
            rows,
            ["endpoint", "resource", "status", "exception", "dataset"],
        )

    def _make_source_df(self, spark, rows):
        return spark.createDataFrame(rows, ["endpoint", "dataset"])

    def test_fills_in_dataset_for_failed_row(self, spark):
        """A row with no dataset gets its dataset from the source lookup."""
        log_df = self._make_log_df(spark, [("endpoint-aaa", "", "404", "", "")])
        source_df = self._make_source_df(spark, [("endpoint-aaa", "conservation-area")])

        result = _backfill_dataset_from_source(log_df, source_df)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["dataset"] == "conservation-area"

    def test_existing_dataset_is_not_changed(self, spark):
        """A row that already has a dataset is left untouched."""
        log_df = self._make_log_df(
            spark, [("endpoint-aaa", "resource-aaa", "200", "", "conservation-area")]
        )
        source_df = self._make_source_df(spark, [("endpoint-aaa", "something-else")])

        result = _backfill_dataset_from_source(log_df, source_df)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["dataset"] == "conservation-area"

    def test_multi_dataset_endpoint_produces_one_row_per_dataset(self, spark):
        """A failing endpoint that serves two datasets produces two task rows."""
        log_df = self._make_log_df(
            spark, [("endpoint-aaa", "", "500", "Connection refused", "")]
        )
        source_df = self._make_source_df(
            spark,
            [
                ("endpoint-aaa", "tree-preservation-order"),
                ("endpoint-aaa", "tree"),
            ],
        )

        result = _backfill_dataset_from_source(log_df, source_df)

        datasets = {row["dataset"] for row in result.collect()}
        assert datasets == {"tree-preservation-order", "tree"}

    def test_endpoint_not_in_source_keeps_empty_dataset(self, spark):
        """A failing endpoint with no source entry stays with dataset=''."""
        log_df = self._make_log_df(spark, [("endpoint-unknown", "", "404", "", "")])
        source_df = self._make_source_df(
            spark, [("endpoint-other", "conservation-area")]
        )

        result = _backfill_dataset_from_source(log_df, source_df)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["dataset"] == ""


class TestBackfillOrganisationFromSource:

    def _make_log_df(self, spark, rows):
        return spark.createDataFrame(
            rows,
            ["endpoint", "resource", "status", "exception", "dataset", "organisation"],
        )

    def _make_source_df(self, spark, rows):
        return spark.createDataFrame(rows, ["endpoint", "organisation"])

    def test_fills_in_organisation_for_failed_row(self, spark):
        """A row with no organisation gets its organisation from the source lookup."""
        log_df = self._make_log_df(
            spark, [("endpoint-aaa", "", "404", "", "conservation-area", "")]
        )
        source_df = self._make_source_df(spark, [("endpoint-aaa", "organisation:1")])

        result = _backfill_organisation_from_source(log_df, source_df)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["organisation"] == "organisation:1"

    def test_existing_organisation_is_not_changed(self, spark):
        """A row that already has an organisation is left untouched."""
        log_df = self._make_log_df(
            spark,
            [
                (
                    "endpoint-aaa",
                    "resource-aaa",
                    "200",
                    "",
                    "conservation-area",
                    "organisation:1",
                )
            ],
        )
        source_df = self._make_source_df(spark, [("endpoint-aaa", "organisation:2")])

        result = _backfill_organisation_from_source(log_df, source_df)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["organisation"] == "organisation:1"

    def test_endpoint_not_in_source_keeps_empty_organisation(self, spark):
        """A failing endpoint with no source entry stays with organisation=''."""
        log_df = self._make_log_df(spark, [("endpoint-unknown", "", "404", "", "", "")])
        source_df = self._make_source_df(spark, [("endpoint-other", "organisation:1")])

        result = _backfill_organisation_from_source(log_df, source_df)

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["organisation"] == ""


def test_active_resources_from_log_uses_latest_successful_per_endpoint(spark):
    log_df = spark.createDataFrame(
        [
            # endpoint-a: an older (now superseded) resource + the current one
            ("endpoint-a", "resource-old", "200", "2026-01-01"),
            ("endpoint-a", "resource-current", "200", "2026-02-01"),
            # a resource that only ever failed here → must not be active
            ("endpoint-b", "resource-fail", "404", "2026-02-01"),
        ],
        ["endpoint", "resource", "status", "entry_date"],
    )
    endpoint_attrs_df = spark.createDataFrame(
        [
            ("endpoint-a", "dataset-a", "org:1"),
            ("endpoint-b", "dataset-b", "org:2"),
        ],
        ["endpoint", "dataset", "organisation"],
    )

    active = _active_resources_from_log(log_df, endpoint_attrs_df)
    rows = {
        (r["endpoint"], r["resource"], r["dataset"], r["organisation"])
        for r in active.collect()
    }

    # Only endpoint-a's latest 200 survives; the superseded resource and the
    # never-successful endpoint-b are both excluded.
    assert rows == {("endpoint-a", "resource-current", "dataset-a", "org:1")}


# -- ProvisionQualityPipeline helpers -----------------------------------------

PQ_DATASET = "conservation-area"
PQ_ADU = "local-authority:ADU"
PQ_LEW = "local-authority:LEW"
PQ_MHCLG = "government-organisation:MHCLG"
PQ_NEW = "local-authority:NEW"


def _provision_quality_inputs(spark):
    """Conservation-area style scenario covering the provider/owner mismatches:
    - Adur  : active endpoint + owns authoritative entities            -> authoritative
    - Lewes : no endpoint, owns entities seeded on its behalf ('some') -> some
    - MHCLG : national seeder, has endpoint, owns nothing, seeded Lewes -> some
    - New LA: active endpoint, owns nothing, seeds nothing (kept + flagged, null)
    """
    providers_df = spark.createDataFrame(
        [(PQ_DATASET, PQ_ADU), (PQ_DATASET, PQ_MHCLG), (PQ_DATASET, PQ_NEW)],
        ["dataset", "organisation"],
    )
    org_df = spark.createDataFrame(
        [
            (PQ_ADU, "100", "Adur DC", True),
            (PQ_LEW, "200", "Lewes DC", True),
            (PQ_MHCLG, "300", "MHCLG", True),
            (PQ_NEW, "400", "New LA", True),
        ],
        ["organisation", "organisation_entity", "organisation_name", "org_active"],
    )
    entity_org_df = spark.createDataFrame(
        [(PQ_DATASET, PQ_ADU), (PQ_DATASET, PQ_LEW)],  # designated provisions
        ["dataset", "organisation"],
    )
    lookup_df = spark.createDataFrame(
        [("3", PQ_MHCLG), ("4", PQ_MHCLG)],  # MHCLG seeded entities 3 & 4
        ["entity", "organisation"],
    )
    entity_quality_df = spark.createDataFrame(
        [
            (PQ_DATASET, "100", "authoritative", "1"),  # owned by Adur
            (PQ_DATASET, "100", "authoritative", "2"),  # owned by Adur
            (PQ_DATASET, "200", "some", "3"),  # owned by Lewes (seeded)
            (PQ_DATASET, "200", "some", "4"),  # owned by Lewes (seeded)
        ],
        ["dataset", "organisation_entity", "quality", "entity"],
    )
    return providers_df, org_df, entity_org_df, lookup_df, entity_quality_df


class TestProvisionQuality:

    def test_flags_and_quality(self, spark):
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        rows = {r["organisation"]: r.asDict() for r in pq.collect()}

        assert set(rows) == {PQ_ADU, PQ_LEW, PQ_MHCLG, PQ_NEW}

        adu = rows[PQ_ADU]
        assert adu["has_active_endpoint"] is True
        assert adu["owns_entities"] is True
        assert adu["is_designated_provider"] is True
        assert adu["quality"] == "authoritative"
        assert adu["entity_count"] == 2

        lew = rows[PQ_LEW]
        assert lew["has_active_endpoint"] is False  # owns but never submitted
        assert lew["owns_entities"] is True
        assert lew["is_designated_provider"] is True
        assert lew["quality"] == "some"
        assert lew["entity_count"] == 2

        mhclg = rows[PQ_MHCLG]
        assert mhclg["has_active_endpoint"] is True
        assert mhclg["owns_entities"] is False  # provider that owns nothing
        assert mhclg["is_designated_provider"] is False
        assert mhclg["quality"] == "some"  # via seeder detection
        assert mhclg["entity_count"] == 2  # seeded count

        new = rows[PQ_NEW]
        assert new["has_active_endpoint"] is True
        assert new["owns_entities"] is False
        assert new["quality"] is None  # endpoint but no data — kept + flagged
        assert new["entity_count"] == 0

    def test_dataset_quality_rollup(self, spark):
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        ds = {r["dataset"]: r.asDict() for r in _build_dataset_quality(pq).collect()}

        row = ds[PQ_DATASET]
        assert row["authoritative_organisations"] == 1  # Adur
        assert row["some_organisations"] == 2  # Lewes, MHCLG
        assert row["total_organisations"] == 3  # New LA excluded (quality null)
        assert row["total_entities"] == 4  # owned counts only, no double count

    def test_organisation_quality_rollup(self, spark):
        pq = _build_provision_quality(*_provision_quality_inputs(spark))
        orgs = {
            r["organisation"]: r.asDict()
            for r in _build_organisation_quality(pq).collect()
        }

        assert orgs[PQ_ADU]["authoritative_datasets"] == 1
        assert orgs[PQ_ADU]["total_entities_owned"] == 2

        assert orgs[PQ_MHCLG]["some_datasets"] == 1
        assert orgs[PQ_MHCLG]["authoritative_datasets"] == 0
        assert orgs[PQ_MHCLG]["total_entities_owned"] == 0  # seeder owns nothing

        assert PQ_NEW not in orgs  # quality null -> excluded from rollup
