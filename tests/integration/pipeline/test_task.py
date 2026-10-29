"""
Integration tests for TaskPipeline and its module-level helper functions.

Uses a real Spark session and local filesystem for reads/writes.
"""

import csv
import json
import os

import pytest

from jobs.pipeline.base import PipelineConfig
from jobs.pipeline.task import (
    TaskPipeline,
    _active_resources_from_log,
    _backfill_dataset_from_source,
    _backfill_organisation_from_source,
    _latest_log_entry_per_endpoint,
    _load_issue_type_df,
)
from jobs.transform.task_transformer import (
    CONVERSION_FAILED_EXCEPTION,
    EMPTY_RESPONSE_EXCEPTION,
)

from ._test_helpers import write_csv


def _write_dataset_specification(base, datasets):
    """specification/dataset.csv, read by the authority leg to scope tasks to
    datasets this environment actually builds. Written as production with no
    end-date so every dataset listed counts as live in any env."""
    write_csv(
        os.path.join(base, "specification", "dataset.csv"),
        ["dataset", "environment", "end-date"],
        [
            {"dataset": dataset, "environment": "production", "end-date": ""}
            for dataset in datasets
        ],
    )


def _write_empty_authority_fixtures(base):
    """Organisation + designation fixtures TaskPipeline.execute() now reads
    unconditionally (for the authority leg). No designated providers here, so
    that leg produces nothing — existing assertions in tests using this
    fixture are unaffected."""
    _write_dataset_specification(base, ["dataset-a"])
    write_csv(
        os.path.join(base, "organisation-collection", "dataset", "organisation.csv"),
        ["organisation", "entity", "name", "end-date"],
        [
            {
                "organisation": "organisation:1",
                "entity": "600001",
                "name": "Test Org",
                "end-date": "",
            }
        ],
    )
    write_csv(
        os.path.join(
            base, "config", "pipeline", "dataset-a", "entity-organisation.csv"
        ),
        ["dataset", "organisation"],
        [],
    )
    write_csv(
        os.path.join(base, "entity", "dataset-a.csv"),
        ["entity", "organisation-entity", "quality"],
        [],
    )


class TestTaskPipeline:

    def test_no_duplicate_references_in_output(self, spark, tmp_path, mocker):
        """TaskPipeline produces no duplicate references even when the same
        endpoint fails on multiple collection days — realistic log.csv scenario
        where extra columns (entry-date, bytes, elapsed) would previously prevent
        .distinct() from deduplicating repeated failures.

        _latest_log_entry_per_endpoint now collapses the repeated-failure case
        upstream, so the endpoint's LAST entry must be the failure for this to
        assert anything about log tasks at all."""
        base = str(tmp_path)
        parquet_base = os.path.join(base, "parquet-output/")

        # Same endpoint failing on two different dates — the key scenario.
        # entry-date and elapsed differ, which previously caused .distinct()
        # to keep both rows and produce duplicate reference hashes. The 200
        # comes first so the endpoint's current state is still "failing".
        write_csv(
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
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                    "bytes": "200",
                    "elapsed": "1.0",
                },
                {
                    "endpoint": "http://endpoint-a",
                    "resource": "resource-aaa",
                    "status": "404",
                    "exception": "",
                    "entry-date": "2026-01-02",
                    "bytes": "200",
                    "elapsed": "1.2",
                },
                {
                    "endpoint": "http://endpoint-a",
                    "resource": "resource-aaa",
                    "status": "404",
                    "exception": "",
                    "entry-date": "2026-01-03",
                    "bytes": "200",
                    "elapsed": "1.1",
                },
            ],
        )

        write_csv(
            os.path.join(
                base, "test-collection", "issue", "dataset-a", "resource-aaa.csv"
            ),
            [
                "dataset",
                "resource",
                "line-number",
                "entry-number",
                "field",
                "entity",
                "issue-type",
                "value",
                "message",
            ],
            [
                {
                    "dataset": "dataset-a",
                    "resource": "resource-aaa",
                    "line-number": "1",
                    "entry-number": "1",
                    "field": "geometry",
                    "entity": "4400001",
                    "issue-type": "invalid-geometry",
                    "value": "POLYGON((0 0))",
                    "message": "invalid",
                }
            ],
        )

        mocker.patch(
            "jobs.pipeline.task._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("invalid-geometry", "error", "external", "validity")],
                ["issue_type", "severity", "responsibility", "quality_dimension"],
            ),
        )

        _write_empty_authority_fixtures(base)

        config = PipelineConfig(
            spark=spark,
            dataset="",
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
        )

        csv_base = os.path.join(base, "csv-output/")
        TaskPipeline(config).run(
            entity_data_path=os.path.join(base, "entity"), output_path=csv_base
        )

        tasks_df = spark.read.format("delta").load(os.path.join(parquet_base, "task"))
        references = [row["reference"] for row in tasks_df.collect()]
        assert len(references) == len(
            set(references)
        ), f"{len(references) - len(set(references))} duplicate references found"

        # The CSV is a third output alongside Delta and Postgres — assert it is
        # written and carries the same rows, not just that the job didn't crash.
        with open(os.path.join(csv_base, "task.csv")) as f:
            csv_rows = list(csv.DictReader(f))
        assert len(csv_rows) == len(references)
        assert {r["reference"] for r in csv_rows} == set(references)

    def test_mixed_issue_csv_layouts_all_produce_tasks(self, spark, tmp_path, mocker):
        """Issue CSVs exist in 7-, 8- and 9-column layouts. All three must
        produce tasks — a single positional multi-file read applies one file's
        header to the others and silently drops the mismatched ones."""
        base = str(tmp_path)
        parquet_base = os.path.join(base, "parquet-output/")

        write_csv(
            os.path.join(base, "test-collection", "collection", "log.csv"),
            ["endpoint", "resource", "status", "exception", "entry-date"],
            [
                {
                    "endpoint": f"http://endpoint-{n}",
                    "resource": f"resource-{n}",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                }
                for n in ("7", "8", "9")
            ],
        )

        issue_dir = os.path.join(base, "test-collection", "issue", "dataset-a")
        common = {
            "dataset": "dataset-a",
            "line-number": "1",
            "entry-number": "1",
            "field": "geometry",
            "issue-type": "OSGB flipped",
            "value": "POLYGON((0 0))",
        }

        write_csv(
            os.path.join(issue_dir, "resource-7.csv"),
            [
                "dataset",
                "resource",
                "line-number",
                "entry-number",
                "field",
                "issue-type",
                "value",
            ],
            [{**common, "resource": "resource-7"}],
        )
        write_csv(
            os.path.join(issue_dir, "resource-8.csv"),
            [
                "dataset",
                "resource",
                "line-number",
                "entry-number",
                "field",
                "issue-type",
                "value",
                "message",
            ],
            [{**common, "resource": "resource-8", "message": "flipped"}],
        )
        write_csv(
            os.path.join(issue_dir, "resource-9.csv"),
            [
                "dataset",
                "resource",
                "line-number",
                "entry-number",
                "field",
                "entity",
                "issue-type",
                "value",
                "message",
            ],
            [
                {
                    **common,
                    "resource": "resource-9",
                    "entity": "4400001",
                    "message": "flipped",
                }
            ],
        )

        mocker.patch(
            "jobs.pipeline.task._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("OSGB flipped", "warning", "external", "validity")],
                ["issue_type", "severity", "responsibility", "quality_dimension"],
            ),
        )

        _write_empty_authority_fixtures(base)

        config = PipelineConfig(
            spark=spark,
            dataset="",
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
        )

        TaskPipeline(config).run(
            entity_data_path=os.path.join(base, "entity"),
            output_path=os.path.join(base, "csv-output/"),
        )

        tasks_df = spark.read.format("delta").load(os.path.join(parquet_base, "task"))
        issue_tasks = tasks_df.filter(tasks_df.task_source == "issue")
        assert {row["resource"] for row in issue_tasks.collect()} == {
            "resource-7",
            "resource-8",
            "resource-9",
        }

    def test_authority_task_reaches_the_output_table(self, spark, tmp_path):
        """A designated provider owning nothing must produce a provision task
        in the final written output — guards against the authority leg being
        computed but dropped before the union (exactly the bug this leg had
        mid-review: a duplicate `frames = [...]` assignment silently
        discarded it)."""
        base = str(tmp_path)
        parquet_base = os.path.join(base, "parquet-output/")

        # A collection has to exist or log_files ends up empty and the CSV
        # read fails to infer a schema — one successful row, no log task.
        write_csv(
            os.path.join(base, "dataset-a-collection", "collection", "log.csv"),
            ["endpoint", "resource", "status", "exception", "entry-date"],
            [
                {
                    "endpoint": "http://endpoint-a",
                    "resource": "resource-a",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                }
            ],
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity", "name", "end-date"],
            [
                {
                    "organisation": "organisation:1",
                    "entity": "600001",
                    "name": "Test Org",
                    "end-date": "",
                }
            ],
        )
        write_csv(
            os.path.join(
                base, "config", "pipeline", "dataset-a", "entity-organisation.csv"
            ),
            ["dataset", "organisation"],
            [{"dataset": "dataset-a", "organisation": "organisation:1"}],
        )
        write_csv(
            os.path.join(base, "entity", "dataset-a.csv"),
            ["entity", "organisation-entity", "quality"],
            [],  # organisation:1 owns nothing -> "none"
        )
        _write_dataset_specification(base, ["dataset-a"])

        config = PipelineConfig(
            spark=spark,
            dataset="",
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
        )
        TaskPipeline(config).run(
            entity_data_path=os.path.join(base, "entity"),
            output_path=os.path.join(base, "csv-output/"),
        )

        tasks_df = spark.read.format("delta").load(os.path.join(parquet_base, "task"))
        authority_rows = [
            r for r in tasks_df.collect() if r["task_source"] == "provision"
        ]
        assert len(authority_rows) == 1
        assert authority_rows[0]["organisation"] == "organisation:1"
        assert authority_rows[0]["quality_dimension"] == "authoritativeness"

    def test_retired_dataset_produces_no_authority_task(self, spark, tmp_path):
        """End-to-end guard for the live-dataset filter: identical to the test
        above except the dataset is end-dated in the specification, so the
        designation must produce no task at all. This is the production bug —
        retired datasets kept their designations and kept making tasks."""
        base = str(tmp_path)
        parquet_base = os.path.join(base, "parquet-output/")

        # A failed endpoint too, so the pipeline still writes an output table
        # once the authority task is correctly filtered out — otherwise there
        # would be no tasks at all and nothing to assert against.
        write_csv(
            os.path.join(base, "dataset-a-collection", "collection", "log.csv"),
            ["endpoint", "resource", "status", "exception", "entry-date"],
            [
                {
                    "endpoint": "http://endpoint-a",
                    "resource": "resource-a",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                },
                {
                    "endpoint": "http://endpoint-b",
                    "resource": "",
                    "status": "404",
                    "exception": "Not Found",
                    "entry-date": "2026-01-01",
                },
            ],
        )
        write_csv(
            os.path.join(
                base, "organisation-collection", "dataset", "organisation.csv"
            ),
            ["organisation", "entity", "name", "end-date"],
            [
                {
                    "organisation": "organisation:1",
                    "entity": "600001",
                    "name": "Test Org",
                    "end-date": "",
                }
            ],
        )
        write_csv(
            os.path.join(
                base, "config", "pipeline", "dataset-a", "entity-organisation.csv"
            ),
            ["dataset", "organisation"],
            [{"dataset": "dataset-a", "organisation": "organisation:1"}],
        )
        write_csv(
            os.path.join(base, "entity", "dataset-a.csv"),
            ["entity", "organisation-entity", "quality"],
            [],
        )
        write_csv(
            os.path.join(base, "specification", "dataset.csv"),
            ["dataset", "environment", "end-date"],
            [
                {
                    "dataset": "dataset-a",
                    "environment": "production",
                    "end-date": "2026-02-03",
                }
            ],
        )

        config = PipelineConfig(
            spark=spark,
            dataset="",
            env="local",
            collection_data_path=f"{base}/",
            parquet_datasets_path=parquet_base,
        )
        TaskPipeline(config).run(
            entity_data_path=os.path.join(base, "entity"),
            output_path=os.path.join(base, "csv-output/"),
        )

        rows = (
            spark.read.format("delta")
            .load(os.path.join(parquet_base, "task"))
            .collect()
        )
        assert [r for r in rows if r["task_source"] == "provision"] == []
        # The filter must remove only the authority task, not suppress the run
        assert [r for r in rows if r["task_source"] == "log"] != []


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


class TestLatestLogEntryPerEndpoint:
    """A log task describes how an endpoint is behaving NOW, so only its most
    recent entry may produce one. Without this every failure an endpoint ever
    had becomes a task for ever, long after the publisher has fixed it."""

    def test_keeps_only_the_most_recent_entry_per_endpoint(self, spark):
        log_df = spark.createDataFrame(
            [
                # recovered: failed once, has collected cleanly since
                ("endpoint-a", "", "404", "2026-01-01"),
                ("endpoint-a", "resource-a", "200", "2026-02-01"),
                # broken now: worked once, currently failing
                ("endpoint-b", "resource-b", "200", "2026-01-01"),
                ("endpoint-b", "", "500", "2026-02-01"),
            ],
            ["endpoint", "resource", "status", "entry_date"],
        )

        rows = {
            (r["endpoint"], r["status"])
            for r in _latest_log_entry_per_endpoint(log_df).collect()
        }

        assert rows == {("endpoint-a", "200"), ("endpoint-b", "500")}

    def test_every_endpoint_keeps_exactly_one_row(self, spark):
        log_df = spark.createDataFrame(
            [
                ("endpoint-a", "404", "2026-01-01"),
                ("endpoint-a", "403", "2026-01-02"),
                ("endpoint-a", "500", "2026-01-03"),
            ],
            ["endpoint", "status", "entry_date"],
        )

        result = _latest_log_entry_per_endpoint(log_df).collect()

        # Three distinct failure statuses on one endpoint used to mean three
        # tasks, permanently. Only the current one survives.
        assert len(result) == 1
        assert result[0]["status"] == "500"

    def test_success_wins_a_tie_on_the_same_date(self, spark):
        log_df = spark.createDataFrame(
            [
                ("endpoint-a", "404", "2026-02-01"),
                ("endpoint-a", "200", "2026-02-01"),
            ],
            ["endpoint", "status", "entry_date"],
        )

        result = _latest_log_entry_per_endpoint(log_df).collect()

        # Collected successfully at some point on its last day, so it works.
        assert len(result) == 1
        assert result[0]["status"] == "200"

    def test_a_row_with_no_status_loses_a_tie_to_a_real_one(self, spark):
        log_df = spark.createDataFrame(
            [
                ("endpoint-a", None, "2026-02-01"),
                ("endpoint-a", "200", "2026-02-01"),
            ],
            ["endpoint", "status", "entry_date"],
        )

        result = _latest_log_entry_per_endpoint(log_df).collect()

        assert len(result) == 1
        assert result[0]["status"] == "200"


def _write_source(base, collection, rows):
    """source.csv for a collection. Only endpoint, pipelines, organisation and
    end-date are read by TaskPipeline; an end-date means the endpoint is retired."""
    write_csv(
        os.path.join(base, collection, "collection", "source.csv"),
        ["endpoint", "pipelines", "organisation", "end-date"],
        rows,
    )


def _write_log(base, collection, rows):
    write_csv(
        os.path.join(base, collection, "collection", "log.csv"),
        ["endpoint", "resource", "status", "exception", "entry-date"],
        rows,
    )


def _write_issue(base, collection, dataset, resource):
    write_csv(
        os.path.join(base, collection, "issue", dataset, f"{resource}.csv"),
        [
            "dataset",
            "resource",
            "line-number",
            "entry-number",
            "field",
            "issue-type",
            "value",
            "message",
        ],
        [
            {
                "dataset": dataset,
                "resource": resource,
                "line-number": "1",
                "entry-number": "1",
                "field": "geometry",
                "issue-type": "invalid-geometry",
                "value": "POLYGON((0 0))",
                "message": "invalid",
            }
        ],
    )


def _run_task_pipeline(spark, base, env="local"):
    parquet_base = os.path.join(base, "parquet-output/")
    config = PipelineConfig(
        spark=spark,
        dataset="",
        env=env,
        collection_data_path=f"{base}/",
        parquet_datasets_path=parquet_base,
    )
    TaskPipeline(config).run(
        entity_data_path=os.path.join(base, "entity"),
        output_path=os.path.join(base, "csv-output/"),
    )
    return spark.read.format("delta").load(os.path.join(parquet_base, "task"))


class TestRetiredAndRecoveredEndpoints:
    """The two ways a task stops being real: the publisher fixed the endpoint,
    or retired it. Both must clear the task. These are the first tests to drive
    execute() with a source.csv, which is what makes an endpoint retired."""

    def test_retired_endpoint_produces_no_tasks(self, spark, tmp_path, mocker):
        """An end-dated source row means the endpoint is gone. Its frozen log and
        issue history must not keep generating tasks every night."""
        base = str(tmp_path)
        collection = "dataset-a-collection"

        _write_log(
            base,
            collection,
            [
                # endpoint-a: live, and currently failing
                {
                    "endpoint": "endpoint-a",
                    "resource": "resource-aaa",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                },
                {
                    "endpoint": "endpoint-a",
                    "resource": "",
                    "status": "404",
                    "exception": "",
                    "entry-date": "2026-01-02",
                },
                # endpoint-b: retired, and failing when it was switched off
                {
                    "endpoint": "endpoint-b",
                    "resource": "resource-bbb",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                },
                {
                    "endpoint": "endpoint-b",
                    "resource": "",
                    "status": "500",
                    "exception": "",
                    "entry-date": "2026-01-02",
                },
            ],
        )
        _write_source(
            base,
            collection,
            [
                {
                    "endpoint": "endpoint-a",
                    "pipelines": "dataset-a",
                    "organisation": "organisation:1",
                    "end-date": "",
                },
                {
                    "endpoint": "endpoint-b",
                    "pipelines": "dataset-a",
                    "organisation": "organisation:1",
                    "end-date": "2026-01-03",
                },
            ],
        )
        _write_issue(base, collection, "dataset-a", "resource-aaa")
        _write_issue(base, collection, "dataset-a", "resource-bbb")

        mocker.patch(
            "jobs.pipeline.task._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("invalid-geometry", "error", "external", "validity")],
                ["issue_type", "severity", "responsibility", "quality_dimension"],
            ),
        )
        _write_empty_authority_fixtures(base)

        tasks = _run_task_pipeline(spark, base).collect()

        endpoints = {r["endpoint"] for r in tasks}
        assert "endpoint-b" not in endpoints, (
            "retired endpoint still produced tasks — its log failures and the "
            "issues on its last resource are both frozen and unactionable"
        )
        # The live endpoint is untouched: still a log task for the 404 and an
        # issue task for its current resource.
        assert endpoints == {"endpoint-a"}
        assert {r["task_source"] for r in tasks} == {"log", "issue"}

    def test_recovered_endpoint_produces_no_log_task(self, spark, tmp_path, mocker):
        """An endpoint that failed and has since collected cleanly is fixed. Its
        old failure must not keep producing a task — this is what makes the task
        self-healing when a publisher repairs their endpoint."""
        base = str(tmp_path)
        collection = "dataset-a-collection"

        _write_log(
            base,
            collection,
            [
                # endpoint-a: failed, then recovered
                {
                    "endpoint": "endpoint-a",
                    "resource": "",
                    "status": "404",
                    "exception": "",
                    "entry-date": "2026-01-01",
                },
                {
                    "endpoint": "endpoint-a",
                    "resource": "resource-aaa",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-02",
                },
                # endpoint-b: worked, then broke and is still broken
                {
                    "endpoint": "endpoint-b",
                    "resource": "resource-bbb",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                },
                {
                    "endpoint": "endpoint-b",
                    "resource": "",
                    "status": "503",
                    "exception": "",
                    "entry-date": "2026-01-02",
                },
            ],
        )
        _write_source(
            base,
            collection,
            [
                {
                    "endpoint": endpoint,
                    "pipelines": "dataset-a",
                    "organisation": "organisation:1",
                    "end-date": "",
                }
                for endpoint in ("endpoint-a", "endpoint-b")
            ],
        )
        _write_issue(base, collection, "dataset-a", "resource-aaa")

        mocker.patch(
            "jobs.pipeline.task._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("invalid-geometry", "error", "external", "validity")],
                ["issue_type", "severity", "responsibility", "quality_dimension"],
            ),
        )
        _write_empty_authority_fixtures(base)

        tasks = _run_task_pipeline(spark, base).collect()

        log_endpoints = {r["endpoint"] for r in tasks if r["task_source"] == "log"}
        assert log_endpoints == {"endpoint-b"}, (
            "a recovered endpoint must not carry a log task; only the one that "
            "is still failing should"
        )
        # Recovery clears the log task but not the issues on the data it did
        # publish — those are still the current resource.
        assert any(
            r["task_source"] == "issue" and r["endpoint"] == "endpoint-a" for r in tasks
        )


def _write_converted_resource(base, collection, dataset, resource, status, exception):
    write_csv(
        os.path.join(
            base, collection, "var", "converted-resource", dataset, f"{resource}.csv"
        ),
        ["dataset", "resource", "elapsed", "status", "exception"],
        [
            {
                "dataset": dataset,
                "resource": resource,
                "elapsed": "0.1",
                "status": status,
                "exception": exception,
            }
        ],
    )


class TestFailedConversion:
    """An endpoint returning a 200 whose resource could not be converted, e.g.
    a WFS ExceptionReport, is not collecting data and gets a log task."""

    def test_200_with_failed_conversion_produces_a_log_task(
        self, spark, tmp_path, mocker
    ):
        base = str(tmp_path)
        collection = "dataset-a-collection"

        _write_log(
            base,
            collection,
            [
                # endpoint-a: converted to nothing
                {
                    "endpoint": "endpoint-a",
                    "resource": "resource-aaa",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                },
                # endpoint-b: converted fine
                {
                    "endpoint": "endpoint-b",
                    "resource": "resource-bbb",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                },
            ],
        )
        _write_source(
            base,
            collection,
            [
                {
                    "endpoint": endpoint,
                    "pipelines": "dataset-a",
                    "organisation": "organisation:1",
                    "end-date": "",
                }
                for endpoint in ("endpoint-a", "endpoint-b")
            ],
        )
        # ogr2ogr's exception spans many lines; it must still read as one row
        _write_converted_resource(
            base,
            collection,
            "dataset-a",
            "resource-aaa",
            "failed",
            "ogr2ogr failed (1). stderr='FAILURE:\n  -> `GML'\n  -> `WFS'\n'",
        )
        _write_converted_resource(
            base, collection, "dataset-a", "resource-bbb", "success", ""
        )

        mocker.patch(
            "jobs.pipeline.task._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("invalid-geometry", "error", "external", "validity")],
                ["issue_type", "severity", "responsibility", "quality_dimension"],
            ),
        )
        _write_empty_authority_fixtures(base)

        tasks = _run_task_pipeline(spark, base).collect()

        log_tasks = [r for r in tasks if r["task_source"] == "log"]
        assert [(r["endpoint"], r["resource"]) for r in log_tasks] == [
            ("endpoint-a", "resource-aaa")
        ]
        assert log_tasks[0]["dataset"] == "dataset-a"
        assert log_tasks[0]["organisation"] == "organisation:1"
        assert json.loads(log_tasks[0]["details"]) == {
            "status": 200,
            "exception": CONVERSION_FAILED_EXCEPTION,
        }


def _write_dataset_resource(
    base, collection, dataset, resource, entity_count, line_count
):
    write_csv(
        os.path.join(
            base, collection, "var", "dataset-resource", dataset, f"{resource}.csv"
        ),
        ["dataset", "resource", "entity-count", "entry-count", "line-count"],
        [
            {
                "dataset": dataset,
                "resource": resource,
                "entity-count": entity_count,
                "entry-count": entity_count,
                "line-count": line_count,
            }
        ],
    )


def _write_raw_resource(base, collection, resource, content):
    path = os.path.join(base, collection, "collection", "resource", resource)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)


def _write_field_specification(base, dataset, fields):
    """The three files current_dataset_fields resolves a dataset's fields from."""
    spec = os.path.join(base, "specification")
    write_csv(
        os.path.join(spec, "dataset-schema.csv"),
        ["dataset", "schema"],
        [{"dataset": dataset, "schema": dataset}],
    )
    write_csv(
        os.path.join(spec, "schema-field.csv"),
        ["schema", "field"],
        [{"schema": dataset, "field": field} for field in fields],
    )
    write_csv(
        os.path.join(spec, "field.csv"),
        ["field", "end-date"],
        [{"field": field, "end-date": ""} for field in fields],
    )


class TestEmptyResponse:
    """An endpoint returning a 200 that converts to no rows. A valid file for
    the dataset with no rows in it is a notice; anything else, such as an
    error page sent with a 200, is an error."""

    def _run(self, spark, tmp_path, mocker, raw, entity_count="0", line_count="1"):
        base = str(tmp_path)
        collection = "dataset-a-collection"
        _write_log(
            base,
            collection,
            [
                {
                    "endpoint": "endpoint-a",
                    "resource": "resource-aaa",
                    "status": "200",
                    "exception": "",
                    "entry-date": "2026-01-01",
                }
            ],
        )
        _write_source(
            base,
            collection,
            [
                {
                    "endpoint": "endpoint-a",
                    "pipelines": "dataset-a",
                    "organisation": "organisation:1",
                    "end-date": "",
                }
            ],
        )
        _write_converted_resource(
            base, collection, "dataset-a", "resource-aaa", "success", ""
        )
        _write_dataset_resource(
            base, collection, "dataset-a", "resource-aaa", entity_count, line_count
        )
        _write_raw_resource(base, collection, "resource-aaa", raw)
        # an issue task too, so the task table is written when no log task is
        _write_issue(base, collection, "dataset-a", "resource-aaa")
        mocker.patch(
            "jobs.pipeline.task._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("invalid-geometry", "error", "external", "validity")],
                ["issue_type", "severity", "responsibility", "quality_dimension"],
            ),
        )
        _write_empty_authority_fixtures(base)
        _write_field_specification(base, "dataset-a", ["reference", "name"])
        write_csv(
            os.path.join(base, "config", "pipeline", "dataset-a", "column.csv"),
            ["dataset", "endpoint", "resource", "column", "field"],
            [
                {"column": "SiteRef", "field": "reference"},
                {"column": "Notes", "field": "IGNORE"},
            ],
        )

        tasks = _run_task_pipeline(spark, base).collect()
        return [r for r in tasks if r["task_source"] == "log"]

    def test_header_only_csv_is_a_notice(self, spark, tmp_path, mocker):
        log_tasks = self._run(spark, tmp_path, mocker, "reference,name\n")
        assert [r["severity"] for r in log_tasks] == ["notice"]
        assert json.loads(log_tasks[0]["details"]) == {
            "status": 200,
            "exception": EMPTY_RESPONSE_EXCEPTION,
        }

    def test_header_mapped_by_column_config_is_a_notice(self, spark, tmp_path, mocker):
        """Publishers' headers rarely match field names; column.csv maps them,
        and headers are normalised the way the pipeline normalises them."""
        log_tasks = self._run(spark, tmp_path, mocker, "Site Ref,Description\n")
        assert [r["severity"] for r in log_tasks] == ["notice"]

    def test_empty_feature_collection_is_a_notice(self, spark, tmp_path, mocker):
        raw = '{"type": "FeatureCollection", "features": []}'
        log_tasks = self._run(spark, tmp_path, mocker, raw)
        assert [r["severity"] for r in log_tasks] == ["notice"]

    @pytest.mark.parametrize(
        "raw",
        [
            "<h1>Mediaprotect configuration error</h1>We are very sorry, but",
            "Required parameter 'SERVICE' missing, incorrect, or unsupported.",
            # a column mapped to IGNORE does not make it a header for the dataset
            "Notes\n",
            "",
        ],
    )
    def test_error_message_is_an_error(self, spark, tmp_path, mocker, raw):
        log_tasks = self._run(spark, tmp_path, mocker, raw)
        assert [r["severity"] for r in log_tasks] == ["error"]
        assert json.loads(log_tasks[0]["details"]) == {
            "status": 200,
            "exception": CONVERSION_FAILED_EXCEPTION,
        }

    def test_resource_with_entities_is_not_a_task(self, spark, tmp_path, mocker):
        log_tasks = self._run(
            spark, tmp_path, mocker, "reference\nA1\n", entity_count="1", line_count="2"
        )
        assert log_tasks == []

    def test_rows_dropped_by_issues_are_not_a_task(self, spark, tmp_path, mocker):
        """Lines but no entities means every row was dropped, which issue tasks
        already report."""
        log_tasks = self._run(
            spark, tmp_path, mocker, "x\n1\n2\n", entity_count="0", line_count="3"
        )
        assert log_tasks == []

    def test_log_without_entity_count_is_left_alone(self, spark, tmp_path, mocker):
        """Older dataset-resource logs predate entity-count and cannot say."""
        log_tasks = self._run(
            spark, tmp_path, mocker, "Notes\n", entity_count="", line_count="1"
        )
        assert log_tasks == []


class TestDatasetsSwitchedOffForEnvironment:
    """A dataset switched off for an environment in the specification is no
    longer built there, but its frozen log and issue files stay in the bucket.
    Those must stop producing tasks, as planning-application's did in production
    for a year after it was moved to staging only."""

    def test_switched_off_dataset_produces_no_tasks(self, spark, tmp_path, mocker):
        base = str(tmp_path)
        collection = "test-collection"

        # Both endpoints worked, then broke: a log task and an issue task each
        _write_log(
            base,
            collection,
            [
                row
                for endpoint, resource in (
                    ("endpoint-a", "resource-aaa"),
                    ("endpoint-b", "resource-bbb"),
                )
                for row in (
                    {
                        "endpoint": endpoint,
                        "resource": resource,
                        "status": "200",
                        "exception": "",
                        "entry-date": "2026-01-01",
                    },
                    {
                        "endpoint": endpoint,
                        "resource": "",
                        "status": "404",
                        "exception": "",
                        "entry-date": "2026-01-02",
                    },
                )
            ],
        )
        _write_source(
            base,
            collection,
            [
                {
                    "endpoint": "endpoint-a",
                    "pipelines": "dataset-a",
                    "organisation": "organisation:1",
                    "end-date": "",
                },
                {
                    "endpoint": "endpoint-b",
                    "pipelines": "dataset-b",
                    "organisation": "organisation:1",
                    "end-date": "",
                },
            ],
        )
        _write_issue(base, collection, "dataset-a", "resource-aaa")
        _write_issue(base, collection, "dataset-b", "resource-bbb")

        mocker.patch(
            "jobs.pipeline.task._load_issue_type_df",
            return_value=spark.createDataFrame(
                [("invalid-geometry", "error", "external", "validity")],
                ["issue_type", "severity", "responsibility", "quality_dimension"],
            ),
        )
        _write_empty_authority_fixtures(base)
        # Overwrite the fixture's specification: dataset-b is staging only
        write_csv(
            os.path.join(base, "specification", "dataset.csv"),
            ["dataset", "environment", "end-date"],
            [
                {"dataset": "dataset-a", "environment": "production", "end-date": ""},
                {"dataset": "dataset-b", "environment": "staging", "end-date": ""},
            ],
        )

        tasks = _run_task_pipeline(spark, base, env="production").collect()

        datasets = {r["dataset"] for r in tasks}
        assert datasets == {"dataset-a"}, f"switched-off dataset made tasks: {datasets}"
        # The live dataset is untouched: still both of its tasks
        assert {r["task_source"] for r in tasks} == {"log", "issue"}


@pytest.mark.parametrize("header", ["quality-dimension", "quality_dimension"])
def test_load_issue_type_df_reads_either_dimension_header(spark, mocker, header):
    """Specification PR #219 renames issue-type.csv's quality_dimension to
    quality-dimension. The file is fetched from spec main at runtime, so both
    spellings must resolve or every issue task silently loses its dimension."""
    lines = [
        f"issue-type,severity,responsibility,{header}\n".encode(),
        b"invalid date,error,external,validity\n",
    ]
    urlopen = mocker.patch("urllib.request.urlopen")
    urlopen.return_value.__enter__.return_value.readlines.return_value = lines

    row = _load_issue_type_df(spark).collect()[0]

    assert row["quality_dimension"] == "validity"
