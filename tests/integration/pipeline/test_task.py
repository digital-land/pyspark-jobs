"""
Integration tests for TaskPipeline and its module-level helper functions.

Uses a real Spark session and local filesystem for reads/writes.
"""

import os

from jobs.pipeline.base import PipelineConfig
from jobs.pipeline.task import (
    TaskPipeline,
    _active_resources_from_log,
    _backfill_dataset_from_source,
    _backfill_organisation_from_source,
)

from ._test_helpers import write_csv


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
                [("OSGB flipped", "warning", "external")],
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
        issue_tasks = tasks_df.filter(tasks_df.task_source == "issue")
        assert {row["resource"] for row in issue_tasks.collect()} == {
            "resource-7",
            "resource-8",
            "resource-9",
        }


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
