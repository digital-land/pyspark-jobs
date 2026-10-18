"""
Integration tests for OldEntityPipeline.

Uses a real Spark session and local filesystem for reads. The Postgres write is mocked.
"""

import os

import pytest

from jobs.pipeline.base import PipelineConfig
from jobs.pipeline.old_entity import OldEntityPipeline

from ._test_helpers import write_csv

SPEC_COLUMNS = ["dataset", "collection", "entity-minimum", "entity-maximum"]
SPEC_ROWS = [
    {
        "dataset": "listed-building",
        "collection": "listed-building",
        "entity-minimum": "31500000",
        "entity-maximum": "31999999",
    },
    {
        "dataset": "listed-building-outline",
        "collection": "listed-building",
        "entity-minimum": "42100000",
        "entity-maximum": "42199999",
    },
]

OLD_ENTITY_COLUMNS = ["old-entity", "status", "entity", "notes"]
OLD_ENTITY_ROWS = [
    {"old-entity": "31860570", "status": "410", "entity": "", "notes": "gone"},
    {"old-entity": "31860571", "status": "410", "entity": "", "notes": "gone"},
    {"old-entity": "42100001", "status": "410", "entity": "", "notes": "outline"},
]


def _config(spark, base, dataset):
    return PipelineConfig(
        spark=spark,
        dataset=dataset,
        env="local",
        collection_data_path=f"{base}/",
        parquet_datasets_path=os.path.join(base, "parquet-output/"),
        database_url="postgresql://user:pass@localhost:5432/testdb",
    )


def _write_inputs(base, old_entity_rows=OLD_ENTITY_ROWS):
    write_csv(
        os.path.join(base, "specification", "dataset.csv"), SPEC_COLUMNS, SPEC_ROWS
    )
    if old_entity_rows is not None:
        write_csv(
            os.path.join(
                base, "config", "pipeline", "listed-building", "old-entity.csv"
            ),
            OLD_ENTITY_COLUMNS,
            old_entity_rows,
        )


class TestOldEntityPipeline:
    def test_execute_writes_the_dataset_rows(self, spark, tmp_path, mocker):
        """Only the old entities in the dataset's range are written, for that dataset."""
        base = str(tmp_path)
        _write_inputs(base)
        mock_write = mocker.patch(
            "jobs.pipeline.old_entity.write_dataset_old_entity_to_postgres"
        )

        pipeline = OldEntityPipeline(_config(spark, base, "listed-building"))
        pipeline.run(collection="listed-building")

        assert pipeline.result["status"] == "success"
        written_df, dataset, _ = mock_write.call_args.args
        assert dataset == "listed-building"
        assert sorted(r.old_entity for r in written_df.collect()) == [
            31860570,
            31860571,
        ]

    def test_execute_writes_no_rows_for_a_dataset_without_old_entities(
        self, spark, tmp_path, mocker
    ):
        """The dataset's rows are still replaced, so ones removed from the config go."""
        base = str(tmp_path)
        _write_inputs(base, old_entity_rows=OLD_ENTITY_ROWS[:2])
        mock_write = mocker.patch(
            "jobs.pipeline.old_entity.write_dataset_old_entity_to_postgres"
        )

        pipeline = OldEntityPipeline(_config(spark, base, "listed-building-outline"))
        pipeline.run(collection="listed-building")

        written_df, dataset, _ = mock_write.call_args.args
        assert dataset == "listed-building-outline"
        assert written_df.count() == 0

    def test_execute_skips_a_collection_without_old_entity_csv(
        self, spark, tmp_path, mocker
    ):
        base = str(tmp_path)
        _write_inputs(base, old_entity_rows=None)
        mock_write = mocker.patch(
            "jobs.pipeline.old_entity.write_dataset_old_entity_to_postgres"
        )

        pipeline = OldEntityPipeline(_config(spark, base, "listed-building"))
        pipeline.run(collection="listed-building")

        assert pipeline.result["status"] == "success"
        mock_write.assert_not_called()

    def test_execute_raises_when_the_dataset_has_no_entity_range(
        self, spark, tmp_path, mocker
    ):
        base = str(tmp_path)
        _write_inputs(base)
        mocker.patch("jobs.pipeline.old_entity.write_dataset_old_entity_to_postgres")

        pipeline = OldEntityPipeline(_config(spark, base, "not-in-the-specification"))
        with pytest.raises(ValueError):
            pipeline.run(collection="listed-building")
