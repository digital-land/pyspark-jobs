"""
Unit tests for BasePipeline.

Tests the timing, result tracking, and kwargs forwarding behaviour
of the base class without Spark or filesystem dependencies.
"""

import pytest

from jobs.pipeline import BasePipeline, PipelineConfig


@pytest.fixture
def config(mocker):
    return PipelineConfig(
        spark=mocker.MagicMock(),
        dataset="test-dataset",
        env="local",
        collection_data_path="/tmp/",
        parquet_datasets_path="/tmp/",
    )


class _DummyPipeline(BasePipeline):
    def execute(self, **kwargs):
        pass


class TestBasePipeline:
    def test_init_raises_type_error_for_abstract_class(self, config):
        """BasePipeline is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            BasePipeline(config)

    def test_init_sets_config(self, config):
        """Constructor stores config on the instance."""
        pipeline = _DummyPipeline(config)
        assert pipeline.config is config

    def test_init_sets_empty_result(self, config):
        """Constructor initialises result as empty dict."""
        pipeline = _DummyPipeline(config)
        assert pipeline.result == {}

    def test_run_sets_status_success_on_normal_execution(self, config):
        """run() sets status to 'success' when execute() completes."""
        pipeline = _DummyPipeline(config)
        pipeline.run()

        assert pipeline.result["status"] == "success"

    def test_run_sets_status_failed_on_exception(self, config):
        """run() sets status to 'failed' when execute() raises."""

        class FailingPipeline(BasePipeline):
            def execute(self, **kwargs):
                raise ValueError("something went wrong")

        pipeline = FailingPipeline(config)
        with pytest.raises(ValueError, match="something went wrong"):
            pipeline.run()

        assert pipeline.result["status"] == "failed"

    def test_run_records_pipeline_name(self, config):
        """run() records the child class name in result."""
        pipeline = _DummyPipeline(config)
        pipeline.run()

        assert pipeline.result["pipeline"] == "_DummyPipeline"

    def test_run_records_dataset(self, config):
        """run() records the dataset from config in result."""
        pipeline = _DummyPipeline(config)
        pipeline.run()

        assert pipeline.result["dataset"] == "test-dataset"

    def test_run_records_start_and_end_time(self, config):
        """run() records start_time and end_time as ISO strings."""
        pipeline = _DummyPipeline(config)
        pipeline.run()

        assert "start_time" in pipeline.result
        assert "end_time" in pipeline.result

    def test_run_records_duration_seconds(self, config):
        """run() records a non-negative duration."""
        pipeline = _DummyPipeline(config)
        pipeline.run()

        assert pipeline.result["duration_seconds"] >= 0

    def test_run_records_timing_even_on_failure(self, config):
        """run() records timing fields even when execute() raises."""

        class FailingPipeline(BasePipeline):
            def execute(self, **kwargs):
                raise RuntimeError("boom")

        pipeline = FailingPipeline(config)
        with pytest.raises(RuntimeError):
            pipeline.run()

        assert "start_time" in pipeline.result
        assert "end_time" in pipeline.result
        assert pipeline.result["duration_seconds"] >= 0

    def test_run_forwards_kwargs_to_execute(self, config):
        """run() forwards keyword arguments to execute()."""

        class CapturePipeline(BasePipeline):
            def execute(self, **kwargs):
                self.captured_kwargs = kwargs

        pipeline = CapturePipeline(config)
        pipeline.run(collection="trees", use_jdbc=True)

        assert pipeline.captured_kwargs == {
            "collection": "trees",
            "use_jdbc": True,
        }

    def test_run_preserves_custom_result_keys_from_execute(self, config):
        """run() preserves custom metrics added to self.result by execute()."""

        class MetricsPipeline(BasePipeline):
            def execute(self, **kwargs):
                self.result["row_count"] = 42

        pipeline = MetricsPipeline(config)
        pipeline.run()

        assert pipeline.result["row_count"] == 42
        assert pipeline.result["status"] == "success"
