# Adding Jobs and Pipelines

This guide explains how the layers of the codebase fit together and how to extend it with new pipelines, jobs, and entry points.

## How the layers fit together

```
entry_points/run_main.py      CLI (click) — minimal, decoupled from logic
        |
src/jobs/job.py               Job function — validates inputs, creates Spark session,
        |                     builds PipelineConfig, runs pipelines in order
        |
src/jobs/pipeline.py          Pipeline classes — ETL logic wired together
        |
src/jobs/transform/           Transformer classes — pure DataFrame transformations
src/jobs/utils/               Utilities — S3, Postgres, logging, etc.
```

Each layer has one responsibility. The entry point only parses CLI arguments. The job function only orchestrates. The pipeline only wires extract → transform → load. Transformers only transform DataFrames.

---

## Adding a new pipeline

### 1. Create a transformer

Transformers are stateless classes with a `transform()` method. Add the file to `src/jobs/transform/`:

```python
# src/jobs/transform/my_transformer.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

class MyTransformer:
    @staticmethod
    def transform(df: DataFrame, dataset: str) -> DataFrame:
        df = df.withColumn("dataset", lit(dataset))
        # ... transformation logic
        return df
```

Write unit tests in `tests/unit/` — transformers are pure functions and easy to test in isolation.

### 2. Create a pipeline class

Add the new pipeline to `src/jobs/pipeline.py` by extending `BasePipeline`:

```python
class MyPipeline(BasePipeline):
    def execute(self, collection: str) -> None:
        spark = self.config.spark
        dataset = self.config.dataset
        collection_data_path = self.config.collection_data_path
        parquet_datasets_path = self.config.parquet_datasets_path

        # -- Extract ---------------------------------------------------------
        input_path = str(AnyPath(collection_data_path) / f"{collection}-collection" / "my-data" / dataset / "*.csv")
        df = spark.read.csv(input_path, header=True)

        if df.count() == 0:
            raise ValueError(f"MyPipeline: No data found at {input_path}")

        # -- Transform -------------------------------------------------------
        df = normalise_column_names(df)
        result_df = MyTransformer.transform(df, dataset)

        # -- Load ------------------------------------------------------------
        parquet_base = AnyPath(parquet_datasets_path)
        output_path = str(parquet_base / "my_table")
        if isinstance(parquet_base, S3Path):
            cleanup_dataset_data(output_path, dataset)
        write_parquet(result_df, output_path, partition_by=["dataset"])
```

Key points:
- `BasePipeline.__init__` stores `self.config` and `self.result = {}`
- Always call via `run()`, not `execute()` directly — `run()` handles timing and status tracking
- Raise `ValueError` for empty inputs so the pipeline result reflects `"status": "failed"`
- Use `AnyPath` / `S3Path` for path construction — avoids trailing slash issues and works locally and on S3
- Guard `cleanup_dataset_data` and other S3-only operations with `isinstance(parquet_base, S3Path)`

### 3. Wire it into the job

In `src/jobs/job.py`, instantiate and run the pipeline inside `assemble_and_load_entity`:

```python
from jobs.pipeline import EntityPipeline, IssuePipeline, MyPipeline, PipelineConfig

# ... existing setup code ...

my_pipeline = MyPipeline(config)
my_pipeline.run(collection=collection)

report = [entity_pipeline.result, issue_pipeline.result, my_pipeline.result]
```

`PipelineConfig` is a frozen dataclass — no changes needed unless the new pipeline requires additional shared configuration.

### 4. Add integration tests

Integration tests live in `tests/integration/test_pipeline.py`. Follow the existing pattern: write CSV fixtures to `tmp_path`, run the pipeline, and assert on the parquet output.

```python
def test_my_pipeline_writes_correct_row_count(self, spark, tmp_path, mocker):
    # write fixture CSVs to tmp_path ...
    config = PipelineConfig(
        spark=spark,
        dataset="test-dataset",
        env="local",
        collection_data_path=f"{tmp_path}/",
        parquet_datasets_path=f"{tmp_path}/parquet-output/",
        database_url="postgresql://user:pass@localhost:5432/testdb",
    )
    MyPipeline(config).run(collection="test-dataset")
    result_df = spark.read.parquet(os.path.join(str(tmp_path), "parquet-output", "my_table"))
    assert result_df.count() == expected_count
```

---

## Adding a new job function

If the new work is a completely separate ETL flow (different datasets, different entry point), add a new function to `src/jobs/job.py` following the same pattern as `assemble_and_load_entity`:

```python
def assemble_and_load_my_data(
    collection_data_path: str,
    parquet_datasets_path: str,
    env: str,
    dataset: str,
    collection: str,
    database_url: str | None = None,
) -> None:
    initialize_logging(env)
    # ... validate inputs, create spark session, build config, run pipelines ...
```

Keep the job function thin — validation, Spark setup, and pipeline orchestration only. No transformation logic.

---

## Adding a new entry point

If the new job needs its own CLI command (e.g. for a different EMR job submission), add a new script to `entry_points/`:

```python
# entry_points/run_my_job.py
import sys
import click
from jobs import job
from jobs.utils.logger_config import get_logger, setup_logging

setup_logging(log_level="INFO", environment="production")
logger = get_logger(__name__)


@click.command()
@click.option("--dataset", required=True)
@click.option("--collection", required=True)
@click.option("--env", required=True,
              type=click.Choice(["development", "staging", "production", "local"]))
@click.option("--collection-data-path", default=None)
@click.option("--parquet-datasets-path", default=None)
@click.option("--database-url", default=None)
def run(dataset, collection, env, collection_data_path, parquet_datasets_path, database_url):
    """ETL process for my data."""
    job.assemble_and_load_my_data(
        collection_data_path=collection_data_path or f"s3://{env}-collection-data/",
        parquet_datasets_path=parquet_datasets_path or f"s3://{env}-parquet-datasets/",
        env=env,
        dataset=dataset,
        collection=collection,
        database_url=database_url,
    )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)
```

Entry points are intentionally minimal. All logic lives in the job function and pipelines — the entry point only parses CLI arguments and calls the job function.

Deploy the same way as `run_main.py`: upload to S3 alongside the `.whl` and reference as the `entryPoint` in the EMR Serverless job submission.

---

## Reference

| File | Responsibility |
|------|---------------|
| `entry_points/run_main.py` | CLI argument parsing, calls job function |
| `src/jobs/job.py` | Input validation, Spark session, PipelineConfig, pipeline orchestration |
| `src/jobs/pipeline.py` | ETL wiring (extract → transform → load), `BasePipeline`, `PipelineConfig` |
| `src/jobs/transform/` | DataFrame transformations (stateless, independently testable) |
| `src/jobs/utils/s3_utils.py` | S3 cleanup and path validation |
| `src/jobs/utils/s3_writer_utils.py` | Parquet writing, CSV/JSON/GeoJSON consumer format writing |
| `src/jobs/utils/postgres_writer_utils.py` | JDBC writes to PostgreSQL |
| `src/jobs/config/schemas.json` | Field schemas for column validation |

[← Back to Architecture](./README.md)
