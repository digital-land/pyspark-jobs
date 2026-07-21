"""
Acceptance tests for the run_main entry point.

These tests use click's CliRunner to invoke the entry point command
and verify the CLI interface works correctly.
"""

import csv
import os

from pyspark.sql.types import StringType, StructField, StructType


def test_missing_required_args_returns_nonzero(cli_runner, run_main_cmd):
    """Running with no arguments should fail with a non-zero exit code."""
    result = cli_runner.invoke(run_main_cmd, [])
    assert result.exit_code != 0
    assert "Missing" in result.output or "required" in result.output.lower()


def test_help_flag_returns_zero(cli_runner, run_main_cmd):
    """Running with --help should succeed and display usage info."""
    result = cli_runner.invoke(run_main_cmd, ["--help"])
    assert result.exit_code == 0
    assert "--dataset" in result.output
    assert "--collection" in result.output
    assert "--env" in result.output
    assert "--collection-data-path" in result.output
    assert "--database-url" in result.output


def test_invalid_env_rejected(cli_runner, run_main_cmd):
    """An invalid env value should be rejected by click's Choice validation."""
    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--dataset",
            "test-dataset",
            "--collection",
            "test-dataset",
            "--env",
            "invalid-env",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid value" in result.output


def test_missing_data_set_rejected(cli_runner, run_main_cmd):
    """Omitting --dataset should produce an error."""
    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--collection",
            "test-dataset",
            "--env",
            "local",
        ],
    )
    assert result.exit_code != 0


def test_missing_collection_rejected(cli_runner, run_main_cmd):
    """Omitting --collection should produce an error."""
    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--dataset",
            "test-dataset",
            "--env",
            "local",
        ],
    )
    assert result.exit_code != 0


def test_valid_args_calls_job(cli_runner, run_main_cmd, mocker):
    """Valid arguments should pass CLI parsing and invoke the job function."""
    mock_job = mocker.patch("jobs.job.assemble_and_load_entity")
    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--dataset",
            "test-dataset",
            "--collection",
            "test-dataset",
            "--env",
            "local",
        ],
    )
    assert result.exit_code == 0, f"CLI failed with valid arguments:\n{result.output}"
    mock_job.assert_called_once()
    kwargs = mock_job.call_args.kwargs
    assert kwargs["dataset"] == "test-dataset"
    assert kwargs["collection"] == "test-dataset"
    assert kwargs["env"] == "local"
    assert kwargs["collection_data_path"] == "s3://local-collection-data/"
    assert kwargs["parquet_datasets_path"] == "s3://local-parquet-datasets/"
    assert kwargs["database_url"] is None


def test_database_url_passed_through(cli_runner, run_main_cmd, mocker):
    """--database-url value should be forwarded to the job function."""
    mock_job = mocker.patch("jobs.job.assemble_and_load_entity")
    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--dataset",
            "test-dataset",
            "--collection",
            "test-dataset",
            "--env",
            "local",
            "--database-url",
            "postgresql://user:pass@host:5432/db",
        ],
    )
    assert result.exit_code == 0
    kwargs = mock_job.call_args.kwargs
    assert kwargs["database_url"] == "postgresql://user:pass@host:5432/db"


# --------------- E2E test ---------------

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

DATASET_RESOURCE_COLUMNS = [
    "entry_date",
    "dataset",
    "entity_count",
    "entry_count",
    "line_count",
    "mime_type",
    "internal_path",
    "internal_mime_type",
    "resource",
]

DATASET_RESOURCE_ROWS = [
    {
        "entry_date": "2024-01-15",
        "dataset": "test-dataset",
        "entity_count": "10",
        "entry_count": "20",
        "line_count": "21",
        "mime_type": "text/csv",
        "internal_path": "",
        "internal_mime_type": "",
        "resource": "res-001",
    },
]

COLUMN_FIELD_COLUMNS = ["entry_date", "field", "dataset", "resource", "column"]

COLUMN_FIELD_ROWS = [
    {
        "entry_date": "2024-01-15",
        "field": "name",
        "dataset": "test-dataset",
        "resource": "res-001",
        "column": "Name",
    },
]


def _write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_parquet(spark, path, fieldnames, rows):
    """Write rows as a parquet dataset at path (a directory), mirroring how
    EntityPipeline reads transformed data as {dataset}/*.parquet."""
    schema = StructType([StructField(f, StringType(), True) for f in fieldnames])
    data = [tuple(row.get(f, "") for f in fieldnames) for row in rows]
    spark.createDataFrame(data, schema=schema).write.mode("overwrite").parquet(path)


def test_e2e_full_load_pipeline(cli_runner, run_main_cmd, spark, tmp_path, mocker):
    """Run the full ETL pipeline end-to-end.

    Spark reads real CSV files from disk and all six transformers
    (Fact, FactResource, Entity, Issue, DatasetResource, ColumnField) run on real DataFrames.
    Parquet writes go to local disk for assertion.
    Infrastructure I/O (S3 cleanup, consumer formats, Postgres) is mocked.
    """
    dataset = "test-dataset"
    base = str(tmp_path)
    collection_dir = os.path.join(base, f"{dataset}-collection")
    parquet_base = os.path.join(base, "parquet-output/")

    # Write transformed parquet (source for fact, fact_resource, entity)
    _write_parquet(
        spark,
        os.path.join(collection_dir, "transformed", dataset),
        TRANSFORMED_COLUMNS,
        TRANSFORMED_ROWS,
    )

    # Write issue CSV
    _write_csv(
        os.path.join(collection_dir, "issue", dataset, "issue.csv"),
        ISSUE_COLUMNS,
        ISSUE_ROWS,
    )

    # Write organisation reference CSV
    _write_csv(
        os.path.join(base, "organisation-collection", "dataset", "organisation.csv"),
        ["organisation", "entity"],
        ORGANISATION_ROWS,
    )

    # Write dataset-resource CSV
    _write_csv(
        os.path.join(collection_dir, "var", "dataset-resource", dataset, "data.csv"),
        DATASET_RESOURCE_COLUMNS,
        DATASET_RESOURCE_ROWS,
    )

    # Write column-field CSV
    _write_csv(
        os.path.join(collection_dir, "var", "column-field", dataset, "data.csv"),
        COLUMN_FIELD_COLUMNS,
        COLUMN_FIELD_ROWS,
    )

    # --- Mock infrastructure I/O ---
    mocker.patch("jobs.job.create_spark_session", return_value=spark)
    mocker.patch(
        "jobs.job.get_aws_secret",
        return_value={
            "database": "testdb",
            "host": "localhost",
            "port": 5432,
            "user": "testuser",
            "password": "testpass",
        },
    )
    mocker.patch.object(spark, "stop")  # prevent finally block killing shared session
    mock_pg = mocker.patch("jobs.pipeline.write_dataframe_to_postgres_jdbc")
    mocker.patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    )

    # Mock consumer format section (flatten/schema alignment use external HTTP)
    mock_consumer_df = mocker.MagicMock()
    mock_consumer_df.columns = []
    mock_consumer_df.count.return_value = 0
    mock_consumer_df.toLocalIterator.return_value = iter([])
    mock_consumer_df.repartition.return_value.toLocalIterator.return_value = iter([])
    mocker.patch("jobs.pipeline.flatten_json_column", return_value=mock_consumer_df)
    mocker.patch("jobs.pipeline.ensure_schema_fields", return_value=mock_consumer_df)
    mocker.patch("jobs.pipeline.EntityPipeline._write_single_parquet")

    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--dataset",
            dataset,
            "--collection",
            dataset,
            "--env",
            "local",
            "--collection-data-path",
            f"{base}/",
            "--parquet-datasets-path",
            parquet_base,
        ],
    )

    assert (
        result.exit_code == 0
    ), f"E2E pipeline failed:\n{result.output}\n{result.exception}"

    # Read back parquet outputs and verify transform correctness
    expected_input_rows = len(TRANSFORMED_ROWS)
    expected_unique_facts = len({r["fact"] for r in TRANSFORMED_ROWS})
    expected_unique_entities = len({r["entity"] for r in TRANSFORMED_ROWS})

    # Fact resource: no rows removed, same count as transformed input
    fact_resource_df = spark.read.format("delta").load(
        os.path.join(parquet_base, "fact_resource")
    )
    assert fact_resource_df.count() == expected_input_rows

    # Fact: one row per unique fact value after deduplication
    fact_df = spark.read.format("delta").load(os.path.join(parquet_base, "fact"))
    assert fact_df.count() == expected_unique_facts

    # Entity: one row per unique entity (pivoted from EAV to wide format)
    entity_df = spark.read.format("delta").load(os.path.join(parquet_base, "entity"))
    assert entity_df.count() == expected_unique_entities

    issue_df = spark.read.format("delta").load(os.path.join(parquet_base, "issue"))
    assert issue_df.count() > 0

    dataset_resource_df = spark.read.format("delta").load(
        os.path.join(parquet_base, "dataset_resource")
    )
    assert dataset_resource_df.count() == len(DATASET_RESOURCE_ROWS)

    column_field_df = spark.read.format("delta").load(
        os.path.join(parquet_base, "column_field")
    )
    assert column_field_df.count() == len(COLUMN_FIELD_ROWS)

    # Entity written to Postgres
    assert mock_pg.call_count == 1
