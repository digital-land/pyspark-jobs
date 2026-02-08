"""
Acceptance tests for the run_main entry point.

These tests use click's CliRunner to invoke the entry point command
and verify the CLI interface works correctly.
"""

import csv
import os
from unittest.mock import patch


def test_missing_required_args_returns_nonzero(cli_runner, run_main_cmd):
    """Running with no arguments should fail with a non-zero exit code."""
    result = cli_runner.invoke(run_main_cmd, [])
    assert result.exit_code != 0
    assert "Missing" in result.output or "required" in result.output.lower()


def test_help_flag_returns_zero(cli_runner, run_main_cmd):
    """Running with --help should succeed and display usage info."""
    result = cli_runner.invoke(run_main_cmd, ["--help"])
    assert result.exit_code == 0
    assert "--load_type" in result.output
    assert "--dataset" in result.output
    assert "--collection" in result.output
    assert "--env" in result.output
    assert "--use-jdbc" in result.output


def test_invalid_load_type_rejected(cli_runner, run_main_cmd):
    """An invalid load_type should be rejected by click's Choice validation."""
    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--load_type",
            "invalid",
            "--dataset",
            "test-dataset",
            "--collection",
            "test-dataset",
            "--env",
            "local",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid value" in result.output


def test_invalid_env_rejected(cli_runner, run_main_cmd):
    """An invalid env value should be rejected by click's Choice validation."""
    result = cli_runner.invoke(
        run_main_cmd,
        [
            "--load_type",
            "full",
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
            "--load_type",
            "full",
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
            "--load_type",
            "full",
            "--dataset",
            "test-dataset",
            "--env",
            "local",
        ],
    )
    assert result.exit_code != 0


def test_valid_args_calls_main(cli_runner, run_main_cmd):
    """Valid arguments should pass CLI parsing and invoke the main function."""
    with patch("jobs.main_collection_data.main") as mock_main:
        result = cli_runner.invoke(
            run_main_cmd,
            [
                "--load_type",
                "full",
                "--dataset",
                "test-dataset",
                "--collection",
                "test-dataset",
                "--env",
                "local",
            ],
        )
        assert (
            result.exit_code == 0
        ), f"CLI failed with valid arguments:\n{result.output}"
        mock_main.assert_called_once()
        kwargs = mock_main.call_args.kwargs
        assert kwargs["load_type"] == "full"
        assert kwargs["dataset"] == "test-dataset"
        assert kwargs["collection"] == "test-dataset"
        assert kwargs["env"] == "local"
        assert kwargs["use_jdbc"] is False
        assert kwargs["collection_data_path"] == "s3://local-collection-data/"
        assert kwargs["parquet_datasets_path"] == "s3://local-parquet-datasets/"


def test_use_jdbc_flag(cli_runner, run_main_cmd):
    """The --use-jdbc flag should be passed through to main."""
    with patch("jobs.main_collection_data.main") as mock_main:
        result = cli_runner.invoke(
            run_main_cmd,
            [
                "--load_type",
                "full",
                "--dataset",
                "test-dataset",
                "--collection",
                "test-dataset",
                "--env",
                "local",
                "--use-jdbc",
            ],
        )
        assert result.exit_code == 0
        assert mock_main.call_args.kwargs["use_jdbc"] is True


# --------------- E2E test ---------------

TRANSFORMED_COLUMNS = [
    "entity",
    "field",
    "value",
    "entry_date",
    "entry_number",
    "priority",
    "end_date",
    "start_date",
    "fact",
    "reference_entity",
    "resource",
]

TRANSFORMED_ROWS = [
    {
        "entity": "1001",
        "field": "name",
        "value": "Test Property A",
        "entry_date": "2024-01-15",
        "entry_number": "1",
        "priority": "2",
        "end_date": "",
        "start_date": "2024-01-01",
        "fact": "fact-001",
        "reference_entity": "",
        "resource": "res-001",
    },
    {
        "entity": "1001",
        "field": "reference",
        "value": "REF-001",
        "entry_date": "2024-01-15",
        "entry_number": "1",
        "priority": "1",
        "end_date": "",
        "start_date": "2024-01-01",
        "fact": "fact-002",
        "reference_entity": "1001",
        "resource": "res-001",
    },
    {
        "entity": "1001",
        "field": "prefix",
        "value": "test",
        "entry_date": "2024-01-15",
        "entry_number": "1",
        "priority": "1",
        "end_date": "",
        "start_date": "2024-01-01",
        "fact": "fact-003",
        "reference_entity": "",
        "resource": "res-002",
    },
    {
        "entity": "1001",
        "field": "organisation",
        "value": "local-authority:ABC",
        "entry_date": "2024-01-15",
        "entry_number": "1",
        "priority": "1",
        "end_date": "",
        "start_date": "2024-01-01",
        "fact": "fact-004",
        "reference_entity": "",
        "resource": "res-002",
    },
    {
        "entity": "1001",
        "field": "entry-date",
        "value": "2024-01-15",
        "entry_date": "2024-01-15",
        "entry_number": "1",
        "priority": "1",
        "end_date": "",
        "start_date": "2024-01-01",
        "fact": "fact-005",
        "reference_entity": "",
        "resource": "res-001",
    },
    {
        "entity": "1001",
        "field": "start-date",
        "value": "2024-01-01",
        "entry_date": "2024-01-15",
        "entry_number": "1",
        "priority": "1",
        "end_date": "",
        "start_date": "2024-01-01",
        "fact": "fact-006",
        "reference_entity": "",
        "resource": "res-001",
    },
]

ISSUE_COLUMNS = [
    "entity",
    "entry_number",
    "field",
    "issue_type",
    "line_number",
    "dataset",
    "resource",
    "value",
    "message",
]

ISSUE_ROWS = [
    {
        "entity": "1001",
        "entry_number": "1",
        "field": "name",
        "issue_type": "warning",
        "line_number": "10",
        "dataset": "test-dataset",
        "resource": "res-001",
        "value": "Test Property A",
        "message": "Name format warning",
    },
]

ORGANISATION_ROWS = [
    {"organisation": "local-authority:ABC", "entity": "100"},
]


def _write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_e2e_full_load_pipeline(cli_runner, run_main_cmd, spark, tmp_path):
    """Run the full ETL pipeline end-to-end.

    Spark reads real CSV files from disk and all four transformers
    (Fact, FactResource, Entity, Issue) run on real DataFrames.
    Only infrastructure I/O (S3 writes, Postgres, external HTTP) is mocked.
    """
    dataset = "test-dataset"
    base = str(tmp_path)
    collection = os.path.join(base, f"{dataset}-collection")

    # Write transformed CSV (source for fact, fact_resource, entity)
    _write_csv(
        os.path.join(collection, "transformed", dataset, "data.csv"),
        TRANSFORMED_COLUMNS,
        TRANSFORMED_ROWS,
    )

    # Write issue CSV
    # issue_path = collection_data_path + "/issue/" + dataset + "/*.csv"
    _write_csv(
        os.path.join(collection, "issue", dataset, "issue.csv"),
        ISSUE_COLUMNS,
        ISSUE_ROWS,
    )

    # Write organisation reference CSV at the path main() derives from collection_data_path:
    # organisation_path = collection_data_path + "organisation-collection/dataset/organisation.csv"
    _write_csv(
        os.path.join(base, "organisation-collection", "dataset", "organisation.csv"),
        ["organisation", "entity"],
        ORGANISATION_ROWS,
    )

    # Entity DF returned by mocked write_entity_formats_to_s3
    # (main() later drops processed_timestamp/year/month/day before Postgres write)
    entity_output_df = spark.createDataFrame(
        [
            {
                "entity": "1001",
                "name": "Test Property A",
                "dataset": dataset,
                "processed_timestamp": "2024-01-15T00:00:00",
                "year": "2024",
                "month": "01",
                "day": "15",
            },
        ]
    )

    with patch("jobs.main_collection_data.validate_s3_path"), patch(
        "jobs.main_collection_data.write_entity_formats_to_s3",
        return_value=entity_output_df,
    ), patch("jobs.main_collection_data.write_parquet_to_s3") as mock_parquet, patch(
        "jobs.main_collection_data.write_dataframe_to_postgres_jdbc"
    ) as mock_pg, patch(
        "jobs.transform.entity_transformer.get_dataset_typology",
        return_value="geography",
    ):

        result = cli_runner.invoke(
            run_main_cmd,
            [
                "--load_type",
                "full",
                "--dataset",
                dataset,
                "--collection",
                dataset,
                "--env",
                "local",
                "--collection-data-path",
                f"{base}/",
            ],
        )

        assert (
            result.exit_code == 0
        ), f"E2E pipeline failed:\n{result.output}\n{result.exception}"
        # 4 tables written to S3: fact, fact_resource, entity, issue
        assert mock_parquet.call_count == 4
        # entity written to Postgres
        assert mock_pg.call_count == 1
