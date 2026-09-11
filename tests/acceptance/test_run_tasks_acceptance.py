"""
Acceptance tests for the run_tasks entry point.

CLI tests verify the interface. The E2E test creates a realistic collection
directory structure, runs the full task generation pipeline, and asserts on
the business outcomes written to the output Delta table.
"""

import csv
import json
import os

# -- CLI tests ----------------------------------------------------------------


def test_missing_required_args_returns_nonzero(cli_runner, run_tasks_cmd):
    """Running with no arguments should fail."""
    result = cli_runner.invoke(run_tasks_cmd, [])
    assert result.exit_code != 0


def test_help_flag_returns_zero(cli_runner, run_tasks_cmd):
    """--help should succeed and list the key options."""
    result = cli_runner.invoke(run_tasks_cmd, ["--help"])
    assert result.exit_code == 0
    assert "--env" in result.output
    assert "--collection-data-path" in result.output
    assert "--entity-data-path" in result.output
    assert "--output-path" in result.output
    assert "--parquet-datasets-path" in result.output


def test_invalid_env_rejected(cli_runner, run_tasks_cmd):
    """An unrecognised env value should be rejected by Click."""
    result = cli_runner.invoke(run_tasks_cmd, ["--env", "not-a-real-env"])
    assert result.exit_code != 0
    assert "Invalid value" in result.output


# -- Test data ----------------------------------------------------------------

LOG_COLUMNS = ["endpoint", "resource", "status", "entry-date", "exception"]

# conservation-area-collection: one success (200) and two failures
CA_LOG_ROWS = [
    {
        "endpoint": "endpoint-ca-aaa",
        "resource": "resource-ca-aaa",
        "status": "200",
        "entry-date": "2026-01-01",
        "exception": "",
    },
    {
        "endpoint": "endpoint-ca-bbb",
        "resource": "",
        "status": "404",
        "entry-date": "2026-01-01",
        "exception": "Not Found",
    },
    {
        "endpoint": "endpoint-ca-ccc",
        "resource": "",
        "status": "500",
        "entry-date": "2026-01-01",
        "exception": "Connection refused",
    },
]

# tree-collection: one failure for an endpoint that serves two datasets
TREE_LOG_ROWS = [
    {
        "endpoint": "endpoint-tree-aaa",
        "resource": "",
        "status": "503",
        "entry-date": "2026-01-01",
        "exception": "Service Unavailable",
    },
]

RESOURCE_COLUMNS = [
    "resource",
    "datasets",
    "organisations",
    "endpoints",
    "start-date",
    "end-date",
]

CA_RESOURCE_ROWS = [
    {
        "resource": "resource-ca-aaa",
        "datasets": "conservation-area",
        "organisations": "organisation:1",
        "endpoints": "endpoint-ca-aaa",
        "start-date": "2026-07-01",
        "end-date": "",
    },
]

SOURCE_COLUMNS = ["endpoint", "pipelines", "organisation", "end-date"]

CA_SOURCE_ROWS = [
    {
        "endpoint": "endpoint-ca-aaa",
        "pipelines": "conservation-area",
        "organisation": "organisation:1",
        "end-date": "",
    },
    {
        "endpoint": "endpoint-ca-bbb",
        "pipelines": "conservation-area",
        "organisation": "organisation:1",
        "end-date": "",
    },
    {
        "endpoint": "endpoint-ca-ccc",
        "pipelines": "conservation-area",
        "organisation": "organisation:1",
        "end-date": "",
    },
]

# endpoint-tree-aaa serves two datasets — tests the multi-dataset expansion fix
TREE_SOURCE_ROWS = [
    {
        "endpoint": "endpoint-tree-aaa",
        "pipelines": "tree-preservation-order;tree",
        "organisation": "organisation:2",
        "end-date": "",
    },
]

ISSUE_COLUMNS = [
    "dataset",
    "resource",
    "line-number",
    "entry-number",
    "field",
    "entity",
    "issue-type",
    "value",
    "message",
]

# Two rows for the same issue — should group into one task with count=2
CA_ISSUE_ROWS = [
    {
        "dataset": "conservation-area",
        "resource": "resource-ca-aaa",
        "line-number": "1",
        "entry-number": "1",
        "field": "geometry",
        "entity": "4400001",
        "issue-type": "invalid-geometry",
        "value": "POLYGON((0 0))",
        "message": "invalid geometry",
    },
    {
        "dataset": "conservation-area",
        "resource": "resource-ca-aaa",
        "line-number": "2",
        "entry-number": "2",
        "field": "geometry",
        "entity": "4400002",
        "issue-type": "invalid-geometry",
        "value": "POLYGON((1 1))",
        "message": "invalid geometry",
    },
]


def _write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# -- E2E test -----------------------------------------------------------------


def test_e2e_task_generation_pipeline(
    cli_runner, run_tasks_cmd, spark, tmp_path, mocker
):
    """Run the full task generation pipeline end-to-end.

    Spark reads real CSV files from local disk. Infrastructure I/O (Spark session
    lifecycle, _load_issue_type_df HTTP call, Postgres write) is mocked.
    The output Delta table is read back and asserted against expected business outcomes.
    """
    base = str(tmp_path)
    parquet_base = os.path.join(base, "parquet-output")

    # -- Write collection files -----------------------------------------------

    # conservation-area collection
    ca = os.path.join(base, "conservation-area-collection")
    _write_csv(os.path.join(ca, "collection", "log.csv"), LOG_COLUMNS, CA_LOG_ROWS)
    _write_csv(
        os.path.join(ca, "collection", "resource.csv"),
        RESOURCE_COLUMNS,
        CA_RESOURCE_ROWS,
    )
    _write_csv(
        os.path.join(ca, "collection", "source.csv"), SOURCE_COLUMNS, CA_SOURCE_ROWS
    )
    _write_csv(
        os.path.join(ca, "issue", "conservation-area", "resource-ca-aaa.csv"),
        ISSUE_COLUMNS,
        CA_ISSUE_ROWS,
    )

    # tree collection — only log + source, no successful resources or issues
    tree = os.path.join(base, "tree-collection")
    _write_csv(os.path.join(tree, "collection", "log.csv"), LOG_COLUMNS, TREE_LOG_ROWS)
    _write_csv(os.path.join(tree, "collection", "resource.csv"), RESOURCE_COLUMNS, [])
    _write_csv(
        os.path.join(tree, "collection", "source.csv"), SOURCE_COLUMNS, TREE_SOURCE_ROWS
    )

    # organisation:1 is designated for conservation-area but owns nothing in
    # the entity data below — exercises the authority leg's "none" case.
    _write_csv(
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
    _write_csv(
        os.path.join(
            base, "config", "pipeline", "conservation-area", "entity-organisation.csv"
        ),
        ["dataset", "organisation"],
        [{"dataset": "conservation-area", "organisation": "organisation:1"}],
    )
    _write_csv(
        os.path.join(base, "entity", "conservation-area.csv"),
        ["entity", "organisation-entity", "quality"],
        [],
    )
    # The authority leg scopes tasks to datasets this environment builds, so
    # every dataset under test has to be live in the specification.
    _write_csv(
        os.path.join(base, "specification", "dataset.csv"),
        ["dataset", "environment", "end-date"],
        [
            {"dataset": dataset, "environment": "production", "end-date": ""}
            for dataset in ("conservation-area", "tree-preservation-order", "tree")
        ],
    )

    # -- Mock infrastructure --------------------------------------------------

    mocker.patch("jobs.job.create_spark_session", return_value=spark)
    mocker.patch.object(spark, "stop")  # prevent shared session being killed

    # Replace the GitHub HTTP call with a controlled issue-type mapping
    mock_issue_type_df = spark.createDataFrame(
        [("invalid-geometry", "error", "external", "validity")],
        ["issue_type", "severity", "responsibility", "quality_dimension"],
    )
    mocker.patch(
        "jobs.pipeline.task._load_issue_type_df", return_value=mock_issue_type_df
    )

    mock_pg = mocker.patch("jobs.pipeline.task.write_task_to_postgres")

    # -- Run ------------------------------------------------------------------

    result = cli_runner.invoke(
        run_tasks_cmd,
        [
            "--env",
            "local",
            "--collection-data-path",
            f"{base}/",
            "--entity-data-path",
            os.path.join(base, "entity"),
            "--parquet-datasets-path",
            parquet_base,
            "--database-url",
            "postgresql://test:test@localhost:5432/testdb",
        ],
    )

    assert (
        result.exit_code == 0
    ), f"Pipeline failed:\n{result.output}\n{result.exception}"

    # -- Assert output --------------------------------------------------------

    tasks_df = spark.read.format("delta").load(os.path.join(parquet_base, "task"))
    rows = tasks_df.collect()

    # No task should have an empty dataset
    assert all(
        row["dataset"] != "" for row in rows
    ), "Some tasks have empty dataset: " + str([r for r in rows if r["dataset"] == ""])

    log_rows = [r for r in rows if r["task_source"] == "log"]
    issue_rows = [r for r in rows if r["task_source"] == "issue"]

    # The 200 response should not have produced a log task
    log_endpoints_with_tasks = {row["endpoint"] for row in log_rows}
    assert "endpoint-ca-aaa" not in log_endpoints_with_tasks

    # Two failed ca endpoints → two log tasks
    assert len(log_rows) == 4  # 2 ca failures + 2 from the multi-dataset tree endpoint
    assert len(issue_rows) == 1

    # The multi-dataset endpoint produced one task per dataset
    tree_log_tasks = [r for r in log_rows if r["endpoint"] == "endpoint-tree-aaa"]
    tree_datasets = {r["dataset"] for r in tree_log_tasks}
    assert tree_datasets == {"tree-preservation-order", "tree"}

    # Issue task grouped the two identical rows into one with count=2
    issue_details = json.loads(issue_rows[0]["details"])
    assert issue_details["issue_type"] == "invalid-geometry"
    assert issue_details["count"] == 2

    # Issue task carries organisation/endpoint from the resource it was raised against
    assert issue_rows[0]["organisation"] == "organisation:1"
    assert issue_rows[0]["endpoint"] == "endpoint-ca-aaa"

    # organisation:1 is designated for conservation-area but owns nothing ->
    # one authority task, quality "none"
    authority_rows = [r for r in rows if r["task_source"] == "provision"]
    assert len(authority_rows) == 1
    assert authority_rows[0]["dataset"] == "conservation-area"
    assert authority_rows[0]["organisation"] == "organisation:1"
    assert authority_rows[0]["quality_dimension"] == "authoritativeness"
    assert json.loads(authority_rows[0]["details"])["quality"] == "none"

    # All references are unique across the whole output
    references = [r["reference"] for r in rows]
    assert len(references) == len(set(references))

    # Postgres write was called once with the full tasks DataFrame
    assert mock_pg.call_count == 1

    # --output-path was not passed, so this also exercises the CLI default
    # ({collection-data-path}dataset/) — the path production actually resolves,
    # since the DAG doesn't pass the flag either.
    with open(os.path.join(base, "dataset", "task.csv")) as f:
        csv_rows = list(csv.DictReader(f))
    assert {r["reference"] for r in csv_rows} == {r["reference"] for r in rows}
