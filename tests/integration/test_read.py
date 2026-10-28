"""Integration tests for jobs.read."""

import csv

from jobs.read import read_issue_csvs, read_old_resources


def test_read_old_resources_normalises_column_names(spark, tmp_path):
    """Kebab-case column names are normalised to snake_case on read."""
    csv_file = tmp_path / "old-resource.csv"
    csv_file.write_text("old-resource,status,resource\nabc123,410,def456\n")

    df = read_old_resources(spark, str(csv_file))

    assert "old_resource" in df.columns
    assert "old-resource" not in df.columns


def test_read_old_resources_preserves_values(spark, tmp_path):
    """Values are read correctly after column normalisation."""
    csv_file = tmp_path / "old-resource.csv"
    csv_file.write_text("old-resource,status,resource\nabc123,410,def456\n")

    df = read_old_resources(spark, str(csv_file))
    row = df.collect()[0]

    assert row["old_resource"] == "abc123"
    assert row["status"] == "410"
    assert row["resource"] == "def456"


def test_read_issue_csvs_aligns_layouts_when_values_hold_line_breaks_and_quotes(
    spark, tmp_path
):
    """A value written by Python's csv module with line breaks or doubled quotes
    stays in one row, and each file is still aligned by its own header."""
    seven = tmp_path / "seven.csv"
    with open(seven, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "dataset",
                "resource",
                "line-number",
                "entry-number",
                "field",
                "issue-type",
                "value",
            ]
        )
        writer.writerow(
            ["ds", "res-7", "2", "1", "Notes", "invalid URI", "line one\r\nline two"]
        )
    nine = tmp_path / "nine.csv"
    with open(nine, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
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
            ]
        )
        writer.writerow(
            [
                "ds",
                "res-9",
                "3",
                "2",
                "SiteNameAddress",
                "1001",
                "invalid category value",
                'The "Old" Mill, 1 High Street',
                'say "hi"',
            ]
        )

    rows = {
        r["resource"]: r
        for r in read_issue_csvs(spark, [str(seven), str(nine)]).collect()
    }

    assert set(rows) == {"res-7", "res-9"}
    assert rows["res-7"]["issue_type"] == "invalid URI"
    assert rows["res-7"]["value"].split() == ["line", "one", "line", "two"]
    assert rows["res-7"]["entity"] is None
    assert rows["res-9"]["entity"] == "1001"
    assert rows["res-9"]["issue_type"] == "invalid category value"
    assert rows["res-9"]["value"] == 'The "Old" Mill, 1 High Street'
    assert rows["res-9"]["message"] == 'say "hi"'
