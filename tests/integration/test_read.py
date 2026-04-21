"""Integration tests for jobs.read."""

from jobs.read import read_old_resources


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
