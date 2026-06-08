"""Integration tests for _optimise_and_vacuum_tables."""

from jobs.job import _optimise_and_vacuum_tables


def _write_delta_table(spark, path, rows=None):
    """Write a small real Delta table to the given local path."""
    rows = rows or [(1, "a"), (2, "b")]
    df = spark.createDataFrame(rows, ["id", "val"])
    df.write.format("delta").save(str(path))
    return str(path)


class TestOptimiseAndVacuumTables:
    def test_runs_optimise_and_vacuum_on_each_table(self, spark, tmp_path):
        """Each given table path is optimised and vacuumed, returning an 'ok' result."""
        path_a = _write_delta_table(spark, tmp_path / "entity")
        path_b = _write_delta_table(spark, tmp_path / "task")

        results = _optimise_and_vacuum_tables(
            spark, [path_a, path_b], retention_hours=168
        )

        assert results == [
            {"path": path_a, "status": "ok"},
            {"path": path_b, "status": "ok"},
        ]

    def test_table_data_is_unchanged_after_maintenance(self, spark, tmp_path):
        """OPTIMIZE + VACUUM don't alter the table's logical content."""
        rows = [(1, "a"), (2, "b"), (3, "c")]
        path = _write_delta_table(spark, tmp_path / "task", rows=rows)

        _optimise_and_vacuum_tables(spark, [path], retention_hours=168)

        result_rows = {
            (row["id"], row["val"])
            for row in spark.read.format("delta").load(path).collect()
        }
        assert result_rows == set(rows)

    def test_continues_past_a_failing_table(self, spark, tmp_path):
        """A bad path produces an 'error' result without stopping the others."""
        good_path = _write_delta_table(spark, tmp_path / "task")
        bad_path = str(tmp_path / "not-a-delta-table")

        results = _optimise_and_vacuum_tables(
            spark, [bad_path, good_path], retention_hours=168
        )

        assert results[0]["status"] == "error"
        assert results[0]["path"] == bad_path
        assert "error" in results[0]
        assert results[1] == {"path": good_path, "status": "ok"}

    def test_returns_empty_list_for_no_tables(self, spark):
        """An empty list of paths produces an empty result list."""
        assert _optimise_and_vacuum_tables(spark, [], retention_hours=168) == []
