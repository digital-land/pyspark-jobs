"""
Integration tests for write_dataframe_to_postgres_jdbc.

Uses a real PostGIS database (testcontainers locally, GitHub Actions service in CI)
and a real Spark session to verify the staging table pattern works end-to-end.
"""

import pytest

from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc


def _build_entity_df(spark, rows):
    """Build a Spark DataFrame matching how the pipeline calls the writer.

    The json column arrives as a string from EntityTransformer (via to_json).
    Geojson may not be present, so _ensure_required_columns adds it as NULL.
    """
    from pyspark.sql.types import (
        DateType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("entity", LongType(), True),
            StructField("name", StringType(), True),
            StructField("entry_date", DateType(), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True),
            StructField("dataset", StringType(), True),
            StructField("json", StringType(), True),
            StructField("organisation_entity", LongType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("typology", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("point", StringType(), True),
            StructField("quality", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _query_entity_rows(db_conn, dataset=None):
    """Query entity table rows, optionally filtered by dataset."""
    cur = db_conn.cursor()
    if dataset:
        cur.execute(
            "SELECT entity, name, dataset, prefix, reference FROM entity "
            "WHERE dataset = %s ORDER BY entity;",
            (dataset,),
        )
    else:
        cur.execute(
            "SELECT entity, name, dataset, prefix, reference FROM entity "
            "ORDER BY entity;"
        )
    rows = cur.fetchall()
    cur.close()
    return rows


@pytest.mark.database
def test_write_creates_staging_and_swaps_to_entity(
    spark, db_url, db_conn, clean_entity_table
):
    """Write a DataFrame and verify rows land in the entity table."""
    from datetime import date

    df = _build_entity_df(
        spark,
        [
            (
                1001,
                "Test A",
                date(2024, 1, 15),
                date(2024, 1, 1),
                None,
                "test-ds",
                '{"key":"value"}',
                100,
                "test",
                "REF-001",
                "geography",
                None,
                None,
                "good",
            ),
        ],
    )

    write_dataframe_to_postgres_jdbc(df, "entity", "test-ds", db_url)

    rows = _query_entity_rows(db_conn, "test-ds")
    assert len(rows) == 1
    assert rows[0][0] == 1001  # entity
    assert rows[0][1] == "Test A"  # name
    assert rows[0][2] == "test-ds"  # dataset


@pytest.mark.database
def test_write_replaces_existing_dataset_rows(
    spark, db_url, db_conn, clean_entity_table
):
    """Writing the same dataset twice replaces the old rows atomically."""
    from datetime import date

    df_v1 = _build_entity_df(
        spark,
        [
            (
                1001,
                "Version 1",
                date(2024, 1, 1),
                None,
                None,
                "replace-ds",
                None,
                None,
                "test",
                "REF-001",
                "geography",
                None,
                None,
                None,
            ),
        ],
    )
    write_dataframe_to_postgres_jdbc(df_v1, "entity", "replace-ds", db_url)

    df_v2 = _build_entity_df(
        spark,
        [
            (
                2001,
                "Version 2a",
                date(2024, 2, 1),
                None,
                None,
                "replace-ds",
                None,
                None,
                "test",
                "REF-002",
                "geography",
                None,
                None,
                None,
            ),
            (
                2002,
                "Version 2b",
                date(2024, 2, 1),
                None,
                None,
                "replace-ds",
                None,
                None,
                "test",
                "REF-003",
                "geography",
                None,
                None,
                None,
            ),
        ],
    )
    write_dataframe_to_postgres_jdbc(df_v2, "entity", "replace-ds", db_url)

    rows = _query_entity_rows(db_conn, "replace-ds")
    assert len(rows) == 2
    assert rows[0][0] == 2001
    assert rows[1][0] == 2002


@pytest.mark.database
def test_write_preserves_other_dataset_rows(spark, db_url, db_conn, clean_entity_table):
    """Writing dataset B does not affect dataset A rows."""
    from datetime import date

    df_a = _build_entity_df(
        spark,
        [
            (
                1001,
                "Dataset A",
                date(2024, 1, 1),
                None,
                None,
                "ds-a",
                None,
                None,
                "test",
                "REF-A",
                "geography",
                None,
                None,
                None,
            ),
        ],
    )
    write_dataframe_to_postgres_jdbc(df_a, "entity", "ds-a", db_url)

    df_b = _build_entity_df(
        spark,
        [
            (
                2001,
                "Dataset B",
                date(2024, 1, 1),
                None,
                None,
                "ds-b",
                None,
                None,
                "test",
                "REF-B",
                "geography",
                None,
                None,
                None,
            ),
        ],
    )
    write_dataframe_to_postgres_jdbc(df_b, "entity", "ds-b", db_url)

    all_rows = _query_entity_rows(db_conn)
    assert len(all_rows) == 2

    rows_a = _query_entity_rows(db_conn, "ds-a")
    assert len(rows_a) == 1
    assert rows_a[0][0] == 1001


@pytest.mark.database
def test_write_handles_null_geometry(spark, db_url, db_conn, clean_entity_table):
    """Rows with NULL geometry and point columns are written correctly."""
    from datetime import date

    df = _build_entity_df(
        spark,
        [
            (
                1001,
                "No geom",
                date(2024, 1, 1),
                None,
                None,
                "null-geom-ds",
                None,
                None,
                "test",
                "REF-001",
                "geography",
                None,
                None,
                None,
            ),
        ],
    )
    write_dataframe_to_postgres_jdbc(df, "entity", "null-geom-ds", db_url)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT geometry, point FROM entity WHERE dataset = %s;",
        ("null-geom-ds",),
    )
    row = cur.fetchone()
    cur.close()

    assert row[0] is None  # geometry
    assert row[1] is None  # point


@pytest.mark.database
def test_write_stores_json_column(spark, db_url, db_conn, clean_entity_table):
    """JSON column string is stored as JSONB in the database."""
    from datetime import date

    df = _build_entity_df(
        spark,
        [
            (
                1001,
                "JSON test",
                date(2024, 1, 1),
                None,
                None,
                "json-ds",
                '{"foo":"bar"}',
                None,
                "test",
                "REF-001",
                "geography",
                None,
                None,
                None,
            ),
        ],
    )
    write_dataframe_to_postgres_jdbc(df, "entity", "json-ds", db_url)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT json, geojson FROM entity WHERE dataset = %s;",
        ("json-ds",),
    )
    row = cur.fetchone()
    cur.close()

    assert row[0] is not None  # json written
    assert row[1] is None  # geojson NULL (column not in input)
