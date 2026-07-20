"""
Integration tests for write_dataframe_to_postgres_jdbc.

Uses a real PostGIS database (testcontainers locally, GitHub Actions service in CI)
and a real Spark session to verify the staging table pattern works end-to-end.
"""

from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from jobs.pipeline import DATASET_QUALITY_PG_TYPES, PROVISION_QUALITY_PG_TYPES
from jobs.utils.postgres_writer_utils import (
    write_dataframe_to_postgres_jdbc,
    write_entity_subdivided_to_postgres,
    write_old_entity_to_postgres,
    write_table_to_postgres,
)


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
def test_write_entity_subdivided_creates_rows(
    spark, db_url, db_conn, clean_entity_subdivided_table
):
    """Entities with polygon geometry produce subdivided rows in entity_subdivided."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("entity", LongType(), True),
            StructField("dataset", StringType(), True),
            StructField("geometry", StringType(), True),
        ]
    )
    # A simple multipolygon — ST_SubDivide with 256 max vertices returns it unchanged
    wkt = "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))"
    df = spark.createDataFrame([(1001, "flood-risk-zones", wkt)], schema)

    write_entity_subdivided_to_postgres(df, "flood-risk-zones", db_url)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT entity, dataset FROM entity_subdivided WHERE dataset = %s ORDER BY entity;",
        ("flood-risk-zones",),
    )
    rows = cur.fetchall()
    cur.close()

    assert len(rows) >= 1
    assert rows[0][0] == 1001
    assert rows[0][1] == "flood-risk-zones"


@pytest.mark.database
def test_write_entity_subdivided_replaces_existing(
    spark, db_url, db_conn, clean_entity_subdivided_table
):
    """Writing the same dataset twice replaces the old subdivided rows atomically."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("entity", LongType(), True),
            StructField("dataset", StringType(), True),
            StructField("geometry", StringType(), True),
        ]
    )
    wkt = "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))"

    df_v1 = spark.createDataFrame([(1001, "flood-risk-zones", wkt)], schema)
    write_entity_subdivided_to_postgres(df_v1, "flood-risk-zones", db_url)

    df_v2 = spark.createDataFrame(
        [(2001, "flood-risk-zones", wkt), (2002, "flood-risk-zones", wkt)], schema
    )
    write_entity_subdivided_to_postgres(df_v2, "flood-risk-zones", db_url)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT entity FROM entity_subdivided WHERE dataset = %s ORDER BY entity;",
        ("flood-risk-zones",),
    )
    rows = cur.fetchall()
    cur.close()

    entities = [r[0] for r in rows]
    assert 1001 not in entities
    assert 2001 in entities
    assert 2002 in entities


@pytest.mark.database
def test_write_entity_subdivided_excludes_null_geometry(
    spark, db_url, db_conn, clean_entity_subdivided_table
):
    """Entities with NULL geometry are excluded from entity_subdivided."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("entity", LongType(), True),
            StructField("dataset", StringType(), True),
            StructField("geometry", StringType(), True),
        ]
    )
    wkt = "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))"
    df = spark.createDataFrame(
        [(1001, "flood-risk-zones", wkt), (1002, "flood-risk-zones", None)], schema
    )

    write_entity_subdivided_to_postgres(df, "flood-risk-zones", db_url)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT entity FROM entity_subdivided WHERE dataset = %s ORDER BY entity;",
        ("flood-risk-zones",),
    )
    rows = cur.fetchall()
    cur.close()

    entities = [r[0] for r in rows]
    assert 1001 in entities
    assert 1002 not in entities


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


def _build_old_entity_df(spark: SparkSession, rows: list) -> DataFrame:
    """Build a Spark DataFrame matching the old_entity schema."""
    schema = StructType(
        [
            StructField("old_entity", LongType(), True),
            StructField("status", IntegerType(), True),
            StructField("entity", LongType(), True),
            StructField("notes", StringType(), True),
            StructField("end_date", DateType(), True),
            StructField("entry_date", DateType(), True),
            StructField("start_date", DateType(), True),
            StructField("dataset", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _query_old_entity_rows(db_conn) -> list:
    """Return all rows from old_entity ordered by old_entity id."""
    cur = db_conn.cursor()
    cur.execute(
        "SELECT old_entity, status, entity, notes, end_date, entry_date, start_date, dataset"
        " FROM old_entity ORDER BY old_entity;"
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def _count_tables_matching(db_conn, pattern: str) -> int:
    """Return the number of tables in the public schema whose names match the given LIKE pattern."""
    cur = db_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE %s;",
        (pattern,),
    )
    count = cur.fetchone()[0]
    cur.close()
    return count


@pytest.mark.database
def test_write_old_entity_creates_rows(spark, db_url, db_conn, clean_old_entity_table):
    """Write a DataFrame and verify rows land in the old_entity table."""
    df = _build_old_entity_df(
        spark,
        [
            (
                111,
                301,
                222,
                None,
                date(2024, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
                "ancient-woodland",
            ),
            (
                333,
                301,
                444,
                "duplicate",
                None,
                date(2021, 6, 1),
                None,
                "ancient-woodland",
            ),
        ],
    )

    write_old_entity_to_postgres(df, db_url)

    rows = _query_old_entity_rows(db_conn)
    assert len(rows) == 2
    assert rows[0][0] == 111
    assert rows[0][1] == 301
    assert rows[0][2] == 222
    assert rows[1][0] == 333
    assert rows[1][3] == "duplicate"
    assert _count_tables_matching(db_conn, "old_entity_staging_%") == 0


@pytest.mark.database
def test_write_old_entity_replaces_all_rows(
    spark, db_url, db_conn, clean_old_entity_table
):
    """Writing twice replaces all rows — the entire table is replaced atomically."""
    df_v1 = _build_old_entity_df(
        spark,
        [(111, 301, 222, None, None, date(2020, 1, 1), None, "ancient-woodland")],
    )
    write_old_entity_to_postgres(df_v1, db_url)

    df_v2 = _build_old_entity_df(
        spark,
        [
            (555, 301, 666, None, None, date(2022, 1, 1), None, "flood-risk-zone"),
            (777, 301, 888, None, None, date(2022, 1, 1), None, "flood-risk-zone"),
        ],
    )
    write_old_entity_to_postgres(df_v2, db_url)

    rows = _query_old_entity_rows(db_conn)
    old_entity_ids = [r[0] for r in rows]
    assert 111 not in old_entity_ids
    assert 555 in old_entity_ids
    assert 777 in old_entity_ids
    assert _count_tables_matching(db_conn, "old_entity_staging_%") == 0


@pytest.mark.database
def test_write_old_entity_handles_nulls(spark, db_url, db_conn, clean_old_entity_table):
    """Rows with all-null optional fields are written correctly."""
    df = _build_old_entity_df(
        spark,
        [(999, None, None, None, None, None, None, "some-dataset")],
    )

    write_old_entity_to_postgres(df, db_url)

    rows = _query_old_entity_rows(db_conn)
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == 999
    assert row[1] is None  # status
    assert row[2] is None  # entity
    assert row[3] is None  # notes
    assert _count_tables_matching(db_conn, "old_entity_staging_%") == 0


@pytest.mark.database
def test_write_table_to_postgres_provision_quality(
    spark, db_url, db_conn, clean_provision_quality_table
):
    """Writer fills provision_quality; nulls preserved; a second write replaces."""
    schema = StructType(
        [
            StructField("dataset", StringType(), True),
            StructField("organisation", StringType(), True),
            StructField("organisation_name", StringType(), True),
            StructField("has_active_endpoint", BooleanType(), True),
            StructField("has_active_resource", BooleanType(), True),
            StructField("owns_entities", BooleanType(), True),
            StructField("is_designated_provider", BooleanType(), True),
            StructField("quality", StringType(), True),
            StructField("entity_count", LongType(), True),
            StructField("quality_score", DoubleType(), True),
        ]
    )
    rows = [
        (
            "conservation-area",
            "local-authority:ADU",
            "Adur",
            True,
            True,
            True,
            True,
            "authoritative",
            42,
            None,
        ),
        (
            "conservation-area",
            "government-organisation:MHCLG",
            None,
            True,
            False,
            False,
            False,
            None,
            0,
            None,
        ),
    ]
    df = spark.createDataFrame(rows, schema)

    write_table_to_postgres(df, "provision_quality", PROVISION_QUALITY_PG_TYPES, db_url)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT organisation, organisation_name, has_active_endpoint, quality, "
        "entity_count, quality_score FROM provision_quality ORDER BY organisation;"
    )
    result = cur.fetchall()
    cur.close()
    # Release the read lock (pg8000 autocommit is off) before the next write's
    # TRUNCATE, or it blocks on this connection's open transaction.
    db_conn.rollback()

    assert len(result) == 2
    by_org = {r[0]: r for r in result}
    adu = by_org["local-authority:ADU"]
    assert adu[1] == "Adur"
    assert adu[2] is True
    assert adu[3] == "authoritative"
    assert adu[4] == 42
    assert adu[5] is None
    mhclg = by_org["government-organisation:MHCLG"]
    assert mhclg[1] is None  # organisation_name null preserved
    assert mhclg[3] is None  # quality null preserved

    # A second write replaces (TRUNCATE + INSERT), it does not append.
    df2 = spark.createDataFrame(
        [
            (
                "green-belt",
                "local-authority:XYZ",
                "XYZ Council",
                False,
                False,
                True,
                False,
                "some",
                5,
                None,
            )
        ],
        schema,
    )
    write_table_to_postgres(
        df2, "provision_quality", PROVISION_QUALITY_PG_TYPES, db_url
    )
    cur = db_conn.cursor()
    cur.execute("SELECT dataset FROM provision_quality;")
    after = cur.fetchall()
    cur.close()
    db_conn.rollback()
    assert len(after) == 1
    assert after[0][0] == "green-belt"
    assert _count_tables_matching(db_conn, "provision_quality_staging_%") == 0


@pytest.mark.database
def test_write_table_to_postgres_dataset_quality_integer_counts(
    spark, db_url, db_conn, clean_dataset_quality_table
):
    """Spark Long count columns land correctly in INTEGER columns."""
    schema = StructType(
        [
            StructField("dataset", StringType(), True),
            StructField("authoritative_organisations", LongType(), True),
            StructField("some_organisations", LongType(), True),
            StructField("total_organisations", LongType(), True),
            StructField("total_entities", LongType(), True),
            StructField("quality_score", DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(
        [("conservation-area", 131, 175, 306, 10941, None)], schema
    )
    write_table_to_postgres(df, "dataset_quality", DATASET_QUALITY_PG_TYPES, db_url)

    cur = db_conn.cursor()
    cur.execute(
        "SELECT dataset, authoritative_organisations, some_organisations, "
        "total_organisations, total_entities FROM dataset_quality;"
    )
    result = cur.fetchall()
    cur.close()
    db_conn.rollback()
    assert len(result) == 1
    assert list(result[0]) == ["conservation-area", 131, 175, 306, 10941]
