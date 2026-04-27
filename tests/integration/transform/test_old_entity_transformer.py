"""Integration tests for old_entity_transformer."""

from unittest.mock import patch

from pyspark.sql.types import LongType, StringType, StructField, StructType

from jobs.config.schema import get_schema
from jobs.transform.old_entity_transformer import fetch_dataset_df, transform_old_entity

_SPEC_CSV = """\
dataset,collection,entity_minimum,entity_maximum,other_col
ancient-woodland,ancient-woodland-collection,11000000,11999999,x
brownfield-land,brownfield-land-collection,4000000,4999999,y
conservation-area,conservation-area-collection,44000000,44999999,z
"""


def _make_spec_df(spark):
    with patch("jobs.transform.old_entity_transformer.requests.get") as mock_get:
        mock_get.return_value.text = _SPEC_CSV
        mock_get.return_value.raise_for_status = lambda: None
        return fetch_dataset_df(spark)


def _make_old_entity_df(spark, rows):
    schema = StructType(
        [
            StructField("old_entity", LongType(), True),
            StructField("status", StringType(), True),
            StructField("entity", LongType(), True),
            StructField("notes", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("collection", StringType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def test_fetch_dataset_df_returns_expected_columns(spark):
    df = _make_spec_df(spark)
    assert df.columns == ["dataset", "collection", "entity_minimum", "entity_maximum"]


def test_fetch_dataset_df_strips_extra_columns(spark):
    df = _make_spec_df(spark)
    assert "other_col" not in df.columns


def test_fetch_dataset_df_normalises_column_names(spark):
    with patch("jobs.transform.old_entity_transformer.requests.get") as mock_get:
        mock_get.return_value.text = (
            "dataset,collection,entity-minimum,entity-maximum\nds,col,1,2\n"
        )
        mock_get.return_value.raise_for_status = lambda: None
        df = fetch_dataset_df(spark)
    assert "entity_minimum" in df.columns
    assert "entity_maximum" in df.columns


def test_fetch_dataset_df_row_count_matches_csv(spark):
    df = _make_spec_df(spark)
    assert df.count() == 3


def test_transform_old_entity_output_columns_match_schema(spark):
    spec_df = _make_spec_df(spark)
    old_entity_df = _make_old_entity_df(
        spark,
        [
            (
                11000001,
                "301",
                11000002,
                None,
                None,
                "2024-01-01",
                None,
                "ancient-woodland-collection",
            )
        ],
    )

    result = transform_old_entity(old_entity_df, spec_df)
    expected = [f.field for f in get_schema("old_entity").fields] + [
        "processed_timestamp"
    ]
    assert result.columns == expected


def test_transform_old_entity_dataset_derived_from_entity_range(spark):
    spec_df = _make_spec_df(spark)
    old_entity_df = _make_old_entity_df(
        spark,
        [
            (
                11000001,
                "301",
                11000002,
                None,
                None,
                "2024-01-01",
                None,
                "ancient-woodland-collection",
            )
        ],
    )

    result = transform_old_entity(old_entity_df, spec_df)
    assert result.collect()[0]["dataset"] == "ancient-woodland"


def test_transform_old_entity_filters_collection_mismatch(spark):
    """Records where the entity range's collection doesn't match the source collection are removed."""
    spec_df = _make_spec_df(spark)
    old_entity_df = _make_old_entity_df(
        spark,
        [
            # entity in ancient-woodland range but collection says brownfield — filtered
            (
                11000001,
                "301",
                11000002,
                None,
                None,
                "2024-01-01",
                None,
                "brownfield-land-collection",
            ),
            # correct match
            (
                4000001,
                "301",
                4000002,
                None,
                None,
                "2024-01-01",
                None,
                "brownfield-land-collection",
            ),
        ],
    )

    result = transform_old_entity(old_entity_df, spec_df)
    assert result.count() == 1
    assert result.collect()[0]["old_entity"] == 4000001


def test_transform_old_entity_drops_entity_outside_all_ranges(spark):
    """An entity number outside every range produces no output row."""
    spec_df = _make_spec_df(spark)
    old_entity_df = _make_old_entity_df(
        spark,
        [
            (
                99999999,
                "301",
                None,
                None,
                None,
                None,
                None,
                "ancient-woodland-collection",
            )
        ],
    )

    result = transform_old_entity(old_entity_df, spec_df)
    assert result.count() == 0


def test_transform_old_entity_preserves_collection_column(spark):
    spec_df = _make_spec_df(spark)
    old_entity_df = _make_old_entity_df(
        spark,
        [
            (
                11000001,
                "301",
                11000002,
                None,
                None,
                "2024-01-01",
                None,
                "ancient-woodland-collection",
            )
        ],
    )

    result = transform_old_entity(old_entity_df, spec_df)
    assert result.collect()[0]["collection"] == "ancient-woodland-collection"


def test_transform_old_entity_adds_processed_timestamp(spark):
    spec_df = _make_spec_df(spark)
    old_entity_df = _make_old_entity_df(
        spark,
        [
            (
                11000001,
                "301",
                11000002,
                None,
                None,
                "2024-01-01",
                None,
                "ancient-woodland-collection",
            )
        ],
    )

    result = transform_old_entity(old_entity_df, spec_df)
    assert "processed_timestamp" in result.columns
    assert result.collect()[0]["processed_timestamp"] is not None


def test_transform_old_entity_missing_schema_fields_added_as_null(spark):
    spec_df = _make_spec_df(spark)
    old_entity_df = _make_old_entity_df(
        spark,
        [
            (
                11000001,
                "301",
                11000002,
                None,
                None,
                "2024-01-01",
                None,
                "ancient-woodland-collection",
            )
        ],
    )

    result = transform_old_entity(old_entity_df, spec_df)
    row = result.collect()[0]
    assert row["notes"] is None
    assert row["end_date"] is None
