import pytest
from pg8000.exceptions import DatabaseError, InterfaceError

from jobs.utils.postgres_writer_utils import (
    ATOMIC_COMMIT_CONNECTION_TIMEOUT_SECONDS,
    ATOMIC_COMMIT_STATEMENT_TIMEOUT_SECONDS,
    _invalid_geometry_entities,
    _is_statement_timeout,
)


def test_statement_timeout_is_recognised():
    """The shape of the error from the October 2026 title-boundary run"""
    error = DatabaseError(
        {
            "S": "ERROR",
            "V": "ERROR",
            "C": "57014",
            "M": "canceling statement due to statement timeout",
        }
    )
    assert _is_statement_timeout(error)


@pytest.mark.parametrize(
    "error",
    [
        DatabaseError({"S": "ERROR", "C": "40P01", "M": "deadlock detected"}),
        InterfaceError("network error"),
        DatabaseError(),
        Exception("something else"),
    ],
)
def test_other_errors_are_not_statement_timeouts(error):
    assert not _is_statement_timeout(error)


def test_connection_timeout_outlasts_the_statement_timeout():
    """Otherwise the client gives up while the statement is still running on the server"""
    assert (
        ATOMIC_COMMIT_CONNECTION_TIMEOUT_SECONDS
        > ATOMIC_COMMIT_STATEMENT_TIMEOUT_SECONDS
    )


def test_invalid_geometry_entities_finds_only_invalid_geometry(spark):
    """Sedona flags the same geometries PostGIS treats as invalid, and skips missing geometry."""
    df = spark.createDataFrame(
        [
            (1, "MULTIPOLYGON(((0 0, 10 10, 10 0, 0 10, 0 0)))"),  # self-intersecting
            (
                2,
                "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0, -5 0, 0 0)))",
            ),  # ring with a spike
            (3, "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))"),  # valid
            (4, None),
            (5, ""),
        ],
        "entity long, geometry string",
    )

    assert sorted(_invalid_geometry_entities(df)) == [1, 2]
