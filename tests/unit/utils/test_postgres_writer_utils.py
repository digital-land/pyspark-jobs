import pytest
from pg8000.exceptions import DatabaseError, InterfaceError

from jobs.utils.postgres_writer_utils import (
    ATOMIC_COMMIT_CONNECTION_TIMEOUT_SECONDS,
    ATOMIC_COMMIT_STATEMENT_TIMEOUT_SECONDS,
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
