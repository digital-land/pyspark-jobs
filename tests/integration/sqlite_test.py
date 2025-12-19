"""
Integration tests for SQLite database operations.
"""

import os
import sqlite3
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


@pytest.fixture
def temp_sqlite_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_file:
        db_path = tmp_file.name

    yield db_path

    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def sqlite_connection(temp_sqlite_db):
    """Create a SQLite connection for testing."""
    conn = sqlite3.connect(temp_sqlite_db)
    yield conn
    conn.close()


@pytest.fixture
def sample_table_setup(sqlite_connection):
    """Set up sample table with test data."""
    cursor = sqlite_connection.cursor()

    # Create test table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_resource (
            id INTEGER PRIMARY KEY,
            end_date TEXT,
            fact TEXT,
            entry_date TEXT,
            entry_number TEXT,
            priority TEXT,
            resource TEXT,
            start_date TEXT
        )
    """
    )

    # Insert sample data
    test_data = [
        (1, "2025-12-31", "fact1", "2025-01-01", "1", "1", "resource1", "2025-01-01"),
        (2, "2025-12-31", "fact2", "2025-01-02", "2", "2", "resource2", "2025-01-01"),
        (3, "2025-12-31", "fact3", "2025-01-03", "3", "1", "resource3", "2025-01-01"),
    ]

    cursor.executemany(
        """
        INSERT INTO fact_resource (id, end_date, fact, entry_date, entry_number, priority, resource, start_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        test_data,
    )

    sqlite_connection.commit()
    return cursor


def test_sqlite_connection_success(temp_sqlite_db):
    """Test successful SQLite database connection."""
    conn = sqlite3.connect(temp_sqlite_db)
    assert conn is not None

    # Test basic operation
    cursor = conn.cursor()
    cursor.execute("SELECT sqlite_version();")
    version = cursor.fetchone()
    assert version is not None
    assert len(version) > 0

    conn.close()


def test_sqlite_connection_to_nonexistent_file():
    """Test connection to non-existent database file."""
    non_existent_path = "/path/that/does/not/exist/test.db"

    # SQLite will create the file, so this should not raise an error
    # But the directory doesn't exist, so it should fail
    with pytest.raises(sqlite3.OperationalError):
        conn = sqlite3.connect(non_existent_path)
        conn.execute("CREATE TABLE test (id INTEGER);")


def test_list_tables(sample_table_setup):
    """Test listing tables in SQLite database."""
    cursor = sample_table_setup

    # Query to list all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_names = [table[0] for table in tables]
    assert "fact_resource" in table_names


def test_query_table_contents(sample_table_setup):
    """Test querying table contents."""
    cursor = sample_table_setup

    # Query all records
    cursor.execute("SELECT * FROM fact_resource;")
    rows = cursor.fetchall()

    assert len(rows) == 3

    # Check first row
    first_row = rows[0]
    assert first_row[0] == 1  # id
    assert first_row[1] == "2025-12-31"  # end_date
    assert first_row[2] == "fact1"  # fact


def test_query_with_filter(sample_table_setup):
    """Test querying with WHERE clause."""
    cursor = sample_table_setup

    # Query with filter
    cursor.execute("SELECT * FROM fact_resource WHERE priority = ?;", ("1",))
    rows = cursor.fetchall()

    # Should return 2 rows with priority '1'
    assert len(rows) == 2
    for row in rows:
        assert row[5] == "1"  # priority column


def test_table_schema_validation(sample_table_setup):
    """Test that the table schema is correct."""
    cursor = sample_table_setup

    # Get table info
    cursor.execute("PRAGMA table_info(fact_resource);")
    columns = cursor.fetchall()

    expected_columns = [
        "id",
        "end_date",
        "fact",
        "entry_date",
        "entry_number",
        "priority",
        "resource",
        "start_date",
    ]

    actual_columns = [col[1] for col in columns]  # col[1] is column name
    assert actual_columns == expected_columns


def test_insert_new_record(sample_table_setup):
    """Test inserting a new record."""
    cursor = sample_table_setup

    # Insert new record
    new_record = (
        4,
        "2025-12-31",
        "fact4",
        "2025-01-04",
        "4",
        "3",
        "resource4",
        "2025-01-01",
    )
    cursor.execute(
        """
        INSERT INTO fact_resource (id, end_date, fact, entry_date, entry_number, priority, resource, start_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        new_record,
    )

    # Verify insertion
    cursor.execute("SELECT COUNT(*) FROM fact_resource;")
    count = cursor.fetchone()[0]
    assert count == 4


def test_update_record(sample_table_setup):
    """Test updating an existing record."""
    cursor = sample_table_setup

    # Update a record
    cursor.execute("UPDATE fact_resource SET priority = ? WHERE id = ?;", ("5", 1))

    # Verify update
    cursor.execute("SELECT priority FROM fact_resource WHERE id = ?;", (1,))
    priority = cursor.fetchone()[0]
    assert priority == "5"


def test_delete_record(sample_table_setup):
    """Test deleting a record."""
    cursor = sample_table_setup

    # Delete a record
    cursor.execute("DELETE FROM fact_resource WHERE id = ?;", (1,))

    # Verify deletion
    cursor.execute("SELECT COUNT(*) FROM fact_resource;")
    count = cursor.fetchone()[0]
    assert count == 2


def test_transaction_rollback(sqlite_connection):
    """Test transaction rollback functionality."""
    cursor = sqlite_connection.cursor()

    # Create table
    cursor.execute(
        """
        CREATE TABLE test_transaction (
            id INTEGER PRIMARY KEY,
            value TEXT
        )
    """
    )

    # Insert initial data
    cursor.execute("INSERT INTO test_transaction (value) VALUES (?);", ("initial",))
    sqlite_connection.commit()

    try:
        # Start transaction
        cursor.execute("INSERT INTO test_transaction (value) VALUES (?);", ("test1",))
        cursor.execute("INSERT INTO test_transaction (value) VALUES (?);", ("test2",))

        # Simulate error
        raise Exception("Simulated error")

    except Exception:
        sqlite_connection.rollback()

    # Verify rollback
    cursor.execute("SELECT COUNT(*) FROM test_transaction;")
    count = cursor.fetchone()[0]
    assert count == 1  # Only initial record should remain


@pytest.mark.integration
def test_sqlite_file_permissions(temp_sqlite_db):
    """Test SQLite file permissions and access."""
    # Check that file exists and is readable
    assert os.path.exists(temp_sqlite_db)
    assert os.access(temp_sqlite_db, os.R_OK)
    assert os.access(temp_sqlite_db, os.W_OK)
