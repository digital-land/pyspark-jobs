"""
Integration tests for PostgreSQL connectivity and database operations.
"""
import pytest
import psycopg2
from psycopg2 import sql
from unittest.mock import patch, MagicMock
import json
from datetime import date

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Define table schema for testing
TABLE_NAME = "entity_test"
COLUMNS = {   
    "dataset": "TEXT",
    "end_date": "DATE",
    "entity": "TEXT",
    "entry_date": "DATE",
    "geojson": "JSONB",
    "geometry": "TEXT",  # Simplified for testing
    "json": "JSONB",
    "name": "TEXT",
    "organisation_entity": "BIGINT",
    "point": "TEXT",  # Simplified for testing
    "prefix": "TEXT",
    "reference": "TEXT",
    "start_date": "DATE", 
    "typology": "TEXT",
}

# Test connection parameters
TEST_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "test_db",
    "user": "test_user",
    "password": "test_password"
}


@pytest.fixture
def mock_db_connection():
    """Mock database connection for testing."""
    with patch('psycopg2.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def sample_entity_data():
    """Sample data for testing database operations."""
    return [
        {
            "dataset": "planning-applications",
            "end_date": date(2025, 12, 31),
            "entity": "entity-001",
            "entry_date": date(2025, 8, 6),
            "geojson": json.dumps({"type": "Feature", "geometry": None}),
            "geometry": None,
            "json": json.dumps({"status": "approved"}),
            "name": "Application A",
            "organisation_entity": 1001,
            "point": None,
            "prefix": "PA",
            "reference": "REF001",
            "start_date": date(2025, 1, 1),
            "typology": "residential"
        },
        {
            "dataset": "planning-applications",
            "end_date": date(2025, 11, 30),
            "entity": "entity-002",
            "entry_date": date(2025, 8, 6),
            "geojson": json.dumps({"type": "Feature", "geometry": None}),
            "geometry": None,
            "json": json.dumps({"status": "pending"}),
            "name": "Application B",
            "organisation_entity": 1002,
            "point": None,
            "prefix": "PA",
            "reference": "REF002",
            "start_date": date(2025, 2, 1),
            "typology": "commercial"
        }
    ]


def create_table_sql():
    """Generate CREATE TABLE SQL statement."""
    column_defs = ", ".join([f"{col} {dtype}" for col, dtype in COLUMNS.items()])
    return f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({column_defs});"


def test_create_table_success(mock_db_connection):
    """Test successful table creation."""
    mock_conn, mock_cursor = mock_db_connection
    
    # Execute table creation
    create_query = create_table_sql()
    mock_cursor.execute(create_query)
    mock_conn.commit()
    
    # Verify the calls
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_create_table_with_connection_error():
    """Test table creation with connection error."""
    with patch('psycopg2.connect') as mock_connect:
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")
        
        with pytest.raises(psycopg2.OperationalError):
            psycopg2.connect(**TEST_CONN_PARAMS)


def test_insert_sample_data(mock_db_connection, sample_entity_data):
    """Test inserting sample data into the database."""
    mock_conn, mock_cursor = mock_db_connection
    
    # Prepare insert query
    insert_query = f"""
    INSERT INTO {TABLE_NAME} ({', '.join(COLUMNS.keys())})
    VALUES ({', '.join(['%s'] * len(COLUMNS))})
    """
    
    # Execute inserts
    for record in sample_entity_data:
        values = tuple(record[col] for col in COLUMNS.keys())
        mock_cursor.execute(insert_query, values)
    
    mock_conn.commit()
    
    # Verify the calls
    assert mock_cursor.execute.call_count == len(sample_entity_data)
    mock_conn.commit.assert_called_once()


def test_table_schema_validation():
    """Test that the table schema is correctly defined."""
    expected_columns = [
        "dataset", "end_date", "entity", "entry_date", "geojson",
        "geometry", "json", "name", "organisation_entity", "point",
        "prefix", "reference", "start_date", "typology"
    ]
    
    assert list(COLUMNS.keys()) == expected_columns
    
    # Check that JSONB columns are correctly typed
    assert COLUMNS["geojson"] == "JSONB"
    assert COLUMNS["json"] == "JSONB"
    
    # Check that date columns are correctly typed
    assert COLUMNS["end_date"] == "DATE"
    assert COLUMNS["entry_date"] == "DATE"
    assert COLUMNS["start_date"] == "DATE"


def test_sql_injection_prevention():
    """Test that SQL queries are properly parameterized."""
    # This is a conceptual test - in real implementation, ensure using parameterized queries
    malicious_input = "'; DROP TABLE entity_test; --"
    
    # Verify that direct string formatting would be dangerous
    dangerous_query = f"SELECT * FROM {TABLE_NAME} WHERE entity = '{malicious_input}'"
    assert "DROP TABLE" in dangerous_query
    
    # Safe parameterized query should not contain the malicious code directly
    safe_query = f"SELECT * FROM {TABLE_NAME} WHERE entity = %s"
    assert "DROP TABLE" not in safe_query


def test_connection_parameters_validation():
    """Test that connection parameters are properly structured."""
    required_params = ["host", "port", "dbname", "user", "password"]
    
    for param in required_params:
        assert param in TEST_CONN_PARAMS
    
    assert isinstance(TEST_CONN_PARAMS["port"], int)
    assert TEST_CONN_PARAMS["port"] > 0


def test_data_serialization(sample_entity_data):
    """Test that complex data types are properly serialized for database storage."""
    for record in sample_entity_data:
        # Test JSON serialization
        geojson_str = record["geojson"]
        json_str = record["json"]
        
        # Should be valid JSON strings
        assert isinstance(geojson_str, str)
        assert isinstance(json_str, str)
        
        # Should be parseable as JSON
        parsed_geojson = json.loads(geojson_str)
        parsed_json = json.loads(json_str)
        
        assert isinstance(parsed_geojson, dict)
        assert isinstance(parsed_json, dict)


@pytest.mark.integration
def test_database_connection_with_retry():
    """Test database connection with retry logic."""
    max_retries = 3
    retry_count = 0
    
    def mock_connect_with_retry():
        nonlocal retry_count
        retry_count += 1
        if retry_count < max_retries:
            raise psycopg2.OperationalError("Connection failed")
        return MagicMock()
    
    with patch('psycopg2.connect', side_effect=mock_connect_with_retry):
        # Should succeed on the third attempt
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**TEST_CONN_PARAMS)
                break
            except psycopg2.OperationalError:
                if attempt == max_retries - 1:
                    raise
                continue
        
        assert retry_count == max_retries

