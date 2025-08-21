import pg8000
import json
import pytest
from datetime import date

# Define table schema
TABLE_NAME = "entity_test"
COLUMNS = {
    "dataset": "TEXT",
    "end_date": "DATE",
    "entity": "TEXT",
    "entry_date": "DATE",
    "geojson": "JSONB",
    "geometry": "TEXT",
    "json": "JSONB",
    "name": "TEXT",
    "organisation_entity": "BIGINT",
    "point": "TEXT",
    "prefix": "TEXT",
    "reference": "TEXT",
    "start_date": "DATE",
    "typology": "TEXT",
}

# Connection parameters
TEST_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "postgres"
}

# Sample data
sample_entity_data = [
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
    column_defs = ", ".join([f"{col} {dtype}" for col, dtype in COLUMNS.items()])
    return f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({column_defs});"

def insert_sql():
    columns = ", ".join(COLUMNS.keys())
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    return f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders});"

@pytest.mark.integration
def test_create_table():
    conn = pg8000.connect(**TEST_CONN_PARAMS)
    cursor = conn.cursor()
    cursor.execute(create_table_sql())
    conn.commit()
    cursor.close()
    conn.close()

@pytest.mark.integration
def test_insert_sample_data():
    conn = pg8000.connect(**TEST_CONN_PARAMS)
    cursor = conn.cursor()
    insert_query = insert_sql()
    for record in sample_entity_data:
        values = tuple(record[col] for col in COLUMNS.keys())
        cursor.execute(insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

@pytest.mark.integration
def test_verify_table_exists():
    conn = pg8000.connect(**TEST_CONN_PARAMS)
    cursor = conn.cursor()
    cursor.execute(f"SELECT to_regclass('{TABLE_NAME}');")
    result = cursor.fetchone()
    assert result[0] == TABLE_NAME
    cursor.close()
    conn.close()

@pytest.mark.integration
def test_verify_data_inserted():
    conn = pg8000.connect(**TEST_CONN_PARAMS)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
    count = cursor.fetchone()[0]
    assert count == len(sample_entity_data)
    cursor.close()
    conn.close()
