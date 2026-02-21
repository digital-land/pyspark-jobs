# Testing Guide

## Test Structure

```
tests/
├── conftest.py                              # Shared fixtures (Spark session, database)
├── unit/                                    # Fast, isolated tests (mocked dependencies)
│   ├── test_pipeline.py                     # Pipeline unit tests
│   └── utils/
│       ├── test_aws_secrets_manager.py      # AWS Secrets Manager
│       ├── test_db_url.py                   # Database URL parsing/building
│       ├── test_df_utils.py                 # DataFrame utilities
│       ├── test_logger_config.py            # Logger configuration
│       └── test_s3_dataset_typology.py      # Dataset typology mapping
├── integration/                             # Tests with real Spark and/or database
│   ├── test_logging_integration.py          # Logging integration
│   ├── test_logging_standalone.py           # Standalone logging diagnostics
│   ├── test_pipeline.py                     # Pipeline with real Spark session
│   ├── transform/
│   │   └── test_entity_transformer.py       # Entity transformer with Spark
│   └── utils/
│       ├── test_geometry_utils.py           # Geometry utilities with Spark
│       └── test_postgres_writer_utils.py    # Postgres writer with real database
└── acceptance/
    └── test_run_main_acceptance.py          # CLI entry point tests
```

## Running Tests

```bash
make test                # All tests with coverage
make test-unit           # Unit tests only
make test-integration    # Integration tests
make test-acceptance     # Acceptance tests
make test-quick          # Unit tests, no coverage
make test-parallel       # All tests in parallel (requires pytest-xdist)
```

Or target a specific file:

```bash
pytest tests/unit/utils/test_db_url.py -v
pytest tests/integration/utils/test_postgres_writer_utils.py -v
```

## Shared Fixtures

Defined in `tests/conftest.py`:

| Fixture | Scope | Description |
|---------|-------|-------------|
| `spark` | session | Sedona-enabled Spark session with PostgreSQL JDBC driver |
| `db_url` | session | PostGIS database URL (testcontainers locally, `DATABASE_URL` env var in CI) |
| `db_conn` | session | pg8000 connection to the test database |
| `clean_entity_table` | function | Creates the entity table before each test, truncates after |

## Database Integration Tests

Tests marked with `@pytest.mark.database` require a real PostgreSQL/PostGIS database.

**Locally:** Docker must be running. Testcontainers starts a `postgis/postgis:14-master` container automatically.

**In CI:** The GitHub Actions workflow provides a PostgreSQL service container and sets the `DATABASE_URL` environment variable.

## Markers

Defined in `pytest.ini`:

| Marker | Purpose |
|--------|---------|
| `unit` | Fast, isolated tests |
| `integration` | Tests with external dependencies |
| `acceptance` | End-to-end workflow tests |
| `slow` | Tests taking >1 second |
| `database` | Tests requiring database connectivity |
| `spark` | Tests requiring a Spark session |

## Conventions

- Test files mirror the source directory structure (e.g. `src/jobs/utils/db_url.py` -> `tests/unit/utils/test_db_url.py`)
- Tests are plain functions, not classes
- Use descriptive names: `test_parse_database_url_special_characters_in_password`
- Use existing fixtures from `conftest.py` rather than creating per-file setup

## Troubleshooting

**Module not found errors:**
```bash
# Ensure venv is active and package is installed in dev mode
source .venv/bin/activate
pip install -e .
```

**Database tests failing locally:**
```bash
# Ensure Docker is running
docker info
```

**Spark session errors:**
```bash
# Check Java is installed
java -version
```
