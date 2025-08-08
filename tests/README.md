# Testing Guide for PySpark Jobs

This directory contains comprehensive tests for the PySpark Jobs project, organized by testing scope and purpose.

## Test Structure

```
tests/
├── unit/           # Unit tests - fast, isolated component tests
├── integration/    # Integration tests - database, file I/O, external services
├── acceptance/     # Acceptance tests - end-to-end workflow validation
├── conftest.py     # Shared test configuration and fixtures
└── README.md       # This file
```

## Test Categories

### Unit Tests (`tests/unit/`)
- **Purpose**: Test individual functions and classes in isolation
- **Characteristics**: Fast (<1s), no external dependencies, mocked I/O
- **Files**:
  - `fact-test.py` - Tests for fact data transformation logic
  - `fact-res.py` - Tests for fact resource transformation logic  
  - `pytest-transport-access-node.py` - Tests for transport data transformations

### Integration Tests (`tests/integration/`)
- **Purpose**: Test interactions with external systems and services
- **Characteristics**: Slower, real/mocked databases, file operations
- **Files**:
  - `postgres_connectivity_test.py` - PostgreSQL database operations
  - `sqlite_test.py` - SQLite database operations
  - `title_boundary_entity_parquet.py` - Parquet file reading/writing

### Acceptance Tests (`tests/acceptance/`)
- **Purpose**: Validate complete end-to-end workflows
- **Characteristics**: Comprehensive scenarios, full data pipelines
- **Files**:
  - `entity_test.py` - Complete entity processing workflow

## Running Tests

### Prerequisites

1. Install test dependencies:
```bash
pip install -r requirements-test.txt
```

2. Ensure PySpark is available:
```bash
pip install pyspark>=3.3.0
```

### Running All Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run in parallel
pytest -n auto
```

### Running by Category
```bash
# Unit tests only (fast)
pytest tests/unit/ -m unit

# Integration tests only
pytest tests/integration/ -m integration

# Acceptance tests only
pytest tests/acceptance/ -m acceptance
```

### Running Specific Test Files
```bash
# Run a specific test file
pytest tests/unit/fact-test.py

# Run a specific test function
pytest tests/unit/fact-test.py::test_transform_data_fact_columns_exist

# Run tests matching a pattern
pytest -k "fact" tests/
```

### Useful Test Options
```bash
# Verbose output
pytest -v

# Stop on first failure
pytest -x

# Show local variables on failure
pytest -l

# Run only failed tests from last run
pytest --lf

# Show test durations
pytest --durations=10

# Run tests in specific order
pytest --maxfail=1 tests/unit/ tests/integration/ tests/acceptance/
```

## Test Configuration

### Markers
Tests are automatically marked based on their directory:
- `@pytest.mark.unit` - Unit tests
- `@pytest.mark.integration` - Integration tests  
- `@pytest.mark.acceptance` - Acceptance tests
- `@pytest.mark.slow` - Tests taking >1 second
- `@pytest.mark.database` - Tests requiring database

### Fixtures
Common fixtures are available across all tests:

#### Shared Fixtures (`tests/conftest.py`)
- `spark` - Session-scoped Spark session
- `spark_local` - Function-scoped isolated Spark session
- `test_config` - Test configuration constants
- `assert_df_equal` - DataFrame equality assertion helper

#### Unit Test Fixtures (`tests/unit/conftest.py`)
- `unit_spark` - Minimal Spark session for unit tests
- `sample_fact_schema` - Schema for fact data testing
- `transformation_validator` - Validation helper functions

#### Integration Test Fixtures (`tests/integration/conftest.py`)
- `integration_spark` - Spark session for integration tests
- `temp_sqlite_db` - Temporary SQLite database
- `mock_postgres_connection` - Mocked PostgreSQL connection
- `sample_parquet_file` - Sample parquet file for testing

#### Acceptance Test Fixtures (`tests/acceptance/conftest.py`)
- `acceptance_spark` - Production-like Spark session
- `end_to_end_test_data` - Comprehensive test data setup
- `mock_external_dependencies` - Mocked external services

## Writing New Tests

### Unit Test Example
```python
def test_my_transformation(spark, sample_fact_data):
    """Test that my transformation works correctly."""
    result = my_transformation_function(sample_fact_data)
    
    assert result.count() == expected_count
    assert result.columns == expected_columns
    assert result.collect()[0].field_name == expected_value
```

### Integration Test Example
```python
@pytest.mark.integration
def test_database_operation(mock_postgres_connection):
    """Test database connectivity and operations."""
    mock_conn, mock_cursor = mock_postgres_connection
    
    # Test database operation
    result = perform_database_operation(mock_conn)
    
    # Verify interactions
    mock_cursor.execute.assert_called_once()
    assert result is not None
```

### Acceptance Test Example
```python
@pytest.mark.acceptance
def test_end_to_end_pipeline(acceptance_spark, end_to_end_test_data):
    """Test complete data processing pipeline."""
    # Load data
    input_df = acceptance_spark.read.csv(end_to_end_test_data["input_path"])
    
    # Process through pipeline
    result_df = complete_pipeline_function(input_df)
    
    # Validate output
    assert result_df.count() > 0
    assert "final_column" in result_df.columns
```

## Test Best Practices

### General Guidelines
1. **Test Names**: Use descriptive names that explain what is being tested
2. **Assertions**: Use specific assertions with helpful error messages
3. **Test Data**: Use minimal data sets that cover edge cases
4. **Isolation**: Each test should be independent and not affect others
5. **Documentation**: Add docstrings explaining complex test scenarios

### PySpark Specific
1. **Spark Sessions**: Use appropriate session scope (session/module/function)
2. **DataFrame Comparison**: Use `assert_df_equal` fixture for DataFrame comparison
3. **Schema Validation**: Always validate DataFrame schemas in tests
4. **Resource Cleanup**: Ensure proper cleanup of Spark sessions and temp files
5. **Performance**: Keep unit tests fast (<1s), mark slow tests appropriately

### Mock Usage
1. **External Systems**: Mock database connections, API calls, file systems
2. **Environment Variables**: Use `monkeypatch` to set test environment
3. **Time**: Use `freezegun` for time-dependent tests
4. **Random Data**: Use `Faker` for generating realistic test data

## Troubleshooting

### Common Issues

1. **Spark Session Conflicts**
   ```python
   # Solution: Use proper session scoping in fixtures
   @pytest.fixture(scope="module")
   def spark():
       # Create session
       yield session
       session.stop()  # Always stop sessions
   ```

2. **Import Errors**
   ```bash
   # Solution: Ensure src is in Python path
   export PYTHONPATH="${PYTHONPATH}:./src"
   # Or run from project root
   ```

3. **Slow Tests**
   ```bash
   # Solution: Run only fast tests during development
   pytest -m "not slow"
   ```

4. **Memory Issues**
   ```python
   # Solution: Use smaller datasets and proper cleanup
   df.unpersist()  # Release cached DataFrames
   ```

### Debug Techniques
1. Use `pytest -s` to see print statements
2. Use `pytest --pdb` to drop into debugger on failure
3. Use `pytest -vv` for extra verbose output
4. Check Spark UI at `http://localhost:4040` during test execution

## Continuous Integration

Tests are designed to run in CI environments with:
- Automated test discovery and execution
- Coverage reporting
- Parallel execution support
- Proper artifact cleanup

For CI configuration, ensure:
1. Java 11+ is available for Spark
2. Required system dependencies are installed
3. Test databases are available (or properly mocked)
4. Sufficient memory allocation for Spark tests
