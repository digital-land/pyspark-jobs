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

1. **Initialize the development environment** (first time setup):
```bash
# From project root directory
make init
```

This will:
- Create a virtual environment (`pyspark-jobs-venv`)
- Install all dependencies from `requirements-local.txt`
- Set up pre-commit hooks
- Install the package in development mode

2. **Activate the virtual environment**:
```bash
source pyspark-jobs-venv/bin/activate
```

### Running Tests with Make Commands (Recommended)

The Makefile provides convenient commands that automatically activate the virtual environment:

```bash
# Run all tests with coverage
make test

# Run unit tests only (fast)
make test-unit

# Run integration tests
make test-integration

# Run acceptance tests
make test-acceptance

# Run tests with HTML coverage report
make test-coverage

# Quick run (unit tests only, no coverage)
make test-quick

# Run tests in parallel
make test-parallel
```

### Running Tests Directly with pytest

If you prefer to use pytest directly (ensure virtual environment is activated):

```bash
# Run all tests
python tests/run_tests.py

# Run with coverage
python tests/run_tests.py --coverage

# Run in parallel
python tests/run_tests.py --parallel
```

### Running by Category
```bash
# Using Make commands
make test-unit           # Unit tests only
make test-integration    # Integration tests only
make test-acceptance     # Acceptance tests only

# Using run_tests.py
python tests/run_tests.py --unit
python tests/run_tests.py --integration
python tests/run_tests.py --acceptance

# Using pytest directly
pytest tests/unit/ -m unit
pytest tests/integration/ -m integration
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
# Using run_tests.py
python tests/run_tests.py --verbose          # Verbose output
python tests/run_tests.py --fail-fast        # Stop on first failure
python tests/run_tests.py --html-report      # Generate HTML coverage report
python tests/run_tests.py --quick            # Quick run (unit tests, no coverage)

# Using pytest directly (with venv activated)
pytest -v                    # Verbose output
pytest -x                    # Stop on first failure
pytest -l                    # Show local variables on failure
pytest --lf                  # Run only failed tests from last run
pytest --durations=10        # Show test durations
pytest -n auto               # Run in parallel
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

1. **ModuleNotFoundError (boto3, botocore, pyspark)**
   ```bash
   # Solution: Ensure virtual environment is activated and dependencies are installed
   source pyspark-jobs-venv/bin/activate
   pip install -r requirements-local.txt
   
   # Or reinitialize the environment
   make init
   ```

2. **Virtual environment not found error**
   ```bash
   # Solution: Initialize the development environment
   make init
   
   # This creates pyspark-jobs-venv and installs all dependencies
   ```

3. **Tests not found or wrong directory**
   ```bash
   # Solution: Always run tests from project root directory
   cd /path/to/pyspark-jobs
   make test-unit
   ```

4. **Spark Session Conflicts**
   ```python
   # Solution: Use proper session scoping in fixtures
   @pytest.fixture(scope="module")
   def spark():
       # Create session
       yield session
       session.stop()  # Always stop sessions
   ```

5. **Import Errors**
   ```bash
   # Solution: Ensure package is installed in development mode
   pip install -e .
   # Or run from project root with proper PYTHONPATH
   export PYTHONPATH="${PYTHONPATH}:./src"
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