# Local Testing Guide for PySpark Jobs

This guide provides comprehensive instructions for setting up and running tests locally for the PySpark collection data jobs.

## Quick Start

1. **Setup Environment**
   ```bash
   make init-local
   ```

2. **Run Smoke Tests**
   ```bash
   ./tests/utils/test_runner --smoke
   ```

3. **Run All Tests**
   ```bash
   ./tests/utils/test_runner --all --coverage
   ```

## Table of Contents

- [Environment Setup](#environment-setup)
- [Test Categories](#test-categories)
- [Running Tests](#running-tests)
- [Test Data and Fixtures](#test-data-and-fixtures)
- [Mocking Strategy](#mocking-strategy)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Environment Setup

### Prerequisites

- Python 3.8+
- Java 11+ (for PySpark)
- Git

### Local Setup

1. **Clone and Setup Virtual Environment**
   ```bash
   cd pyspark-jobs
   make init-local  # Creates virtual environment and installs dependencies
   ```

2. **Activate Virtual Environment**
   ```bash
   source pyspark-jobs-venv/bin/activate
   ```

3. **Verify Setup**
   ```bash
   ./tests/utils/test_runner --check-deps
   ```

### Manual Setup (Alternative)

If the Makefile approach doesn't work:

```bash
# Create virtual environment
python3 -m venv pyspark-jobs-venv
source pyspark-jobs-venv/bin/activate

# Install dependencies
pip install --upgrade pip setuptools wheel
pip install -r requirements-local.txt
pip install -e .

# Verify PySpark
python -c "from pyspark.sql import SparkSession; print('PySpark installed successfully')"
```

## Test Categories

### Unit Tests (`tests/unit/`)
- **Purpose**: Test individual functions and classes in isolation
- **Characteristics**: Fast, no external dependencies, heavy mocking
- **Location**: `tests/unit/`
- **Run Command**: `./tests/utils/test_runner --unit`

### Integration Tests (`tests/integration/`)
- **Purpose**: Test component interactions with mocked external services
- **Characteristics**: Medium speed, mocked AWS services, real Spark operations
- **Location**: `tests/integration/`
- **Run Command**: `./tests/utils/test_runner --integration`

### Acceptance Tests (`tests/acceptance/`)
- **Purpose**: End-to-end testing of complete workflows
- **Characteristics**: Slower, comprehensive scenarios
- **Location**: `tests/acceptance/`
- **Run Command**: `./tests/utils/test_runner --acceptance`

## Running Tests

### Using the Test Runner (Recommended)

The `./tests/utils/test_runner` script provides a convenient interface for running tests:

```bash
# Quick validation (fastest)
./tests/utils/test_runner --smoke

# Run specific test types
./tests/utils/test_runner --unit
./tests/utils/test_runner --integration
./tests/utils/test_runner --acceptance

# Run all tests
./tests/utils/test_runner --all

# Run with coverage
./tests/utils/test_runner --unit --coverage

# Run specific test
./tests/utils/test_runner --specific test_main_collection_data

# Run specific integration test
./tests/utils/test_runner --specific test_main_collection_data_integration --test-type integration

# Code quality checks
./tests/utils/test_runner --lint
./tests/utils/test_runner --format

# Clean up artifacts
./tests/utils/test_runner --clean
```

### Using pytest Directly

```bash
# Activate virtual environment first
source pyspark-jobs-venv/bin/activate

# Run all tests
pytest tests/ -v

# Run specific test categories
pytest tests/unit/ -m unit -v
pytest tests/integration/ -m integration -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

# Run specific test file
pytest tests/unit/test_main_collection_data.py -v

# Run specific test method
pytest tests/unit/test_main_collection_data.py::TestCreateSparkSession::test_create_spark_session_success -v
```

### Using Make Commands

```bash
# Run all tests
make test

# Run specific test types
make test-unit
make test-integration
make test-acceptance

# Run with coverage
make test-coverage

# Code quality
make lint
make format
```

## Test Data and Fixtures

### Sample Data Generation

The testing framework includes comprehensive sample data generators:

```python
# In your tests
def test_my_function(sample_transport_dataframe, sample_fact_data):
    # sample_transport_dataframe: PySpark DataFrame with realistic transport data
    # sample_fact_data: List of dictionaries with fact table data
    result = my_function(sample_transport_dataframe)
    assert result is not None
```

### Available Fixtures

- **Spark Sessions**: `spark`, `spark_local`, `unit_spark`
- **Sample Data**: `sample_fact_data`, `sample_entity_data`, `sample_transport_data`
- **DataFrames**: `sample_fact_dataframe`, `sample_entity_dataframe`, `sample_transport_dataframe`
- **Mock Services**: `mock_s3`, `mock_secrets_manager`, `mock_postgres_connection`
- **Configuration**: `mock_configuration_files`, `mock_environment_variables`

### Creating Custom Test Data

```python
from tests.fixtures.sample_data import SampleDataGenerator

# Generate custom data
generator = SampleDataGenerator()
custom_data = generator.generate_fact_data(num_records=50)

# Create DataFrame
schema = generator.get_fact_schema()
df = spark.createDataFrame(custom_data, schema)
```

## Mocking Strategy

### AWS Services

All AWS services are mocked using `moto`:

```python
def test_with_s3(mock_s3):
    # S3 operations are automatically mocked
    # Buckets: development-collection-data, development-target-data
    mock_s3.list_objects_v2(Bucket='development-collection-data')
```

### Database Connections

PostgreSQL connections are mocked:

```python
def test_with_postgres(mock_postgres_connection):
    # Database operations are mocked
    conn = mock_postgres_connection['connection']
    cursor = mock_postgres_connection['cursor']
```

### Spark Operations

For unit tests, use minimal Spark mocking:

```python
def test_with_mock_spark(mock_spark_session_minimal):
    # Lightweight mock for unit tests
    result = function_under_test(mock_spark_session_minimal)
```

For integration tests, use real Spark with mocked I/O:

```python
def test_with_real_spark(spark, mock_spark_write_operations):
    # Real Spark DataFrame operations, mocked writes
    df = spark.createDataFrame([{"col": "value"}])
    result = transform_function(df)
```

## Best Practices

### Test Organization

1. **One test class per module**: `TestMainCollectionData` for `main_collection_data.py`
2. **Descriptive test names**: `test_transform_data_fact_with_valid_input`
3. **Group related tests**: Use classes to group tests by functionality
4. **Use markers**: Mark tests with `@pytest.mark.unit`, `@pytest.mark.integration`, etc.

### Test Structure

```python
class TestMyFunction:
    """Test suite for my_function."""
    
    def test_my_function_success_case(self, spark, sample_data):
        """Test my_function with valid input."""
        # Arrange
        input_df = spark.createDataFrame(sample_data)
        
        # Act
        result = my_function(input_df)
        
        # Assert
        assert result is not None
        assert result.count() > 0
    
    def test_my_function_error_case(self):
        """Test my_function with invalid input."""
        with pytest.raises(ValueError, match="Invalid input"):
            my_function(None)
```

### Performance

1. **Use session-scoped fixtures** for expensive setup (Spark sessions)
2. **Use module-scoped fixtures** for test data generation
3. **Use function-scoped fixtures** for test-specific mocks
4. **Run smoke tests** for quick validation during development

### Assertions

```python
# DataFrame assertions
assert_dataframe_equal(actual_df, expected_df)

# Row count assertions
assert actual_df.count() == expected_count

# Schema assertions
assert actual_df.columns == expected_columns
assert actual_df.schema == expected_schema

# Content assertions
rows = actual_df.collect()
assert len(rows) > 0
assert rows[0]['column_name'] == expected_value
```

## Troubleshooting

### Common Issues

#### 1. Spark Session Creation Fails

```bash
# Error: Java not found or wrong version
Error: JAVA_HOME is not set and java command could not be found

# Solution: Install and set JAVA_HOME
brew install openjdk@11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

#### 2. Import Errors

```bash
# Error: Module not found
ModuleNotFoundError: No module named 'jobs'

# Solution: Ensure PYTHONPATH includes src/
export PYTHONPATH="${PWD}/src:${PYTHONPATH}"
# Or use the test runner which sets this automatically
```

#### 3. Mock Not Working

```python
# Issue: Mock not being applied
# Solution: Use correct import path in patch
with patch('jobs.main_collection_data.boto3.client') as mock_client:
    # Not: patch('boto3.client')
```

#### 4. Test Database Issues

```bash
# Error: Connection refused to PostgreSQL
# Solution: Tests use mocked connections by default
# Ensure you're using the mock_postgres_connection fixture
```

#### 5. Memory Issues with Large DataFrames

```python
# Issue: OutOfMemoryError in tests
# Solution: Use smaller test datasets
def test_with_small_data(spark):
    small_data = sample_data[:10]  # Limit to 10 rows
    df = spark.createDataFrame(small_data)
```

### Debug Mode

Run tests in debug mode:

```bash
# Verbose output
python test_runner.py --unit --verbose

# Drop into debugger on failure
pytest tests/unit/test_main_collection_data.py --pdb

# Run single test with maximum verbosity
pytest tests/unit/test_main_collection_data.py::TestCreateSparkSession::test_create_spark_session_success -v -s
```

### Log Analysis

Enable detailed logging in tests:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

def test_with_logging(caplog):
    with caplog.at_level(logging.INFO):
        result = function_under_test()
        assert "Expected log message" in caplog.text
```

## CI/CD Integration

The tests are designed to run in CI/CD environments:

```yaml
# GitHub Actions example
- name: Run Tests
  run: |
    python test_runner.py --all --coverage
    
- name: Upload Coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage.xml
```

## Performance Benchmarks

Expected test execution times:

- **Smoke tests**: < 30 seconds
- **Unit tests**: < 2 minutes
- **Integration tests**: < 5 minutes
- **All tests**: < 10 minutes

## Contributing

When adding new tests:

1. **Follow naming conventions**: `test_function_name_scenario`
2. **Add appropriate markers**: `@pytest.mark.unit` or `@pytest.mark.integration`
3. **Include docstrings**: Describe what the test validates
4. **Update fixtures**: If new test data patterns are needed
5. **Run full test suite**: Ensure no regressions

## Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html#testing)
- [moto AWS Mocking](https://docs.getmoto.org/)
- [Project Makefile](../Makefile) - Available commands
- [Test Configuration](../pytest.ini) - pytest settings
