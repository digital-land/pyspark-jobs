# Testing Guide for PySpark Jobs

This directory contains tests for the PySpark Jobs project, organized by testing scope and purpose.

## Test Structure

```
tests/
├── unit/           # Unit tests - fast, isolated component tests
│   └── utils/      # Tests for utility modules
├── integration/    # Integration tests - external service interactions
├── utils/          # Test utilities and helpers
└── docs/           # Test documentation
```

## Available Tests

### Unit Tests (`tests/unit/utils/`)
- `test_logger_config.py` - Logger configuration and setup (9 tests)
- `test_aws_secrets_manager.py` - AWS Secrets Manager integration (15 tests)
- `test_df_utils.py` - DataFrame utility functions (12 tests)
- `test_s3_dataset_typology.py` - Dataset typology mapping (34 tests)

### Integration Tests (`tests/integration/`)
- `test_logging_integration.py` - Logging integration tests (1 test)
- `test_logging_standalone.py` - Standalone logging tests (1 test)
- `test_main_collection_data_integration.py` - Main collection data integration (1 test)

### Test Utilities (`tests/utils/`)
- `test_argument_parser.py` - Argument parsing tests (4 tests)
- `test_copy_protocol_setup.py` - Copy protocol setup tests (1 test)
- `test_logging_quick.py` - Quick logging tests (13 tests)
- `test_utils.py` - General test utilities (6 tests)

**Total: 96 tests (100% pass rate)**

## Running Tests

### Prerequisites

1. **Initialize the development environment**:
```bash
make init
```

This creates `pyspark-jobs-venv` and installs all dependencies.

2. **Activate the virtual environment**:
```bash
source pyspark-jobs-venv/bin/activate
```

### Using Make Commands (Recommended)

```bash
# Run all tests with coverage
make test

# Run unit tests only
make test-unit

# Run integration tests
make test-integration

# Run tests with HTML coverage report
make test-coverage

# Quick run (no coverage)
make test-quick
```

### Using pytest Directly

```bash
# Activate venv first
source pyspark-jobs-venv/bin/activate

# Run all tests
pytest tests/

# Run specific test file
pytest tests/unit/utils/test_logger_config.py

# Run with verbose output
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Test Configuration

### pytest.ini
Configuration in `pytest.ini` includes:
- Test discovery patterns
- Coverage settings (80% minimum)
- Markers for test categorization
- Warning filters

### Markers
- `unit` - Fast, isolated unit tests
- `integration` - Tests with external dependencies
- `slow` - Tests taking >1 second
- `database` - Tests requiring database
- `spark` - Tests requiring Spark session

## Writing New Tests

### Unit Test Example
```python
def test_my_function():
    """Test that my function works correctly."""
    result = my_function(input_data)
    assert result == expected_output
```

### Integration Test Example
```python
@pytest.mark.integration
def test_external_service():
    """Test interaction with external service."""
    result = call_external_service()
    assert result is not None
```

## Test Best Practices

1. **Keep tests fast** - Unit tests should run in <1s
2. **Use descriptive names** - Test names should explain what is being tested
3. **Test one thing** - Each test should verify a single behavior
4. **Use fixtures** - Share common setup using pytest fixtures
5. **Mock external dependencies** - Don't rely on external services in unit tests

## Troubleshooting

### Virtual environment not found
```bash
# Solution: Initialize the environment
make init
```

### Module not found errors
```bash
# Solution: Ensure venv is activated and dependencies installed
source pyspark-jobs-venv/bin/activate
pip install -r requirements-local.txt
```

### Tests not discovered
```bash
# Solution: Run from project root
cd /path/to/pyspark-jobs
make test-unit
```

## CI/CD Considerations

For CI/CD environments with full infrastructure:
- All external services should be available (PostgreSQL, AWS, Spark)
- Use environment variables for configuration
- Run tests in parallel for faster execution
- Generate coverage reports for tracking

## Coverage

Current coverage focuses on:
- ✅ Logger configuration (98% coverage)
- ✅ AWS Secrets Manager (89% coverage)
- ✅ S3 utilities (92% coverage)
- ✅ DataFrame utilities (100% coverage)
- ✅ Dataset typology (100% coverage)

Modules requiring full infrastructure have lower coverage in local environment but are tested in CI/CD.
