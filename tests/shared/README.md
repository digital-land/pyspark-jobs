# PySpark Jobs - Test Utilities

This directory contains test utilities and executable scripts for managing and testing the PySpark jobs project.

## Scripts

### `test_runner`
**Main test execution script** - Provides comprehensive testing capabilities with proper environment management.

```bash
./tests/utils/test_runner --help     # Show all options
./tests/utils/test_runner --smoke    # Quick validation
./tests/utils/test_runner --unit     # Unit tests
./tests/utils/test_runner --all      # All tests with coverage
```

**Key Features:**
- Automatic environment setup and validation
- Support for unit, integration, and acceptance tests
- Coverage reporting with HTML output
- Code quality checks (linting, formatting)
- Parallel test execution options

### `simple_test`
**Basic setup verification** - Lightweight script to verify the development environment is working correctly.

```bash
./tests/utils/simple_test           # Run basic verification tests
```

**Use Cases:**
- Quick environment validation after setup
- Troubleshooting import or configuration issues
- CI/CD pipeline health checks

## Usage Patterns

### Development Workflow
```bash
# After making changes
./tests/utils/test_runner --smoke

# Before committing
./tests/utils/test_runner --all --coverage

# Code quality
./tests/utils/test_runner --lint
./tests/utils/test_runner --format
```

### Continuous Integration
```bash
# Environment check
./tests/utils/simple_test

# Full test suite
./tests/utils/test_runner --all --coverage
```

### Debugging
```bash
# Check dependencies
./tests/utils/test_runner --check-deps

# Run specific test
./tests/utils/test_runner --specific test_name

# Clean artifacts
./tests/utils/test_runner --clean
```

## Requirements

- Python 3.8+
- Virtual environment: `pyspark-jobs-venv/`
- Dependencies installed via `make init-local`

## Notes

- Scripts automatically handle PYTHONPATH and environment variables
- All paths are relative to the project root
- Use `make` commands for convenient access to common operations
