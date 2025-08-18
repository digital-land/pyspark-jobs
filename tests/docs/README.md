# Test Documentation

This directory contains documentation related to testing the PySpark Jobs project.

## Contents

### Logging Test Documentation

- **[TEST_LOGS_SETUP.md](./TEST_LOGS_SETUP.md)** - Comprehensive guide to the test logs directory setup and organization
- **[LOGGING_TEST_RESULTS.md](./LOGGING_TEST_RESULTS.md)** - Results and analysis of logging configuration tests

## Test Structure

The tests are organized into the following categories:

```
tests/
├── docs/                    # Test documentation (this directory)
├── utils/                   # Test utilities and helper scripts
│   ├── test_utils.py       # Test utility functions
│   └── test_logging_quick.py # Quick logging verification script
├── unit/                   # Unit tests
│   ├── simple_logging_test.py # Simple logging tests (no external deps)
│   ├── test_simple_logging.py # Pytest-based unit tests
│   └── test_logger_config.py  # Comprehensive logger configuration tests
├── integration/            # Integration tests
│   ├── test_logging_standalone.py # Standalone diagnostic tests
│   └── test_logging_integration.py # Pytest-based integration tests
├── acceptance/             # End-to-end acceptance tests
└── logs/                   # Test log files (gitignored)
```

## Quick Test Commands

```bash
# Quick verification
python3 tests/utils/test_logging_quick.py

# Unit tests
python3 tests/unit/simple_logging_test.py
python3 -m pytest tests/unit/ -v

# Integration tests
python3 tests/integration/test_logging_standalone.py
python3 -m pytest tests/integration/ -v

# All tests
python3 -m pytest tests/ -v
```

## Documentation Guidelines

### For Test Documentation

Test-specific documentation should be placed in this directory (`tests/docs/`) and should cover:

- Test setup and configuration guides
- Test results and analysis
- Testing methodology and best practices
- Troubleshooting test issues

### For General Project Documentation

General project documentation should be placed in the main `docs/` directory and covers:

- Build guides and deployment instructions
- API documentation
- Architecture documentation
- User guides

## Contributing

When adding new test documentation:

1. Place test-specific docs in `tests/docs/`
2. Use clear, descriptive filenames
3. Update this README to include new documentation
4. Follow the existing documentation style and format
