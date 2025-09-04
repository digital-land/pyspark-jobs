# Testing Documentation

This directory contains comprehensive guides for setting up and running local tests for the PySpark jobs project.

## 📚 Available Guides

### 🧪 **Testing Setup & Guides**
- **[Local Testing Guide](./LOCAL_TESTING_GUIDE.md)**
  - Complete guide for setting up and running tests locally
  - Environment setup, test categories, best practices
  - Comprehensive coverage of unit, integration, and acceptance tests

- **[Testing Setup Summary](./TESTING_SETUP_SUMMARY.md)**
  - Quick reference and setup instructions
  - Overview of implemented testing infrastructure
  - Fast-track guide for getting started

## 🎯 Quick Start

### Setup (One-time)
```bash
# Set up testing environment
make init-local
source pyspark-jobs-venv/bin/activate
```

### Run Tests
```bash
# Quick validation
./tests/utils/test_runner --smoke

# Full test suite
./tests/utils/test_runner --all --coverage

# Basic environment check
./tests/utils/simple_test
```

## 🏗️ Testing Infrastructure

### Test Structure
```
tests/
├── utils/                    # Test utilities and runners
│   ├── test_runner          # Main test execution script
│   ├── simple_test          # Basic setup verification
│   └── README.md            # Test utilities documentation
├── fixtures/                # Test data and mocks
│   ├── sample_data.py       # Realistic test data generators
│   └── mock_services.py     # AWS service mocking
├── unit/                    # Unit tests (fast, isolated)
│   └── test_*.py           # Individual function tests
├── integration/             # Integration tests (components)
│   └── test_*_integration.py # Component interaction tests
├── acceptance/              # Acceptance tests (end-to-end)
│   └── test_*.py           # Complete workflow tests
└── conftest.py             # pytest configuration and fixtures
```

### Test Categories

| Type | Purpose | Speed | Dependencies |
|------|---------|-------|--------------|
| **Unit** | Individual functions | Fast (~2min) | Mocked |
| **Integration** | Component interaction | Medium (~5min) | Mocked AWS |
| **Acceptance** | End-to-end workflows | Slow (~10min) | Full system |
| **Smoke** | Quick validation | Very fast (~30s) | Core only |

## 🔧 Available Test Commands

### Using Test Runner (Recommended)
```bash
# Test execution
./tests/utils/test_runner --smoke          # Quick validation
./tests/utils/test_runner --unit           # Unit tests only
./tests/utils/test_runner --integration    # Integration tests only
./tests/utils/test_runner --all            # All tests
./tests/utils/test_runner --coverage       # With coverage report

# Code quality
./tests/utils/test_runner --lint           # Run linting
./tests/utils/test_runner --format         # Format code

# Utilities
./tests/utils/test_runner --check-deps     # Verify dependencies
./tests/utils/test_runner --clean          # Clean artifacts
```

### Using Make Commands
```bash
make test                   # Run all tests
make test-unit             # Unit tests only
make test-integration      # Integration tests only
make test-coverage         # With coverage report
make test-smoke            # Quick validation
```

### Direct pytest Usage
```bash
# Activate environment first
source pyspark-jobs-venv/bin/activate

# Run tests
pytest tests/ -v                                    # All tests
pytest tests/unit/ -m unit -v                      # Unit tests only
pytest tests/ --cov=src --cov-report=html          # With coverage
```

## 🎛️ Test Features

### Mock Services
All AWS services are automatically mocked:
- **S3** with pre-configured test buckets
- **Secrets Manager** with test secrets
- **PostgreSQL** connections
- **HTTP requests** for external APIs

### Sample Data
Realistic test data generators for:
- **Transport access node** data
- **Fact table** data with proper schemas
- **Entity data** with geometry columns
- **Configuration files** and metadata

### Spark Testing
- **Local Spark sessions** optimized for testing
- **DataFrame fixtures** with realistic data
- **Schema validation** helpers
- **Performance benchmarking** utilities

## 📊 Performance Benchmarks

Expected execution times:
- **Smoke tests**: < 30 seconds
- **Unit tests**: < 2 minutes
- **Integration tests**: < 5 minutes
- **Full test suite**: < 10 minutes

## 🚨 Common Testing Issues

### Environment Issues
```bash
# Virtual environment not found
make init-local

# Import errors
export PYTHONPATH="${PWD}/src:${PYTHONPATH}"
# Or use test_runner which handles this automatically

# Java/Spark issues
brew install openjdk@11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

### Dependency Issues
```bash
# Missing test dependencies
pip install -r requirements-local.txt

# PySpark not working
python -c "from pyspark.sql import SparkSession; print('OK')"
```

### Performance Issues
```bash
# Tests running slowly
./tests/utils/test_runner --smoke  # Use faster subset

# Memory issues with large DataFrames
# Use smaller test datasets in fixtures
```

## 🎯 Best Practices

### Test Organization
1. **One test class per module** being tested
2. **Descriptive test names** explaining the scenario
3. **Proper test markers** for categorization
4. **Clear arrange-act-assert** structure

### Mock Usage
1. **Use provided fixtures** for AWS services
2. **Mock external dependencies** completely
3. **Test error scenarios** with mocked failures
4. **Verify mock interactions** when appropriate

### Performance
1. **Use session-scoped fixtures** for expensive setup
2. **Run smoke tests** during development
3. **Use smaller datasets** in unit tests
4. **Profile slow tests** and optimize

## 🔍 Related Documentation

- **[Architecture](../architecture/LOGGING.md)** - Logging setup for tests
- **[Database](../database/)** - Database connectivity for integration tests
- **[Troubleshooting](../troubleshooting/)** - Solutions for common testing issues

## 🤝 Contributing Tests

When adding new tests:
1. **Choose appropriate category** (unit/integration/acceptance)
2. **Add proper markers** (`@pytest.mark.unit`, etc.)
3. **Use existing fixtures** where possible
4. **Follow naming conventions** (`test_function_scenario`)
5. **Include docstrings** explaining test purpose
6. **Update this documentation** if adding new patterns

---

[← Back to Main Documentation](../README.md)
