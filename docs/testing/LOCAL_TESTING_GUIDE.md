# Local Testing Guide

## Quick Start

```bash
# Setup virtual environment
make init

# Run all tests with coverage
make test

# Run unit tests only
make test-unit
```

## Environment Setup

### Prerequisites

- Python 3.9+
- Java 11+ (for PySpark)

### Setup

```bash
cd pyspark-jobs
make init  # Creates .venv and installs dependencies
source .venv/bin/activate
```

### Manual Setup (Alternative)

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip setuptools wheel
pip install -r requirements-local.txt
pip install -e .

# Verify PySpark
python -c "from pyspark.sql import SparkSession; print('PySpark OK')"
```

## Running Tests

### Using Make (Recommended)

```bash
make test                # All tests with coverage
make test-unit           # Unit tests only
make test-integration    # Integration tests only
make test-acceptance     # Acceptance tests only
make test-quick          # Unit tests, no coverage
make lint                # Linting checks
make format              # Format code with black and isort
```

### Using pytest Directly

```bash
source .venv/bin/activate

# All tests
pytest tests/ -v

# By marker
pytest tests/ -m unit -v
pytest tests/ -m integration -v
pytest tests/ -m database -v

# With coverage
pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

# Specific file
pytest tests/unit/test_entity_transformer.py -v

# Specific test function
pytest tests/unit/test_entity_transformer.py::test_transform_creates_expected_columns -v
```

## Database Integration Tests

Tests marked `@pytest.mark.database` run against a real PostgreSQL/PostGIS database:

- **Locally**: Uses testcontainers to spin up a `postgis/postgis:14-master` Docker container automatically
- **CI**: Uses the GitHub Actions postgres service via the `DATABASE_URL` environment variable

```bash
# Run database tests (requires Docker locally)
pytest tests/ -m database -v
```

See [tests/README.md](../../tests/README.md) for fixture details and test conventions.

## Troubleshooting

### Spark Session Creation Fails

```bash
# Error: JAVA_HOME is not set
brew install openjdk@11
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

### Import Errors

```bash
# Error: ModuleNotFoundError: No module named 'jobs'
# Solution: Install the package in editable mode
pip install -e .
```

### Memory Issues

```python
# Use smaller test datasets
def test_with_small_data(spark):
    small_data = sample_data[:10]
    df = spark.createDataFrame(small_data)
```

### Debug Mode

```bash
# Drop into debugger on failure
pytest tests/unit/test_entity_transformer.py --pdb

# Maximum verbosity
pytest tests/unit/test_entity_transformer.py -v -s
```
