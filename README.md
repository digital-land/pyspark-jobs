# PySpark Jobs for Digital Land

ETL pipelines for processing digital land data collections, designed for AWS EMR Serverless.

## Project Structure

```
pyspark-jobs/
├── entry_points/
│   └── run_main.py              # EMR entry point (CLI)
├── src/jobs/                    # Application code
│   ├── job.py                   # Job orchestration
│   ├── pipeline.py              # Pipeline classes (Entity, Issue, Fact, etc.)
│   ├── config/                  # Dataset and schema configuration
│   ├── dbaccess/                # Database connectivity (pg8000, secrets)
│   ├── transform/               # Transformers (entity, fact, fact_resource, issue)
│   └── utils/                   # Utilities (S3, logging, geometry, postgres writer)
├── tests/
│   ├── unit/                    # Fast, isolated tests
│   ├── integration/             # Database and Spark integration tests
│   └── acceptance/              # End-to-end workflow tests
├── docs/                        # Documentation (database, deployment, architecture)
├── requirements.txt             # Production dependencies (EMR Serverless)
├── requirements-local.txt       # Local dev/test dependencies
├── Makefile                     # Development commands
└── setup.py                     # Package configuration
```

## Quick Start

### Prerequisites

- Python 3.9+
- Java 11+ (for PySpark)
- Docker (for database integration tests)

### Setup

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies and package
make init
```

`make init` installs all dependencies, sets up the package in development mode, and configures pre-commit hooks.

### Running Tests

```bash
make test                # All tests with coverage
make test-unit           # Unit tests only
make test-integration    # Integration tests (requires Docker for testcontainers)
make test-acceptance     # Acceptance tests
make test-quick          # Unit tests, no coverage
```

Or use pytest directly:

```bash
pytest tests/ -v
pytest tests/unit/ -v
pytest tests/integration/ -v
```

For more detail on test structure, fixtures, markers, and conventions, see [tests/README.md](tests/README.md).

### Code Quality

```bash
make format              # Format with black and isort
make lint                # Check with black and flake8
make security            # Run bandit and safety
make pre-commit          # Run all pre-commit hooks
```

## Running Locally

```bash
python entry_points/run_main.py \
  --load_type full \
  --dataset transport-access-node \
  --collection transport-access-node-collection \
  --env local \
  --database-url "postgresql://user:pass@localhost:5432/dbname"
```

If `--database-url` is not provided, credentials are resolved from AWS Secrets Manager.

## Deployment

### Build and upload to S3

```bash
make package             # Create AWS deployment package
make upload-s3           # Build and upload to S3
```

### EMR Serverless job submission

```json
{
  "sparkSubmit": {
    "entryPoint": "s3://{bucket}/pkg/entry_script/run_main.py",
    "sparkSubmitParameters": "--py-files s3://{bucket}/pkg/whl_pkg/pyspark_jobs-*.whl"
  }
}
```

## CI/CD

### Test Workflow (`test.yml`)
- Linting checks and full test suite with a PostgreSQL service container
- Coverage reports uploaded as artifacts

### Publish Workflow (`publish.yml`)
- Matrix-based deployment to multiple environments
- Builds Python wheel, dependencies zip, and multi-platform Docker images (amd64/arm64)
- Pushes to S3 and Amazon ECR

## Documentation

See the [docs/](docs/) directory for detailed guides on database connectivity, deployment, architecture, and troubleshooting.
