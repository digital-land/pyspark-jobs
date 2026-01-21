pyspark-dev
# PySpark Jobs for Digital Land

A comprehensive PySpark data processing framework designed for Amazon EMR Serverless with Apache Airflow integration. This project provides scalable ETL pipelines for processing and transforming digital land data collections.

## 🏗️ Project Overview

This repository contains PySpark jobs that process various digital land datasets including:
- Transport access nodes
- Title boundaries  
- Entity data transformations
- Fact and fact resource processing
- Issue tracking and validation

### Key Features

- ✅ **EMR Serverless Ready**: Optimized for AWS EMR Serverless execution
- ✅ **Airflow Integration**: DAGs for orchestrating data workflows
- ✅ **Modular Design**: Reusable transformation components
- ✅ **Comprehensive Testing**: Unit, integration, and acceptance tests with pytest
- ✅ **Configuration Management**: JSON-based dataset and schema configuration
- ✅ **AWS Secrets Integration**: Secure credential management
- ✅ **Multiple Output Formats**: Support for Parquet, CSV, and database outputs

## 📁 Project Structure

```
pyspark-jobs/
├── src/                          # Source code
│   ├── jobs/                     # Core PySpark job modules
│   │   ├── main_collection_data.py      # Main ETL pipeline
│   │   ├── transform_collection_data.py # Data transformation logic
│   │   ├── run_main.py                  # EMR entry point script
│   │   ├── config/                      # Configuration files
│   │   │   ├── datasets.json           # Dataset definitions
│   │   │   └── transformed_source.json # Schema configurations
│   │   └── dbaccess/                    # Database connectivity modules
│   ├── utils/                    # Utility modules
│   │   ├── aws_secrets_manager.py      # AWS Secrets Manager integration
│   │   └── path_utils.py               # Path resolution utilities
│   ├── airflow/                  # Airflow DAGs and configuration
│   │   └── dags/                       # Airflow DAG definitions
│   └── infra/                    # Infrastructure scripts
│       └── emr/                        # EMR deployment scripts
├── tests/                        # Comprehensive test suite
│   ├── unit/                     # Unit tests (fast, isolated)
│   ├── integration/              # Integration tests (databases, files)
│   ├── acceptance/               # End-to-end workflow tests
│   └── conftest.py              # Shared test configuration
├── examples/                     # Usage examples
├── requirements.txt              # EMR Serverless dependencies (excludes pre-installed packages)
├── requirements-local.txt        # Local testing dependencies (includes requirements.txt + dev tools)
├── pytest.ini                   # Pytest configuration
├── setup.py                     # Package configuration
└── README.md                    # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+ (Python 3.9+ recommended)
- Java 11+ (for PySpark)
- Apache Spark 3.3+
- AWS CLI configured (for deployment)
- Git (for version control)

### Automated Setup (Recommended)

The easiest way to get started is using our automated setup script:

```bash
# Clone the repository
git clone <repository-url>
cd pyspark-jobs

# Create local testing environment (default)
make init

# Or use the setup script directly  
./setup_venv.sh --type local
```

This will automatically:
- ✅ Create a Python virtual environment
- ✅ Install all development dependencies
- ✅ Set up pre-commit hooks
- ✅ Install the package in development mode
- ✅ Create a .env configuration template

### Manual Installation

If you prefer manual setup:

1. **Clone the repository:**
```bash
git clone <repository-url>
cd pyspark-jobs
```

2. **Create and activate virtual environment:**
```bash
# Create virtual environment
python3 -m venv pyspark-jobs-venv

# Activate virtual environment
source pyspark-jobs-venv/bin/activate

# Upgrade pip
pip install --upgrade pip setuptools wheel
```

3. **Install dependencies:**
```bash
# For local testing and development (recommended)
pip install -r requirements-local.txt

# OR for EMR deployment dependencies only
pip install -r requirements.txt
```

4. **Install the package in development mode:**
```bash
pip install -e .
```

5. **Set up pre-commit hooks (optional but recommended):**
```bash
pre-commit install
```

### Environment Types

Choose the appropriate environment for your use case:

| Environment Type | Command | Use Case |
|-----------------|---------|----------|
| **Local Testing** | `make init` | Local development and testing (default) |
| **Production Deployment** | `./build_aws_package.sh` | Create deployment package for EMR Serverless |

### Verification

After setup, verify your installation:

```bash
# Check Python version
python --version

# Check installed packages
pip list | grep -E "(pyspark|boto3|pytest)"

# Run quick test
pytest tests/unit/simple_logging_test.py -v

# Check code formatting
make format && make lint
```

### Running Locally

1. **Run a specific transformation:**
```bash
python run_main.py \
  --load_type full \
  --data_set transport-access-node \
  --path s3://your-bucket/data/
```

2. **Execute the main ETL pipeline:**
```bash
python src/jobs/main_collection_data.py
```

## 🧪 Testing

This project includes a comprehensive test suite with **80%+ code coverage** across all modules:

### Quick Testing

```bash
# Run all tests with coverage (recommended)
make test

# Run specific test categories
make test-unit           # Fast unit tests only
make test-integration    # Integration tests
make test-quick          # Quick development testing
make test-parallel       # Parallel execution
make test-coverage       # HTML coverage report
```

### Advanced Testing

```bash
# Using the test runner directly
python run_tests.py --coverage --html-report
python run_tests.py --unit --parallel
python run_tests.py --quick --verbose

# Pytest directly
pytest tests/unit/ -v --cov=src
pytest -m "unit and not slow"
```

### Test Structure

- **Unit Tests** (`tests/unit/`): Fast, isolated component tests (80%+ coverage)
- **Integration Tests** (`tests/integration/`): Database and external service tests  
- **Acceptance Tests** (`tests/acceptance/`): Complete workflow validation

### Coverage by Module

| Module | Coverage Target | Test File |
|--------|----------------|----------|
| `main_collection_data.py` | 85%+ | `test_main_collection_data.py` |
| `transform_collection_data.py` | 90%+ | `test_transform_collection_data.py` |
| `csv_s3_writer.py` | 85%+ | `test_csv_s3_writer.py` |
| `logger_config.py` | 95%+ | `utils/test_logger_config.py` |
| `aws_secrets_manager.py` | 90%+ | `utils/test_aws_secrets_manager.py` |
| `s3_utils.py` | 85%+ | `utils/test_s3_utils.py` |
| `df_utils.py` | 95%+ | `utils/test_df_utils.py` |

For detailed testing information, see [tests/README.md](tests/README.md).

## 📊 Data Processing Workflows

### Main ETL Pipeline

The core ETL pipeline (`main_collection_data.py`) processes data through these stages:

1. **Data Extraction**: Load from S3 CSV files
2. **Data Transformation**: Apply business logic transformations
3. **Data Loading**: Output to partitioned Parquet files

### Supported Datasets

Configure datasets in `src/jobs/config/datasets.json`:

```json
{
  "transport-access-node": {
    "path": "s3://bucket/transport-access-node-collection/",
    "enabled": true
  },
  "title-boundaries": {
    "path": "s3://bucket/title-boundary-collection/", 
    "enabled": false
  }
}
```

### Transformation Types

- **Fact Processing**: Deduplicate and prioritize fact records
- **Fact Resource Processing**: Extract resource relationships
- **Entity Processing**: Pivot fields into structured entity records
- **Issue Processing**: Track and validate data quality issues

## 🔧 Configuration

### Environment Variables

```bash
# AWS Configuration
export AWS_REGION=eu-west-2
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Database Configuration (optional)
export POSTGRES_SECRET_NAME=your-secret-name
export USE_DATABASE=true

# Spark Configuration
export PYSPARK_PYTHON=python3
export SPARK_HOME=/path/to/spark
```

### AWS Secrets Manager

Use AWS Secrets Manager for secure credential storage:

```python
from utils.aws_secrets_manager import get_database_credentials

# Retrieve database credentials
db_creds = get_database_credentials("myapp/database/postgres")
```

See [examples/secrets_usage_example.py](examples/secrets_usage_example.py) for detailed usage.

## 🚁 Deployment

### EMR Serverless Deployment

1. **Package the application:**
```bash
python setup.py bdist_wheel
```

2. **Upload to S3:**
```bash
aws s3 cp dist/pyspark_jobs-*.whl s3://your-bucket/packages/
aws s3 cp run_main.py s3://your-bucket/scripts/
```

3. **Submit EMR Serverless job:**
```bash
aws emr-serverless start-job-run \
  --application-id your-app-id \
  --execution-role-arn your-role-arn \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-bucket/scripts/run_main.py",
      "sparkSubmitParameters": "--py-files s3://your-bucket/packages/pyspark_jobs-*.whl"
    }
  }'
```

### Airflow Integration

Deploy DAGs to Amazon MWAA:

```bash
aws s3 sync src/airflow/dags/ s3://your-airflow-bucket/dags/
```

## 📈 Monitoring and Logging

### Spark UI
Access Spark UI at `http://localhost:4040` during local execution.

### CloudWatch Logs
EMR Serverless jobs automatically log to CloudWatch under:
- `/aws/emr-serverless/applications/{application-id}/jobs/{job-run-id}`

### Application Logs
Structured logging with configurable levels:

```python
import logging
logger = logging.getLogger(__name__)
logger.info("Processing started for dataset: %s", dataset_name)
```

## 🔍 Data Quality

### Schema Validation
- Automatic schema inference and validation
- Support for required and optional fields
- Data type enforcement

### Issue Tracking
- Comprehensive data quality checks
- Issue categorization and reporting
- Integration with fact/entity processing

## 🛠️ Development

### Code Structure

- **Jobs**: Main processing logic in `src/jobs/`
- **Utils**: Shared utilities in `src/utils/`
- **Configuration**: JSON-based config in `src/jobs/config/`
- **Tests**: Comprehensive test suite in `tests/`

### Adding New Transformations

1. Create transformation function in `transform_collection_data.py`
2. Add schema configuration to `config/` directory
3. Write comprehensive tests in appropriate test directory
4. Update dataset configuration if needed

### Code Quality

```bash
# Run linters
black src/ tests/
flake8 src/ tests/
isort src/ tests/

# Type checking
mypy src/
```

## 📚 Examples

See the `examples/` directory for:
- AWS Secrets Manager usage
- Custom transformation examples
- Configuration templates
- Deployment scripts

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run the test suite
5. Submit a pull request

### Development Setup

```bash
# Automated setup (recommended)
make init

# Or manual setup
pip install -r requirements-dev.txt
pre-commit install
pip install -e .
```

### Available Make Commands

Once your environment is set up, you can use these convenient Make commands:

```bash
# Environment Setup
make init              # Initialize development environment
make init-prod         # Initialize production environment  
make init-emr          # Initialize EMR-compatible environment

# Testing (Comprehensive Suite)
make test              # Run all tests with coverage
make test-unit         # Run unit tests only
make test-integration  # Run integration tests
make test-acceptance   # Run acceptance tests
make test-coverage     # Run tests with HTML coverage report
make test-quick        # Quick unit tests (no coverage)
make test-parallel     # Run tests in parallel

# Code Quality
make lint              # Run all linting checks (flake8, mypy)
make format            # Format code with black and isort
make type-check        # Run type checking with mypy
make security          # Run security scans (bandit, safety)
make pre-commit        # Run pre-commit hooks on all files
make install-hooks     # Install pre-commit hooks

# Building and Packaging
make build             # Build Python package
make package           # Create AWS deployment package
make upload-s3         # Build and upload to S3

# Development Tools
make run-notebook      # Start Jupyter Lab
make docs              # Generate documentation
make clean             # Clean cache and log files
make clean-all         # Clean everything including venv
make help              # Show all available commands
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For issues and questions:
1. Check the [tests/README.md](tests/README.md) for testing guidance
2. Review examples in the `examples/` directory
3. Check documentation in the `docs/` directory:
   - [PostgreSQL JDBC Configuration](docs/POSTGRESQL_JDBC_CONFIGURATION.md)
   - [AWS Secrets Manager Troubleshooting](docs/TROUBLESHOOTING_SECRETS_MANAGER.md)
   - [Logging Configuration](docs/LOGGING.md)
4. Open an issue on GitHub

## 🔄 CI/CD

The project includes automated CI/CD workflows for:
- **Testing**: Automated unit, integration, and acceptance tests
- **Code Quality**: Black formatting, Flake8 linting, and security scans
- **Build & Deploy**: Automated package building and artifact publishing
- **Docker Images**: Multi-platform container builds (linux/amd64, linux/arm64)
- **S3 Artifacts**: Automated deployment of wheels, dependencies, and scripts

### Deployment Pipeline

#### Matrix-Based Deployment
The workflow uses a matrix strategy to deploy to multiple environments:

- **Automatic Deployment (Push to Main)**: Deploys to all configured environments simultaneously
- **Manual Deployment (Workflow Dispatch)**: Deploy to a specific selected environment only

#### Environment Detection
The workflow automatically detects available environments from GitHub repository settings:
- If manual trigger: deploys only to the selected environment
- If push to main: deploys to all configured environments in parallel

### Automated Workflows

#### Test Workflow (`test.yml`)
- **Triggers**: Push to main, manual dispatch, workflow calls
- **Jobs**: Linting checks and comprehensive test suite with PostgreSQL
- **Artifacts**: Coverage reports and test results
- **Database**: Automated PostgreSQL setup for integration tests

#### Publish Workflow (`publish.yml`)
- **Triggers**: Push to main (automatic deployment), manual dispatch (targeted deployment)
- **Strategy**: Matrix-based deployment to multiple environments
- **Multi-Platform**: Docker buildx with linux/amd64 and linux/arm64 support
- **Artifacts**: 
  - Python wheels uploaded to S3
  - Dependencies uploaded to S3
  - Entry scripts deployed to S3
  - Multi-platform Docker images pushed to ECR
  - SBOM (Software Bill of Materials) generated
- **Versioning**: Semantic versioning with SHA and date-based tags

### Environment Configuration

Environments are automatically detected from GitHub repository settings:
1. Go to GitHub Settings → Environments
2. Create environments (e.g., `development`, `staging`, `production`)
3. Configure environment-specific secrets and protection rules
4. Add required reviewers for sensitive environments

### Docker Multi-Platform Support

The workflow builds Docker images for multiple architectures:
- **linux/amd64**: Standard x86_64 architecture
- **linux/arm64**: ARM64 architecture (Apple Silicon, AWS Graviton)
- **Build Tool**: Docker Buildx with cross-platform compilation
- **Registry**: Automatic push to Amazon ECR

### Docker Image Tags

| Tag Type | Format | Description |
|----------|--------|-------------|
| **SHA** | `{repo}:{short-sha}` | Immutable reference to specific commit |
| **Version** | `{repo}:v{YYYY.MM.DD}-{short-sha}` | Semantic versioning with date |
| **Latest** | `{repo}:latest` | Points to most recent successful build |

### Multi-Platform Architecture Support

| Platform | Architecture | Use Case |
|----------|-------------|----------|
| **linux/amd64** | x86_64 | Standard EC2 instances, most cloud environments |
| **linux/arm64** | ARM64 | AWS Graviton processors, Apple Silicon (M1/M2) |

### Build Artifacts

```bash
# Generated artifacts per build:
├── pyspark-build-artifacts/     # Python wheels and dependencies
├── sbom.json                   # Software Bill of Materials
├── coverage-report/            # HTML coverage reports
└── Docker images:              # Multi-tagged container images
    ├── {repo}:{sha}           # Immutable SHA tag
    ├── {repo}:v{date}-{sha}   # Version tag
    └── {repo}:latest            # Latest tag
```

### GitHub Actions (if configured)

```yaml
# Example workflow
name: CI/CD Pipeline
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
      - run: pip install -r requirements-test.txt
      - run: pytest --cov=src
```

---

**Built with ❤️ for Digital Land data processing**

## 📦 Deployment & Versioning

### Docker Image Versioning
- **SHA Tag**: `{repo}:{short-sha}` (e.g., `repo:abc1234`) - Immutable reference
- **Version Tag**: `{repo}:v{YYYY.MM.DD}-{short-sha}` (e.g., `repo:v2025.01.15-abc1234`) - Semantic versioning
- **Latest Tag**: `{repo}:latest` - Points to latest successful deployment

### S3 Artifact Layout
```
s3://{bucket}/pkg/
├── whl_pkg/                    # Python wheel packages
│   └── pyspark_jobs-*.whl
├── dependencies/               # External dependencies
│   └── dependencies.zip
├── entry_script/              # EMR entry points
│   └── run_main.py

```

### How Jobs Reference Artifacts
**EMR Serverless Configuration:**
```json
{
  "sparkSubmit": {
    "entryPoint": "s3://{bucket}/pkg/entry_script/run_main.py",
    "sparkSubmitParameters": "--py-files s3://{bucket}/pkg/whl_pkg/pyspark_jobs-*.whl"
  }
}
```

### Runtime Dependency Strategy
- **Base Image**: `public.ecr.aws/emr-serverless/spark/emr-7.9.0:latest`
- **Pre-installed**: Apache Sedona 1.8.0, PostgreSQL JDBC 42.7.4, pandas 2.2.3
- **Runtime**: Dependencies loaded from S3 artifacts with exact version pinning
pyspark-jobs
repo for pyspark jobs. added code for issue table, fact-res, fact tables.
main
