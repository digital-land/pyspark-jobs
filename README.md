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
├── requirements.txt              # Production dependencies
├── requirements-test.txt         # Testing dependencies
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

# Create development environment with all dependencies
make init

# Or use the setup script directly
./setup_venv.sh --type development
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
# For development (includes all testing and linting tools)
pip install -r requirements-dev.txt

# OR for production only
pip install -r requirements.txt

# For EMR Serverless deployment
pip install -r requirements-emr.txt
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
| **Local Testing** | `make init-local` or `./setup_venv.sh --type local` | Lightweight testing on any platform (Windows, Mac, Linux) |
| **Development** | `make init` or `./setup_venv.sh --type development` | Full development with testing, linting, and documentation tools |
| **Production** | `./setup_venv.sh --type production` | Production deployment with minimal dependencies |
| **EMR Serverless** | `./setup_venv.sh --type emr` | EMR-compatible dependencies (excludes pre-installed packages) |

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
python src/jobs/run_main.py \
  --load_type full \
  --data_set transport-access-node \
  --path s3://your-bucket/data/
```

2. **Execute the main ETL pipeline:**
```bash
python src/jobs/main_collection_data.py
```

## 🧪 Testing

This project includes a comprehensive test suite with three levels of testing:

### Running Tests

```bash
# Run all tests
pytest

# Run specific test categories
pytest -m unit                    # Fast unit tests
pytest -m integration             # Integration tests
pytest -m acceptance              # End-to-end tests

# Run with coverage
pytest --cov=src --cov-report=html

# Run in parallel
pytest -n auto
```

### Test Structure

- **Unit Tests** (`tests/unit/`): Fast, isolated component tests
- **Integration Tests** (`tests/integration/`): Database and external service tests  
- **Acceptance Tests** (`tests/acceptance/`): Complete workflow validation

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
aws s3 cp src/jobs/run_main.py s3://your-bucket/scripts/
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

# Development
make test              # Run all tests
make test-unit         # Run unit tests only
make test-integration  # Run integration tests
make test-coverage     # Run tests with coverage report
make lint              # Run all linting checks
make format            # Format code with black and isort
make type-check        # Run type checking with mypy
make security          # Run security scans

# Code Quality
make pre-commit        # Run pre-commit hooks on all files
make install-hooks     # Install pre-commit hooks

# Building and Packaging
make build             # Build Python package
make package           # Create AWS deployment package
make upload-s3         # Build and upload to S3

# Utilities
make clean             # Clean cache and log files
make clean-all         # Clean everything including venv
make run-notebook      # Start Jupyter Lab
make docs              # Generate documentation
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

The project includes configuration for:
- Automated testing with pytest
- Code quality checks
- AWS deployment pipelines
- Docker containerization support

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
pyspark-jobs
repo for pyspark jobs. added code for issue table, fact-res, fact tables.
main
