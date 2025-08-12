# Logging Configuration Guide

This guide explains how to use the centralized logging module in the PySpark jobs project.

## Overview

The `utils.logger_config` module provides a flexible, environment-aware logging configuration that can be used across all PySpark jobs and utilities. It eliminates the need for duplicated logging setup code and ensures consistent logging behavior.

## Key Features

- **Environment-aware configuration**: Different log formats and levels for development vs production
- **Multiple output destinations**: Console, file, S3, or any combination
- **S3 integration**: Built-in S3 logging with buffering and automatic uploads
- **Automatic log rotation**: Prevents log files from growing too large
- **Execution time tracking**: Decorator for measuring function performance
- **Context logging**: Add contextual information to log messages
- **Spark integration**: Automatic Spark log level management
- **Security-focused**: No sensitive information in logs

## Quick Start

### Basic Setup

```python
from utils.logger_config import setup_logging, get_logger

# Setup logging (call once at application start)
setup_logging()

# Get a logger for your module
logger = get_logger(__name__)

# Use the logger
logger.info("Application started")
logger.error("Something went wrong")
```

### Environment Variables

Configure logging behavior using environment variables:

```bash
# Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
export LOG_LEVEL=INFO

# Enable file logging
export LOG_FILE_ENABLED=true
export LOG_FILE=logs/my_app.log

# Enable S3 logging
export LOG_S3_ENABLED=true
export LOG_S3_BUCKET=arn:aws:s3:::development-pyspark-jobs-logs
export LOG_S3_KEY_PREFIX=pyspark-jobs

# Environment type (affects log format)
export ENVIRONMENT=production

# Console logging (default: true)
export LOG_CONSOLE=true
```

## Configuration Options

### Log Levels

- **DEBUG**: Detailed information, typically only of interest when diagnosing problems
- **INFO**: General information about program execution
- **WARNING**: Something unexpected happened, but the software is still working
- **ERROR**: A serious problem occurred, but the program can continue
- **CRITICAL**: A very serious error occurred, program may not be able to continue

### Log Formats

**Development Format** (includes line numbers):
```
[2023-12-07 10:30:45] INFO - jobs.main_collection_data:123 - create_spark_session() - Spark session created
```

**Production Format** (cleaner, no line numbers):
```
[2023-12-07 10:30:45] INFO - jobs.main_collection_data - Spark session created
```

## Usage Examples

### 1. Basic Logging Setup

```python
from utils.logger_config import setup_logging, get_logger

# Setup with custom configuration
setup_logging(
    log_level="INFO",
    enable_file=True,
    log_file="logs/etl_job.log",
    environment="development"
)

logger = get_logger(__name__)
```

### 2. Environment-Based Configuration

```python
from utils.logger_config import setup_logging, get_logger

# Uses environment variables for configuration
setup_logging()

logger = get_logger(__name__)
```

### 3. Execution Time Tracking

```python
from utils.logger_config import log_execution_time, get_logger

logger = get_logger(__name__)

@log_execution_time
def process_data(df):
    """This function's execution time will be automatically logged."""
    # Process data
    return processed_df

# Output: 
# [2023-12-07 10:30:45] INFO - Starting execution of process_data
# [2023-12-07 10:30:47] INFO - Completed process_data in 2.35 seconds
```

### 4. Context Logging

```python
from utils.logger_config import LogContext, get_logger

logger = get_logger(__name__)

with LogContext({"job_id": "ETL_123", "dataset": "transport"}):
    logger.info("Processing started")  # Will include job_id and dataset in logs
    logger.info("Data transformation completed")
```

### 5. Spark Integration

```python
from utils.logger_config import set_spark_log_level

# Reduce Spark's verbose logging
set_spark_log_level("WARN")  # Options: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL
```

### 6. S3 Logging

```python
from utils.logger_config import setup_logging, get_logger

# Setup with S3 logging
setup_logging(
    log_level="INFO",
    enable_console=True,
    enable_s3=True,
    s3_bucket="arn:aws:s3:::development-pyspark-jobs-logs",
    s3_key_prefix="my-app-logs",
    environment="production"
)

logger = get_logger(__name__)
logger.info("This message will be sent to both console and S3")

# S3 logs are organized by timestamp:
# s3://development-pyspark-jobs-logs/my-app-logs/2023/12/07/14/job-EMR_123-20231207-143025.log
```

### 7. Quick Setup for Scripts

```python
from utils.logger_config import quick_setup

# One-liner setup for simple scripts
logger = quick_setup(log_level="INFO", environment="development")
logger.info("Script started")
```

## Integration with Existing Code

### Updating main_collection_data.py

**Before:**
```python
import logging
from logging.config import dictConfig

LOGGING_CONFIG = {
    # ... complex configuration
}

dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)
```

**After:**
```python
from utils.logger_config import setup_logging, get_logger, log_execution_time

setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    enable_file=True,
    log_file="logs/emr_transform_job.log",
    enable_s3=True,
    s3_bucket="arn:aws:s3:::development-pyspark-jobs-logs",
    s3_key_prefix="pyspark-jobs",
    environment=os.getenv("ENVIRONMENT", "development")
)

logger = get_logger(__name__)

# Add execution time tracking to key functions
@log_execution_time
def main(args):
    # ... function code
```

## Best Practices

### 1. Security
- **Never log sensitive information**: passwords, API keys, personal data
- **Use appropriate log levels**: Don't log everything at INFO level
- **Sanitize inputs**: Remove or mask sensitive data before logging

### 2. Performance
- **Use appropriate log levels**: DEBUG logs should only be enabled when needed
- **Avoid expensive operations in log statements**: Don't compute complex values just for logging
- **Use lazy evaluation**: `logger.debug("Result: %s", expensive_function())` only calls `expensive_function()` if DEBUG is enabled

### 3. Structure
- **Use meaningful logger names**: Use `__name__` or descriptive module names
- **Group related logs**: Use consistent prefixes for related operations
- **Log at appropriate points**: Function entry/exit, error conditions, major milestones

### 4. Environment-Specific Configuration

**Development:**
```python
setup_logging(
    log_level="DEBUG",
    enable_console=True,
    enable_file=True,
    environment="development"
)
```

**Production:**
```python
setup_logging(
    log_level="WARNING",
    enable_console=False,
    enable_file=True,
    log_file="/var/log/pyspark/job.log",
    enable_s3=True,
    s3_bucket="arn:aws:s3:::production-pyspark-jobs-logs",
    s3_key_prefix="production-logs",
    environment="production"
)
```

## File Structure

```
logs/
├── emr_transform_job.log      # Main ETL job logs
├── emr_transform_job.log.1    # Rotated log files
├── emr_transform_job.log.2
└── ...

src/utils/
├── logger_config.py           # Main logging module
└── ...

examples/
├── logging_usage_example.py   # Usage examples
└── ...
```

## Troubleshooting

### Common Issues

1. **Logs not appearing**: Check log level configuration
2. **File permission errors**: Ensure log directory is writable
3. **Log files not rotating**: Check file size limits and backup count
4. **Duplicate log messages**: Avoid calling `setup_logging()` multiple times

### Debug Logging Configuration

```python
from utils.logger_config import get_logging_config
import json

# Print current logging configuration
config = get_logging_config(log_level="DEBUG", enable_file=True)
print(json.dumps(config, indent=2))
```

## Migration from Old Logging

If you have existing code with manual logging configuration:

1. **Remove old logging setup**: Delete `LOGGING_CONFIG` dictionaries and `dictConfig()` calls
2. **Import new module**: `from utils.logger_config import setup_logging, get_logger`
3. **Setup logging once**: Call `setup_logging()` at application startup
4. **Get loggers**: Replace `logging.getLogger(__name__)` with `get_logger(__name__)`
5. **Add decorators**: Use `@log_execution_time` for key functions
6. **Configure environment**: Set environment variables for production deployment

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Minimum log level to output |
| `LOG_FORMAT` | Auto | Custom log format string |
| `LOG_FILE` | None | Path to log file |
| `LOG_CONSOLE` | `true` | Enable console logging |
| `LOG_FILE_ENABLED` | `false` | Enable file logging |
| `LOG_S3_ENABLED` | `false` | Enable S3 logging |
| `LOG_S3_BUCKET` | None | S3 bucket name or ARN |
| `LOG_S3_KEY_PREFIX` | `logs` | S3 key prefix |
| `ENVIRONMENT` | `development` | Environment type (affects format) |

## Examples

See `examples/logging_usage_example.py` for comprehensive usage examples covering all features of the logging module.
