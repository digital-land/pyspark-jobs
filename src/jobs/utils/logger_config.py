"""
Simplified logging configuration module for PySpark jobs.

This module provides essential logging functionality with a focus on simplicity
and ease of use while maintaining the core features needed for PySpark jobs.

Usage:
    from jobs.utils.logger_config import setup_logging, get_logger

    # Setup logging (call once at application start)
    setup_logging()

    # Get a logger for your module
    logger = get_logger(__name__)
    logger.info("This is an info message")
"""

import functools
import logging.config
import os
import time
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    environment: Optional[str] = None,
) -> None:
    """
    Setup logging configuration for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
        environment: Environment name (development, staging, production)

    Environment Variables:
        LOG_LEVEL: Logging level (default: INFO)
        LOG_FILE: Path to log file
        ENVIRONMENT: Environment name (default: development)
    """
    # Get configuration from environment variables with defaults
    log_level = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = log_file or os.getenv("LOG_FILE")
    environment = environment or os.getenv("ENVIRONMENT", "development")

    # Default format based on environment
    if environment.lower() == "production":
        log_format = "[%(asctime)s] %(levelname)s - %(name)s - %(message)s"
    else:
        log_format = "[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s"

    # Create basic configuration
    handlers = ["console"]
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {"format": log_format, "datefmt": "%Y-%m-%d %H:%M:%S"}
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": log_level,
                "stream": "ext://sys.stdout",
            }
        },
        "loggers": {
            # Reduce noise from third - party libraries
            "boto3": {"level": "WARNING"},
            "botocore": {"level": "WARNING"},
            "urllib3": {"level": "WARNING"},
            "py4j": {"level": "WARNING"},
            "pyspark": {"level": "WARNING"},
        },
    }

    # Add file handler if log file is specified
    if log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        logging_config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "level": log_level,
            "filename": log_file,
            "mode": "a",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
            "encoding": "utf - 8",
        }
        handlers.append("file")

    # Set root logger configuration
    logging_config["root"] = {"handlers": handlers, "level": log_level}

    # Apply configuration
    logging.config.dictConfig(logging_config)

    # Log the configuration setup
    root_logger = logging.getLogger()
    root_logger.info(
        f"Logging configured - Level: {log_level}, Environment: {environment}"
    )
    if log_file:
        root_logger.info(f"File logging enabled: {log_file}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_execution_time(func):
    """
    Decorator to log function execution time.

    Usage:
        @log_execution_time
        def my_function():
            # function code
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()

        try:
            logger.info(f"Starting execution of {func.__name__}")
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"Completed {func.__name__} in {duration:.2f} seconds")
            return result

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Failed {func.__name__} after {duration:.2f} seconds: {e}")
            raise

    return wrapper


def set_spark_log_level(log_level: str = "WARN") -> None:
    """
    Set Spark - specific logging level to reduce verbosity.

    Args:
        log_level: Spark log level (OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL)
    """
    try:
        from pyspark import SparkContext

        # Get or create Spark context
        sc = SparkContext.getOrCreate()
        sc.setLogLevel(log_level)

        logger = get_logger(__name__)
        logger.info(f"Spark log level set to: {log_level}")

    except ImportError:
        # PySpark not available
        pass
    except Exception as e:
        logger = get_logger(__name__)
        logger.warning(f"Failed to set Spark log level: {e}")


# Convenience function for quick setup
def quick_setup(
    log_level: str = "INFO", environment: str = "development"
) -> logging.Logger:
    """
    Quick logging setup for simple scripts.

    Args:
        log_level: Logging level
        environment: Environment name

    Returns:
        Root logger
    """
    setup_logging(log_level=log_level, environment=environment)
    return get_logger(__name__)


if __name__ == "__main__":
    # Example usage and testing
    print("Testing simplified logging configuration...")

    # Test basic setup
    setup_logging(log_level="DEBUG", environment="development")
    logger = get_logger(__name__)

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")

    # Test execution time decorator
    @log_execution_time
    def test_function():
        time.sleep(1)
        return "completed"

    result = test_function()
    print("Logging test completed successfully!")
