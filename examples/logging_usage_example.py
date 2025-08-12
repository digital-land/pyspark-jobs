"""
Example usage of the centralized logging module for PySpark jobs.

This example demonstrates how to use the new logging configuration module
to set up consistent logging across your PySpark applications.
"""

import os
import time
import logging
from utils.logger_config import (
    setup_logging, 
    get_logger, 
    log_execution_time, 
    LogContext,
    set_spark_log_level,
    quick_setup,
    S3LogHandler
)

# Example 1: Basic logging setup
def example_basic_setup():
    """Example of basic logging setup."""
    print("=== Example 1: Basic Logging Setup ===")
    
    # Setup logging with default configuration
    setup_logging(
        log_level="INFO",
        enable_console=True,
        enable_file=False,
        environment="development"
    )
    
    # Get a logger for this module
    logger = get_logger(__name__)
    
    logger.debug("This debug message won't show (log level is INFO)")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")


# Example 2: File and console logging
def example_file_logging():
    """Example of logging to both console and file."""
    print("\n=== Example 2: File and Console Logging ===")
    
    # Setup logging with file output enabled
    setup_logging(
        log_level="DEBUG",
        enable_console=True,
        enable_file=True,
        log_file="logs/example_job.log",
        environment="development"
    )
    
    logger = get_logger(__name__)
    
    logger.debug("This debug message will appear in both console and file")
    logger.info("Processing started")
    logger.warning("This is a warning that will be logged to file")
    logger.info("Processing completed")


# Example 3: Environment-based configuration
def example_environment_config():
    """Example of environment-based logging configuration."""
    print("\n=== Example 3: Environment-Based Configuration ===")
    
    # Set environment variables (in real usage, these would be set by your deployment)
    os.environ["LOG_LEVEL"] = "WARNING"
    os.environ["LOG_FILE"] = "logs/production_job.log"
    os.environ["LOG_FILE_ENABLED"] = "true"
    os.environ["ENVIRONMENT"] = "production"
    
    # Setup logging using environment variables
    setup_logging()
    
    logger = get_logger(__name__)
    
    logger.debug("This won't show (log level is WARNING)")
    logger.info("This won't show (log level is WARNING)")
    logger.warning("This warning will show")
    logger.error("This error will show")


# Example 4: Execution time decorator
@log_execution_time
def example_long_running_task():
    """Example function with execution time logging."""
    logger = get_logger(__name__)
    logger.info("Starting a simulated long-running task")
    
    # Simulate some work
    time.sleep(2)
    
    logger.info("Task processing completed")
    return "Task completed successfully"


# Example 5: Context logging
def example_context_logging():
    """Example of adding context to log messages."""
    print("\n=== Example 5: Context Logging ===")
    
    logger = get_logger(__name__)
    
    # Normal logging
    logger.info("Processing dataset")
    
    # Logging with context
    with LogContext({"job_id": "JOB_12345", "dataset": "transport-access-node"}):
        logger.info("Processing started for dataset")
        logger.info("Data transformation in progress")
        logger.warning("Some data quality issues detected")
        logger.info("Processing completed successfully")


# Example 6: Spark logging configuration
def example_spark_logging():
    """Example of configuring Spark logging levels."""
    print("\n=== Example 6: Spark Logging Configuration ===")
    
    logger = get_logger(__name__)
    
    # This would normally be called after creating a Spark session
    logger.info("Setting Spark log level to reduce verbosity")
    set_spark_log_level("WARN")
    logger.info("Spark logging configured")


# Example 7: Quick setup for simple scripts
def example_quick_setup():
    """Example of quick setup for simple scripts."""
    print("\n=== Example 7: Quick Setup ===")
    
    # Quick setup with minimal configuration
    logger = quick_setup(log_level="INFO", environment="development")
    
    logger.info("Quick setup completed")
    logger.info("This is useful for simple scripts")


# Example 8: Different loggers for different modules
def example_multiple_loggers():
    """Example of using different loggers for different modules."""
    print("\n=== Example 8: Multiple Loggers ===")
    
    # Loggers for different components
    main_logger = get_logger("main_process")
    data_logger = get_logger("data_processing")
    s3_logger = get_logger("s3_operations")
    
    main_logger.info("Main process started")
    data_logger.info("Data transformation initiated")
    s3_logger.info("Writing data to S3")
    data_logger.info("Data transformation completed")
    main_logger.info("Main process completed")


# Example 9: Production vs Development logging
def example_s3_logging():
    """Example of S3 logging configuration."""
    print("\n=== Example 9: S3 Logging ===")
    
    # S3 logging setup
    setup_logging(
        log_level="INFO",
        enable_console=True,
        enable_s3=True,
        s3_bucket="arn:aws:s3:::development-pyspark-jobs-logs",
        s3_key_prefix="example-logs",
        environment="development"
    )
    
    logger = get_logger("s3_example")
    
    logger.info("This message will be sent to both console and S3")
    logger.warning("S3 logging buffers messages and uploads periodically")
    logger.error("Error messages are also captured in S3")
    
    # Force flush to S3 (normally happens automatically)
    for handler in logging.getLogger().handlers:
        if isinstance(handler, S3LogHandler):
            handler.flush()
            break


def example_production_vs_development():
    """Example showing differences between production and development logging."""
    print("\n=== Example 10: Production vs Development Logging ===")
    
    # Development setup (more verbose, includes line numbers)
    print("Development logging:")
    setup_logging(
        log_level="DEBUG",
        environment="development"
    )
    
    dev_logger = get_logger("development_module")
    dev_logger.debug("Debug message with line number")
    dev_logger.info("Info message with detailed context")
    
    # Production setup (less verbose, no line numbers, S3 logging)
    print("\nProduction logging:")
    setup_logging(
        log_level="WARNING", 
        enable_console=False,
        enable_s3=True,
        s3_bucket="arn:aws:s3:::development-pyspark-jobs-logs",
        s3_key_prefix="production-logs",
        environment="production"
    )
    
    prod_logger = get_logger("production_module")
    prod_logger.debug("This debug won't show")
    prod_logger.info("This info won't show")
    prod_logger.warning("Production warning message sent to S3")


def main():
    """Run all examples."""
    print("PySpark Jobs Logging Examples")
    print("=" * 50)
    
    # Run all examples
    example_basic_setup()
    example_file_logging()
    example_environment_config()
    
    print("\n=== Example 4: Execution Time Decorator ===")
    result = example_long_running_task()
    print(f"Result: {result}")
    
    example_context_logging()
    example_spark_logging()
    example_quick_setup()
    example_multiple_loggers()
    example_s3_logging()
    example_production_vs_development()
    
    print("\n" + "=" * 50)
    print("All logging examples completed!")
    print("Check the 'logs/' directory for log files.")


if __name__ == "__main__":
    main()
