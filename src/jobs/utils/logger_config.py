"""
Centralized logging configuration module for PySpark jobs.

This module provides a flexible logging configuration that can be used across
all PySpark jobs and utilities. It supports different logging levels, formats,
and output destinations based on environment settings.

Usage:
    from jobs.utils.logger_config import get_logger, setup_logging
    
    # Setup logging configuration (call once at application start)
    setup_logging()
    
    # Get a logger for your module
    logger = get_logger(__name__)
    logger.info("This is an info message")
"""

import logging
import logging.config
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import boto3
from datetime import datetime
import threading
import queue
import time


class S3LogHandler(logging.Handler):
    """
    Custom logging handler that uploads log messages to Amazon S3.
    
    This handler buffers log messages and uploads them to S3 periodically
    or when the buffer reaches a certain size.
    """
    
    def __init__(self, s3_bucket: str, s3_key_prefix: str = "logs", 
                 buffer_size: int = 100, flush_interval: int = 60,
                 level: int = logging.NOTSET):
        """
        Initialize the S3 log handler.
        
        Args:
            s3_bucket: S3 bucket name (without arn: prefix)
            s3_key_prefix: Prefix for S3 object keys
            buffer_size: Number of log records to buffer before uploading
            flush_interval: Time in seconds between automatic flushes
            level: Minimum log level to handle
        """
        super().__init__(level)
        
        # Extract bucket name from ARN if provided
        if s3_bucket.startswith("arn:aws:s3:::"):
            self.s3_bucket = s3_bucket.replace("arn:aws:s3:::", "")
        else:
            self.s3_bucket = s3_bucket
            
        self.s3_key_prefix = s3_key_prefix
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client('s3')
        except Exception as e:
            # Fall back to console logging if S3 is not available
            print(f"Warning: Could not initialize S3 client: {e}")
            print("S3 logging will be disabled")
            self.s3_client = None
        
        # Buffer for log records
        self.buffer = queue.Queue()
        self.last_flush = time.time()
        
        # Start background thread for periodic flushing
        self._stop_event = threading.Event()
        self._flush_thread = threading.Thread(target=self._flush_periodically, daemon=True)
        self._flush_thread.start()
    
    def emit(self, record):
        """
        Emit a log record to the buffer.
        """
        if self.s3_client is None:
            return
            
        try:
            # Format the record
            msg = self.format(record)
            
            # Add to buffer
            self.buffer.put({
                'timestamp': datetime.utcnow().isoformat(),
                'level': record.levelname,
                'logger': record.name,
                'message': msg
            })
            
            # Check if we need to flush
            if self.buffer.qsize() >= self.buffer_size:
                self._flush_buffer()
                
        except Exception:
            self.handleError(record)
    
    def _flush_periodically(self):
        """
        Background thread that flushes the buffer periodically.
        """
        while not self._stop_event.is_set():
            time.sleep(self.flush_interval)
            if time.time() - self.last_flush >= self.flush_interval:
                self._flush_buffer()
    
    def _flush_buffer(self):
        """
        Flush the buffer to S3.
        """
        if self.s3_client is None or self.buffer.empty():
            return
            
        try:
            # Collect all buffered messages
            messages = []
            while not self.buffer.empty():
                try:
                    messages.append(self.buffer.get_nowait())
                except queue.Empty:
                    break
            
            if not messages:
                return
                
            # Create log content
            log_content = "\n".join([
                f"[{msg['timestamp']}] {msg['level']} - {msg['logger']} - {msg['message']}"
                for msg in messages
            ]) + "\n"
            
            # Generate S3 key
            timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H")
            job_id = os.getenv("EMR_JOB_ID", "unknown")
            s3_key = f"{self.s3_key_prefix}/{timestamp}/job-{job_id}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.log"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=log_content.encode('utf-8'),
                ContentType='text/plain'
            )
            
            self.last_flush = time.time()
            
        except Exception as e:
            # Don't raise exceptions from logging handler
            print(f"Failed to upload logs to S3: {e}")
    
    def flush(self):
        """
        Flush any remaining log records to S3.
        """
        self._flush_buffer()
    
    def close(self):
        """
        Close the handler and flush any remaining records.
        """
        self._stop_event.set()
        self._flush_buffer()
        super().close()


def get_logging_config(
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    log_file: Optional[str] = None,
    enable_console: bool = True,
    enable_file: bool = False,
    enable_s3: bool = False,
    s3_bucket: Optional[str] = None,
    s3_key_prefix: str = "logs",
    environment: str = "development"
) -> Dict[str, Any]:
    """
    Generate logging configuration dictionary.
    
    Args:
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format (str, optional): Custom log format string
        log_file (str, optional): Path to log file
        enable_console (bool): Enable console logging
        enable_file (bool): Enable file logging
        enable_s3 (bool): Enable S3 logging
        s3_bucket (str, optional): S3 bucket name or ARN
        s3_key_prefix (str): S3 key prefix for log files
        environment (str): Environment name (development, staging, production)
    
    Returns:
        Dict[str, Any]: Logging configuration dictionary
    """
    
    # Default format based on environment
    if log_format is None:
        if environment.lower() == "production":
            log_format = "[%(asctime)s] %(levelname)s - %(name)s - %(message)s"
        else:
            log_format = "[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s"
    
    # Create formatters
    formatters = {
        "default": {
            "format": log_format,
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "detailed": {
            "format": "[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(funcName)s() - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    }
    
    # Create handlers
    handlers = {}
    root_handlers = []
    
    if enable_console:
        handlers["console"] = {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": log_level,
            "stream": "ext://sys.stdout"
        }
        root_handlers.append("console")
    
    if enable_file and log_file:
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "detailed",
            "level": log_level,
            "filename": log_file,
            "mode": "a",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
            "encoding": "utf-8"
        }
        root_handlers.append("file")
    
    if enable_s3 and s3_bucket:
        handlers["s3"] = {
            "class": "jobs.utils.logger_config.S3LogHandler",
            "formatter": "detailed",
            "level": log_level,
            "s3_bucket": s3_bucket,
            "s3_key_prefix": s3_key_prefix,
            "buffer_size": 50,  # Buffer 50 log messages before uploading
            "flush_interval": 30  # Upload every 30 seconds
        }
        root_handlers.append("s3")
    
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": formatters,
        "handlers": handlers,
        "root": {
            "handlers": root_handlers,
            "level": log_level,
        },
        "loggers": {
            # Reduce noise from third-party libraries
            "boto3": {"level": "WARNING"},
            "botocore": {"level": "WARNING"},
            "urllib3": {"level": "WARNING"},
            "py4j": {"level": "WARNING"},
            "pyspark": {"level": "WARNING"}
        }
    }


def setup_logging(
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_file: Optional[str] = None,
    enable_console: Optional[bool] = None,
    enable_file: Optional[bool] = None,
    enable_s3: Optional[bool] = None,
    s3_bucket: Optional[str] = None,
    s3_key_prefix: Optional[str] = None,
    environment: Optional[str] = None
) -> None:
    """
    Setup logging configuration for the application.
    
    This function configures logging based on environment variables with
    sensible defaults. Call this once at the start of your application.
    
    Args:
        log_level (str, optional): Override default log level
        log_format (str, optional): Override default log format
        log_file (str, optional): Override default log file path
        enable_console (bool, optional): Override console logging setting
        enable_file (bool, optional): Override file logging setting
        enable_s3 (bool, optional): Override S3 logging setting
        s3_bucket (str, optional): Override S3 bucket
        s3_key_prefix (str, optional): Override S3 key prefix
        environment (str, optional): Override environment setting
    
    Environment Variables:
        LOG_LEVEL: Logging level (default: INFO)
        LOG_FORMAT: Custom log format string
        LOG_FILE: Path to log file
        LOG_CONSOLE: Enable console logging (true/false, default: true)
        LOG_FILE_ENABLED: Enable file logging (true/false, default: false)
        LOG_S3_ENABLED: Enable S3 logging (true/false, default: false)
        LOG_S3_BUCKET: S3 bucket name or ARN
        LOG_S3_KEY_PREFIX: S3 key prefix (default: logs)
        ENVIRONMENT: Environment name (default: development)
    """
    
    # Get configuration from environment variables with defaults
    log_level = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_format = log_format or os.getenv("LOG_FORMAT")
    log_file = log_file or os.getenv("LOG_FILE")
    s3_bucket = s3_bucket or os.getenv("LOG_S3_BUCKET")
    s3_key_prefix = s3_key_prefix or os.getenv("LOG_S3_KEY_PREFIX", "logs")
    environment = environment or os.getenv("ENVIRONMENT", "development")
    
    # Handle boolean environment variables
    if enable_console is None:
        enable_console = os.getenv("LOG_CONSOLE", "true").lower() in ("true", "1", "yes")
    
    if enable_file is None:
        enable_file = os.getenv("LOG_FILE_ENABLED", "false").lower() in ("true", "1", "yes")
    
    if enable_s3 is None:
        enable_s3 = os.getenv("LOG_S3_ENABLED", "false").lower() in ("true", "1", "yes")
    
    # Default log file path if file logging is enabled but no path specified
    if enable_file and not log_file:
        log_file = "logs/pyspark_job.log"
    
    # Generate and apply configuration
    config = get_logging_config(
        log_level=log_level,
        log_format=log_format,
        log_file=log_file,
        enable_console=enable_console,
        enable_file=enable_file,
        enable_s3=enable_s3,
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix,
        environment=environment
    )
    
    logging.config.dictConfig(config)
    
    # Log the configuration setup (only to console to avoid circular logging)
    root_logger = logging.getLogger()
    if enable_console:
        root_logger.info(f"Logging configured - Level: {log_level}, Environment: {environment}")
        if enable_file and log_file:
            root_logger.info(f"File logging enabled: {log_file}")
        if enable_s3 and s3_bucket:
            root_logger.info(f"S3 logging enabled: {s3_bucket}/{s3_key_prefix}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified name.
    
    Args:
        name (str): Logger name (typically __name__)
    
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


def set_spark_log_level(log_level: str = "WARN") -> None:
    """
    Set Spark-specific logging level to reduce verbosity.
    
    Args:
        log_level (str): Spark log level (OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL)
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


def log_execution_time(func):
    """
    Decorator to log function execution time.
    
    Usage:
        @log_execution_time
        def my_function():
            # function code
    """
    import functools
    import time
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = time.time()
        
        try:
            logger.info(f"Starting execution of {func.__name__}")
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"Completed {func.__name__} in {duration:.2f} seconds")
            return result
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"Failed {func.__name__} after {duration:.2f} seconds: {e}")
            raise
    
    return wrapper


class LogContext:
    """
    Context manager for adding contextual information to logs.
    
    Usage:
        with LogContext({"job_id": "123", "dataset": "transport"}):
            logger.info("Processing data")  # Will include context
    """
    
    def __init__(self, context: Dict[str, Any]):
        self.context = context
        self.original_factory = logging.getLogRecordFactory()
    
    def __enter__(self):
        def record_factory(*args, **kwargs):
            record = self.original_factory(*args, **kwargs)
            for key, value in self.context.items():
                setattr(record, key, value)
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.setLogRecordFactory(self.original_factory)


# Convenience function for quick setup
def quick_setup(log_level: str = "INFO", environment: str = "development") -> logging.Logger:
    """
    Quick logging setup for simple scripts.
    
    Args:
        log_level (str): Logging level
        environment (str): Environment name
    
    Returns:
        logging.Logger: Root logger
    """
    setup_logging(log_level=log_level, environment=environment)
    return get_logger(__name__)


if __name__ == "__main__":
    # Example usage and testing
    print("Testing logging configuration...")
    
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
        import time
        time.sleep(1)
        return "completed"
    
    result = test_function()
    
    # Test context manager
    with LogContext({"job_id": "test_123", "dataset": "sample"}):
        logger.info("Processing with context")
    
    print("Logging test completed successfully!")
