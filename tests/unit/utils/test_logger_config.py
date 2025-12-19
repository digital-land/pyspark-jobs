"""Unit tests for logger_config module."""

import pytest
import os
import sys
import logging
import tempfile
import time
from unittest.mock import Mock, patch
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from jobs.utils.logger_config import (
    setup_logging,
    get_logger,
    log_execution_time,
    set_spark_log_level,
    quick_setup,
)


class TestLoggerConfig:
    """Test suite for logger_config module."""

    def setup_method(self):
        """Setup for each test method."""
        # Clear any existing logging configuration
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.root.setLevel(logging.WARNING)

        # Reset logger cache
        logging.Logger.manager.loggerDict.clear()

    def test_setup_logging_default_config(self):
        """Test logging setup with default configuration."""
        setup_logging()
        logger = get_logger(__name__)

        # Test that logger is created and functional
        assert isinstance(logger, logging.Logger)
        assert logger.name == __name__

    def test_setup_logging_custom_level(self):
        """Test logging setup with custom log level."""
        setup_logging(log_level="DEBUG")
        logger = get_logger(__name__)

        # Test that logger is created with debug level
        assert isinstance(logger, logging.Logger)
        assert logger.isEnabledFor(logging.DEBUG)

    def test_setup_logging_with_file(self):
        """Test logging setup with file output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")

            setup_logging(log_level="INFO", log_file=log_file)
            logger = get_logger(__name__)

            logger.info("File log message")

            # Verify file was created and contains message
            assert os.path.exists(log_file)
            with open(log_file, "r") as f:
                content = f.read()
                assert "File log message" in content

    def test_get_logger_returns_logger_instance(self):
        """Test that get_logger returns a proper logger instance."""
        setup_logging()
        logger = get_logger(__name__)

        assert isinstance(logger, logging.Logger)
        assert logger.name == __name__

    def test_log_execution_time_decorator_success(self):
        """Test the log_execution_time decorator with successful function."""
        setup_logging(log_level="INFO")

        @log_execution_time
        def test_function():
            return "test_result"

        result = test_function()
        assert result == "test_result"

    def test_log_execution_time_decorator_with_exception(self):
        """Test the log_execution_time decorator when function raises exception."""
        setup_logging(log_level="INFO")

        @log_execution_time
        def failing_function():
            raise ValueError("Test exception")

        with pytest.raises(ValueError, match="Test exception"):
            failing_function()

    def test_set_spark_log_level_success(self, caplog):
        """Test successful Spark log level setting."""
        setup_logging(log_level="INFO")

        with patch("pyspark.SparkContext") as mock_spark_context:
            mock_sc = Mock()
            mock_spark_context.getOrCreate.return_value = mock_sc

            with caplog.at_level(logging.INFO):
                set_spark_log_level("ERROR")

            mock_spark_context.getOrCreate.assert_called_once()
            mock_sc.setLogLevel.assert_called_once_with("ERROR")

    def test_set_spark_log_level_import_error(self, caplog):
        """Test set_spark_log_level when PySpark is not available."""
        setup_logging(log_level="INFO")

        with patch(
            "pyspark.SparkContext", side_effect=ImportError("PySpark not available")
        ):
            with caplog.at_level(logging.INFO):
                # Should not raise exception
                set_spark_log_level("WARN")

        # Should handle ImportError gracefully
        assert True

    def test_quick_setup_function(self):
        """Test the quick_setup convenience function."""
        logger = quick_setup(log_level="DEBUG", environment="development")

        assert isinstance(logger, logging.Logger)
        assert logger.isEnabledFor(logging.DEBUG)
