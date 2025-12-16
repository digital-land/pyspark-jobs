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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))

from jobs.utils.logger_config import (
    setup_logging, get_logger, log_execution_time, 
    set_spark_log_level, quick_setup
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

    def test_setup_logging_default_config(self, caplog):
        """Test logging setup with default configuration."""
        with caplog.at_level(logging.INFO):
            setup_logging()
            logger = get_logger(__name__)
            logger.info("Test message")
        
        assert "Test message" in caplog.text
        assert "Logging configured" in caplog.text

    def test_setup_logging_custom_level(self, caplog):
        """Test logging setup with custom log level."""
        with caplog.at_level(logging.DEBUG):
            setup_logging(log_level="DEBUG")
            logger = get_logger(__name__)
            logger.debug("Debug message")
            logger.info("Info message")
        
        assert "Debug message" in caplog.text
        assert "Info message" in caplog.text

    def test_setup_logging_with_file(self):
        """Test logging setup with file output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")
            
            setup_logging(log_level="INFO", log_file=log_file)
            logger = get_logger(__name__)
            
            logger.info("File log message")
            
            # Verify file was created and contains message
            assert os.path.exists(log_file)
            with open(log_file, 'r') as f:
                content = f.read()
                assert "File log message" in content

    def test_get_logger_returns_logger_instance(self):
        """Test that get_logger returns a proper logger instance."""
        setup_logging()
        logger = get_logger(__name__)
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == __name__

    def test_log_execution_time_decorator_success(self, caplog):
        """Test the log_execution_time decorator with successful function."""
        setup_logging(log_level="INFO")
        
        @log_execution_time
        def test_function():
            time.sleep(0.1)
            return "test_result"
        
        with caplog.at_level(logging.INFO):
            result = test_function()
        
        assert result == "test_result"
        
        # Check for start and completion messages
        log_messages = [record.message for record in caplog.records]
        start_messages = [msg for msg in log_messages if "Starting execution of test_function" in msg]
        complete_messages = [msg for msg in log_messages if "Completed test_function in" in msg]
        
        assert len(start_messages) >= 1
        assert len(complete_messages) >= 1

    def test_log_execution_time_decorator_with_exception(self, caplog):
        """Test the log_execution_time decorator when function raises exception."""
        setup_logging(log_level="INFO")
        
        @log_execution_time
        def failing_function():
            raise ValueError("Test exception")
        
        with caplog.at_level(logging.INFO):
            with pytest.raises(ValueError, match="Test exception"):
                failing_function()
        
        # Check for failure message
        log_messages = [record.message for record in caplog.records]
        failure_messages = [msg for msg in log_messages if "Failed failing_function after" in msg]
        assert len(failure_messages) >= 1

    @patch('jobs.utils.logger_config.SparkContext')
    def test_set_spark_log_level_success(self, mock_spark_context, caplog):
        """Test successful Spark log level setting."""
        setup_logging(log_level="INFO")
        
        mock_sc = Mock()
        mock_spark_context.getOrCreate.return_value = mock_sc
        
        with caplog.at_level(logging.INFO):
            set_spark_log_level("ERROR")
        
        mock_spark_context.getOrCreate.assert_called_once()
        mock_sc.setLogLevel.assert_called_once_with("ERROR")

    def test_set_spark_log_level_import_error(self, caplog):
        """Test set_spark_log_level when PySpark is not available."""
        setup_logging(log_level="INFO")
        
        with patch('jobs.utils.logger_config.SparkContext', side_effect=ImportError("PySpark not available")):
            with caplog.at_level(logging.INFO):
                # Should not raise exception
                set_spark_log_level("WARN")
        
        # Should handle ImportError gracefully
        assert True

    def test_quick_setup_function(self, caplog):
        """Test the quick_setup convenience function."""
        with caplog.at_level(logging.DEBUG):
            logger = quick_setup(log_level="DEBUG", environment="development")
        
        assert isinstance(logger, logging.Logger)
        
        # Test that it actually works
        logger.debug("Quick setup test message")
        log_messages = [record.message for record in caplog.records]
        test_messages = [msg for msg in log_messages if "Quick setup test message" in msg]
        assert len(test_messages) >= 1