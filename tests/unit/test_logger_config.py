"""
Unit tests for logger_config module to diagnose logging issues.

This test module comprehensively tests the logging configuration to identify
what's not working and ensure it can execute locally.
"""

import pytest
import logging
import tempfile
import os
import sys
import io
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from jobs.utils.logger_config import (
    setup_logging, 
    get_logger, 
    log_execution_time, 
    set_spark_log_level,
    quick_setup
)


class TestLoggerConfig:
    """Test suite for logger configuration module."""
    
    def setup_method(self):
        """Setup for each test method."""
        # Clear any existing logging configuration
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.root.setLevel(logging.WARNING)
        
        # Reset logger cache
        logging.Logger.manager.loggerDict.clear()
    
    def test_setup_logging_basic(self, caplog):
        """Test basic logging setup with default configuration."""
        with caplog.at_level(logging.INFO):
            setup_logging(log_level="INFO", environment="development")
            logger = get_logger(__name__)
            
            # Test that logger works
            logger.info("Test info message")
            logger.warning("Test warning message")
            logger.error("Test error message")
            
        # Verify messages were logged
        assert "Test info message" in caplog.text
        assert "Test warning message" in caplog.text
        assert "Test error message" in caplog.text
        assert "Logging configured" in caplog.text
    
    def test_setup_logging_with_file(self):
        """Test logging setup with file output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")
            
            setup_logging(log_level="DEBUG", log_file=log_file, environment="development")
            logger = get_logger(__name__)
            
            # Log some messages
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")
            
            # Verify file was created and contains messages
            assert os.path.exists(log_file)
            
            with open(log_file, 'r') as f:
                log_content = f.read()
                assert "Debug message" in log_content
                assert "Info message" in log_content
                assert "Warning message" in log_content
                assert "Error message" in log_content
    
    def test_setup_logging_production_format(self, caplog):
        """Test production environment logging format."""
        with caplog.at_level(logging.INFO):
            setup_logging(log_level="INFO", environment="production")
            logger = get_logger(__name__)
            
            logger.info("Production test message")
            
        # Production format should not include line numbers
        log_records = [record for record in caplog.records if "Production test message" in record.message]
        assert len(log_records) > 0
    
    def test_setup_logging_development_format(self, caplog):
        """Test development environment logging format."""
        with caplog.at_level(logging.INFO):
            setup_logging(log_level="DEBUG", environment="development")
            logger = get_logger(__name__)
            
            logger.info("Development test message")
            
        # Should work without errors
        assert len(caplog.records) > 0
    
    def test_get_logger_returns_logger_instance(self):
        """Test that get_logger returns a proper logger instance."""
        setup_logging()
        logger = get_logger(__name__)
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == __name__
    
    def test_get_logger_different_names(self):
        """Test that get_logger returns different loggers for different names."""
        setup_logging()
        logger1 = get_logger("test.module1")
        logger2 = get_logger("test.module2")
        
        assert logger1.name == "test.module1"
        assert logger2.name == "test.module2"
        assert logger1 is not logger2
    
    def test_log_execution_time_decorator(self, caplog):
        """Test the log_execution_time decorator."""
        setup_logging(log_level="INFO")
        
        @log_execution_time
        def test_function():
            import time
            time.sleep(0.1)  # Small delay to test timing
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
    
    def test_set_spark_log_level_without_spark(self, caplog):
        """Test set_spark_log_level when PySpark is not available."""
        setup_logging(log_level="INFO")
        
        with patch('jobs.utils.logger_config.SparkContext') as mock_spark:
            mock_spark.side_effect = ImportError("PySpark not available")
            
            with caplog.at_level(logging.INFO):
                # Should not raise exception
                set_spark_log_level("WARN")
            
            # Should handle ImportError gracefully
            assert True  # If we get here, no exception was raised
    
    def test_set_spark_log_level_with_mock_spark(self, caplog):
        """Test set_spark_log_level with mocked Spark context."""
        setup_logging(log_level="INFO")
        
        with patch('jobs.utils.logger_config.SparkContext') as mock_spark_class:
            mock_sc = MagicMock()
            mock_spark_class.getOrCreate.return_value = mock_sc
            
            with caplog.at_level(logging.INFO):
                set_spark_log_level("ERROR")
            
            # Verify Spark context was called
            mock_spark_class.getOrCreate.assert_called_once()
            mock_sc.setLogLevel.assert_called_once_with("ERROR")
            
            # Check log message
            log_messages = [record.message for record in caplog.records]
            spark_messages = [msg for msg in log_messages if "Spark log level set to: ERROR" in msg]
            assert len(spark_messages) >= 1
    
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
    
    def test_environment_variables_configuration(self, monkeypatch, caplog):
        """Test configuration via environment variables."""
        # Set environment variables
        monkeypatch.setenv("LOG_LEVEL", "WARNING")
        monkeypatch.setenv("ENVIRONMENT", "production")
        
        with caplog.at_level(logging.WARNING):
            setup_logging()  # Should use environment variables
            logger = get_logger(__name__)
            
            logger.debug("Debug message")  # Should not appear
            logger.info("Info message")   # Should not appear
            logger.warning("Warning message")  # Should appear
            logger.error("Error message")      # Should appear
        
        log_messages = [record.message for record in caplog.records]
        
        # Debug and info should not appear
        debug_messages = [msg for msg in log_messages if "Debug message" in msg]
        info_messages = [msg for msg in log_messages if "Info message" in msg]
        assert len(debug_messages) == 0
        assert len(info_messages) == 0
        
        # Warning and error should appear
        warning_messages = [msg for msg in log_messages if "Warning message" in msg]
        error_messages = [msg for msg in log_messages if "Error message" in msg]
        assert len(warning_messages) >= 1
        assert len(error_messages) >= 1
    
    def test_log_directory_creation(self):
        """Test that log directories are created automatically."""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_log_path = os.path.join(temp_dir, "logs", "nested", "test.log")
            
            # Directory should not exist initially
            assert not os.path.exists(os.path.dirname(nested_log_path))
            
            setup_logging(log_file=nested_log_path)
            logger = get_logger(__name__)
            logger.info("Test message")
            
            # Directory should be created
            assert os.path.exists(os.path.dirname(nested_log_path))
            assert os.path.exists(nested_log_path)
    
    def test_multiple_setup_calls(self, caplog):
        """Test that multiple setup_logging calls don't break logging."""
        with caplog.at_level(logging.INFO):
            # First setup
            setup_logging(log_level="INFO", environment="development")
            logger = get_logger(__name__)
            logger.info("First setup message")
            
            # Second setup
            setup_logging(log_level="DEBUG", environment="development")
            logger.info("Second setup message")
            
            # Third setup
            setup_logging(log_level="WARNING", environment="production")
            logger.warning("Third setup message")
        
        log_messages = [record.message for record in caplog.records]
        
        # All messages should be present
        first_messages = [msg for msg in log_messages if "First setup message" in msg]
        second_messages = [msg for msg in log_messages if "Second setup message" in msg]
        third_messages = [msg for msg in log_messages if "Third setup message" in msg]
        
        assert len(first_messages) >= 1
        assert len(second_messages) >= 1
        assert len(third_messages) >= 1
    
    def test_logger_hierarchy(self, caplog):
        """Test that logger hierarchy works correctly."""
        setup_logging(log_level="DEBUG")
        
        # Create parent and child loggers
        parent_logger = get_logger("parent")
        child_logger = get_logger("parent.child")
        grandchild_logger = get_logger("parent.child.grandchild")
        
        with caplog.at_level(logging.DEBUG):
            parent_logger.info("Parent message")
            child_logger.info("Child message")
            grandchild_logger.info("Grandchild message")
        
        log_messages = [record.message for record in caplog.records]
        
        assert any("Parent message" in msg for msg in log_messages)
        assert any("Child message" in msg for msg in log_messages)
        assert any("Grandchild message" in msg for msg in log_messages)
    
    def test_import_from_package(self):
        """Test that the module can be imported as expected."""
        # This test verifies the import structure works
        try:
            from jobs.utils.logger_config import setup_logging, get_logger
            assert callable(setup_logging)
            assert callable(get_logger)
        except ImportError as e:
            pytest.fail(f"Failed to import logger_config: {e}")
    
    def test_logging_with_real_pyspark_job_simulation(self, caplog):
        """Test logging in a simulated PySpark job environment."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "pyspark_job.log")
            
            # Simulate main_collection_data.py setup
            setup_logging(
                log_level="INFO",
                log_file=log_file,
                environment="development"
            )
            
            # Simulate different modules logging
            main_logger = get_logger("jobs.main_collection_data")
            transform_logger = get_logger("jobs.transform_collection_data")
            postgres_logger = get_logger("jobs.dbaccess.postgres_connectivity")
            
            with caplog.at_level(logging.INFO):
                main_logger.info("Starting ETL process")
                transform_logger.info("Transforming data for fact table")
                postgres_logger.info("Writing to PostgreSQL database")
                main_logger.info("ETL process completed")
            
            # Verify console logging
            log_messages = [record.message for record in caplog.records]
            assert any("Starting ETL process" in msg for msg in log_messages)
            assert any("Transforming data for fact table" in msg for msg in log_messages)
            assert any("Writing to PostgreSQL database" in msg for msg in log_messages)
            assert any("ETL process completed" in msg for msg in log_messages)
            
            # Verify file logging
            assert os.path.exists(log_file)
            with open(log_file, 'r') as f:
                file_content = f.read()
                assert "Starting ETL process" in file_content
                assert "Transforming data for fact table" in file_content
                assert "Writing to PostgreSQL database" in file_content
                assert "ETL process completed" in file_content


class TestLoggerConfigExecutable:
    """Test that the logger_config module is executable as a script."""
    
    def test_module_executable_directly(self, capsys):
        """Test that running the module directly works."""
        # Test the if __name__ == "__main__" block
        import subprocess
        import sys
        
        # Get the path to the logger_config module
        module_path = os.path.join(
            os.path.dirname(__file__), '..', '..', 'src', 'jobs', 'utils', 'logger_config.py'
        )
        
        try:
            result = subprocess.run(
                [sys.executable, module_path],
                capture_output=True,
                text=True,
                timeout=10  # 10 second timeout
            )
            
            # Should execute without errors
            assert result.returncode == 0
            
            # Should produce some output
            assert "Testing simplified logging configuration" in result.stdout
            assert "Logging test completed successfully" in result.stdout
            
        except subprocess.TimeoutExpired:
            pytest.fail("Module execution timed out")
        except Exception as e:
            pytest.fail(f"Failed to execute module directly: {e}")


class TestLoggerConfigDiagnostics:
    """Diagnostic tests to identify specific logging issues."""
    
    def test_python_logging_module_available(self):
        """Test that Python's logging module is available and working."""
        import logging
        
        # Basic logging should work
        logger = logging.getLogger("test")
        logger.setLevel(logging.DEBUG)
        
        # Create a handler
        handler = logging.StreamHandler()
        logger.addHandler(handler)
        
        # Should not raise exception
        logger.info("Test message")
        
        # Clean up
        logger.removeHandler(handler)
    
    def test_pathlib_available(self):
        """Test that pathlib is available for log directory creation."""
        from pathlib import Path
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = Path(temp_dir) / "test" / "nested" / "path"
            test_path.mkdir(parents=True, exist_ok=True)
            assert test_path.exists()
    
    def test_environment_variables_accessible(self):
        """Test that environment variables can be read."""
        import os
        
        # Should be able to read environment variables
        default_value = os.getenv("NONEXISTENT_VAR", "default")
        assert default_value == "default"
        
        # Test setting and reading
        os.environ["TEST_VAR"] = "test_value"
        assert os.getenv("TEST_VAR") == "test_value"
        
        # Clean up
        del os.environ["TEST_VAR"]
    
    def test_sys_stdout_accessible(self):
        """Test that sys.stdout is accessible for console logging."""
        import sys
        import io
        
        # Should be able to access sys.stdout
        assert hasattr(sys, 'stdout')
        assert hasattr(sys.stdout, 'write')
        
        # Should be able to redirect stdout
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        print("Test output")
        output = sys.stdout.getvalue()
        sys.stdout = old_stdout
        
        assert "Test output" in output


def test_logger_config_comprehensive():
    """Comprehensive test that exercises all major functionality."""
    import tempfile
    import time
    
    with tempfile.TemporaryDirectory() as temp_dir:
        log_file = os.path.join(temp_dir, "comprehensive_test.log")
        
        # Test complete workflow
        setup_logging(
            log_level="DEBUG",
            log_file=log_file,
            environment="development"
        )
        
        # Get loggers for different modules
        main_logger = get_logger("main")
        utils_logger = get_logger("utils")
        
        # Test basic logging
        main_logger.debug("Debug message from main")
        main_logger.info("Info message from main")
        main_logger.warning("Warning message from main")
        main_logger.error("Error message from main")
        
        utils_logger.info("Message from utils")
        
        # Test execution time decorator
        @log_execution_time
        def test_decorated_function():
            time.sleep(0.01)  # Very short sleep
            return "success"
        
        result = test_decorated_function()
        assert result == "success"
        
        # Test file logging worked
        assert os.path.exists(log_file)
        
        with open(log_file, 'r') as f:
            content = f.read()
            assert "Debug message from main" in content
            assert "Info message from main" in content
            assert "Warning message from main" in content
            assert "Error message from main" in content
            assert "Message from utils" in content
            assert "Starting execution of test_decorated_function" in content
            assert "Completed test_decorated_function" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
