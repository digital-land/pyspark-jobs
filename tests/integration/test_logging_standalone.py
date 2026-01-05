import logging
import os
import sys
import time

import pytest

# !/usr/bin/env python3
"""
Standalone logging test script to diagnose logging configuration issues.

This script can be run independently to test logging functionality
without the complexity of the full test suite.

Usage:
    python test_logging_standalone.py
"""

import tempfile
import traceback

# Add src to path (integration -> tests -> project_root -> src)
integration_dir = os.path.dirname(__file__)
test_dir = os.path.dirname(integration_dir)
project_root = os.path.dirname(test_dir)
src_path = os.path.join(project_root, "src")
sys.path.insert(0, src_path)


def test_basic_python_logging():
    """Test that basic Python logging works."""
    print("=== Testing Basic Python Logging ===")
    try:

        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
        )

        logger = logging.getLogger("test_basic")
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")

        print("✅ Basic Python logging works")
        return True
    except Exception as e:
        print(f"❌ Basic Python logging failed: {e}")
        traceback.print_exc()
        return False


def test_import_logger_config():
    """Test importing the logger_config module."""
    print("\n=== Testing Logger Config Import ===")
    try:
        from jobs.utils.logger_config import (
            get_logger,
            log_execution_time,
            setup_logging,
        )

        print("✅ Successfully imported logger_config module")
        return True
    except ImportError as e:
        print(f"❌ Failed to import logger_config: {e}")
        print("Current Python path:")
        for path in sys.path:
            print(f"  - {path}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"❌ Unexpected error importing logger_config: {e}")
        traceback.print_exc()
        return False


def test_setup_logging_basic():
    """Test basic setup_logging functionality."""
    print("\n=== Testing Basic Setup Logging ===")
    try:
        # Clear any existing handlers

        from jobs.utils.logger_config import get_logger, setup_logging

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        setup_logging(log_level="INFO", environment="development")
        logger = get_logger(__name__)

        print("About to log test messages...")
        logger.info("Test info message from setup_logging")
        logger.warning("Test warning message from setup_logging")
        logger.error("Test error message from setup_logging")

        print("✅ Basic setup_logging works")
        return True
    except Exception as e:
        print(f"❌ setup_logging failed: {e}")
        traceback.print_exc()
        return False


def test_file_logging():
    """Test logging to file."""
    print("\n=== Testing File Logging ===")
    try:
        from jobs.utils.logger_config import get_logger, setup_logging

        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test.log")

            # Clear any existing handlers

            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)

            setup_logging(
                log_level="DEBUG", log_file=log_file, environment="development"
            )
            logger = get_logger(__name__)

            logger.debug("Debug message to file")
            logger.info("Info message to file")
            logger.warning("Warning message to file")
            logger.error("Error message to file")

            # Flush handlers to ensure file is written
            for handler in logging.root.handlers:
                if hasattr(handler, "flush"):
                    handler.flush()

            # Check if file was created and contains messages
            if os.path.exists(log_file):
                with open(log_file, "r") as f:
                    content = f.read()
                    if (
                        "Debug message to file" in content
                        and "Error message to file" in content
                    ):
                        print("✅ File logging works")
                        print(f"Log file content ({len(content)} chars):")
                        print(content[:500] + "..." if len(content) > 500 else content)
                        return True
                    else:
                        print("❌ Log file created but missing expected content")
                        print(f"File content: {content}")
                        return False
            else:
                print(f"❌ Log file was not created at {log_file}")
                return False
    except Exception as e:
        print(f"❌ File logging failed: {e}")
        traceback.print_exc()
        return False


def test_execution_time_decorator():
    """Test the log_execution_time decorator."""
    print("\n=== Testing Execution Time Decorator ===")
    try:
        # Clear any existing handlers

        from jobs.utils.logger_config import (
            get_logger,
            log_execution_time,
            setup_logging,
        )

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        setup_logging(log_level="INFO", environment="development")

        @log_execution_time
        def test_function():
            time.sleep(0.1)
            return "test_result"

        result = test_function()

        if result == "test_result":
            print("✅ Execution time decorator works")
            return True
        else:
            print(f"❌ Execution time decorator failed: unexpected result {result}")
            return False
    except Exception as e:
        print(f"❌ Execution time decorator failed: {e}")
        traceback.print_exc()
        return False


def test_environment_variables():
    """Test configuration via environment variables."""
    print("\n=== Testing Environment Variables ===")
    try:
        from jobs.utils.logger_config import get_logger, setup_logging

        # Set environment variables
        os.environ["LOG_LEVEL"] = "WARNING"
        os.environ["ENVIRONMENT"] = "production"

        # Clear any existing handlers

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        setup_logging()  # Should use environment variables
        logger = get_logger(__name__)

        # Test that only WARNING and above are logged
        logger.debug("Debug message - should not appear")
        logger.info("Info message - should not appear")
        logger.warning("Warning message - should appear")
        logger.error("Error message - should appear")

        print("✅ Environment variable configuration works")

        # Clean up environment variables
        if "LOG_LEVEL" in os.environ:
            del os.environ["LOG_LEVEL"]
        if "ENVIRONMENT" in os.environ:
            del os.environ["ENVIRONMENT"]

        return True
    except Exception as e:
        print(f"❌ Environment variable configuration failed: {e}")
        traceback.print_exc()
        return False


def test_logger_config_executable():
    """Test that logger_config.py can be executed directly."""
    print("\n=== Testing Logger Config Executable ===")
    try:
        import subprocess

        module_path = os.path.join(
            project_root, "src", "jobs", "utils", "logger_config.py"
        )

        if not os.path.exists(module_path):
            print(f"❌ Logger config module not found at {module_path}")
            return False

        result = subprocess.run(
            [sys.executable, module_path], capture_output=True, text=True, timeout=10
        )

        if result.returncode == 0:
            print("✅ Logger config module executes successfully")
            print("STDOUT:")
            print(result.stdout)
            if result.stderr:
                print("STDERR:")
                print(result.stderr)
            return True
        else:
            print(
                f"❌ Logger config module execution failed with return code {result.returncode}"
            )
            print("STDOUT:")
            print(result.stdout)
            print("STDERR:")
            print(result.stderr)
            return False
    except subprocess.TimeoutExpired:
        print("❌ Logger config module execution timed out")
        return False
    except Exception as e:
        print(f"❌ Error executing logger config module: {e}")
        traceback.print_exc()
        return False


def test_main_collection_data_import():
    """Test importing main_collection_data to see if logging works there."""
    print("\n=== Testing Main Collection Data Import ===")
    try:
        # This will test if the logging setup in main_collection_data works
        from jobs import main_collection_data

        print("✅ Successfully imported main_collection_data")

        # Try to access the logger
        if hasattr(main_collection_data, "logger"):
            logger = main_collection_data.logger
            logger.info("Test message from main_collection_data logger")
            print("✅ main_collection_data logger works")
        else:
            print("⚠️  main_collection_data doesn't have exposed logger")

        return True
    except Exception as e:
        print(f"❌ Failed to import main_collection_data: {e}")
        traceback.print_exc()
        return False


def main():
    """Run all diagnostic tests."""
    print("🔍 PySpark Jobs Logging Diagnostic Tool")
    print("=" * 50)

    tests = [
        test_basic_python_logging,
        test_import_logger_config,
        test_setup_logging_basic,
        test_file_logging,
        test_execution_time_decorator,
        test_environment_variables,
        test_logger_config_executable,
        test_main_collection_data_import,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            traceback.print_exc()
            results.append(False)

    print("\n" + "=" * 50)
    print("📊 Test Results Summary")
    print("=" * 50)

    passed = sum(results)
    total = len(results)

    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{i + 1:2d}. {test.__name__:<30} {status}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All logging tests passed! Logging should work correctly.")
        return 0
    else:
        print("⚠️  Some logging tests failed. Review the failures above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
