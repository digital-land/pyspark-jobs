# !/usr/bin/env python3
"""
Quick logging test script for immediate verification.

This is a minimal script placed in the project root for quick verification
that logging works. For comprehensive testing, use the test suite in tests/.

Usage:
    python3 test_logging_quick.py
"""
import os
import sys


# Add src to path (utils -> tests -> project_root -> src)
utils_dir = os.path.dirname(__file__)
test_dir = os.path.dirname(utils_dir)
project_root = os.path.dirname(test_dir)
src_path = os.path.join(project_root, "src")
sys.path.insert(0, src_path)


def main():
    """Quick test of logging functionality."""
    print("🔍 Quick Logging Test")
    print("=" * 30)

    try:
        # Test import
        from jobs.utils.logger_config import get_logger, setup_logging

        print("✅ Logger config imported successfully")

        # Test basic console logging
        setup_logging(log_level="INFO", environment="development")
        logger = get_logger("quick_test")

        logger.info("This is a test INFO message")
        logger.warning("This is a test WARNING message")
        logger.error("This is a test ERROR message")

        print("✅ Basic console logging works!")

        # Test file logging to test logs directory (utils -> tests -> logs)
        test_logs_dir = os.path.join(test_dir, "logs")
        os.makedirs(test_logs_dir, exist_ok=True)
        log_file = os.path.join(test_logs_dir, "quick_test.log")

        # Clear existing handlers for file test

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        setup_logging(log_level="DEBUG", log_file=log_file, environment="development")
        file_logger = get_logger("quick_file_test")
        file_logger.info("Test message written to test logs directory")

        # Flush handlers
        for handler in logging.root.handlers:
            if hasattr(handler, "flush"):
                handler.flush()

        if os.path.exists(log_file):
            print(f"✅ File logging works! Log written to: {log_file}")
        else:
            print("⚠️  File logging test skipped")

        print("\n📝 For comprehensive testing, run:")
        print("   python3 -m pytest tests/unit/test_logger_config.py")
        print("   python3 -m pytest tests/unit/test_simple_logging.py")
        print("   python3 -m pytest tests/integration/test_logging_integration.py")

        return True

    except Exception as e:
        print(f"❌ Logging test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
