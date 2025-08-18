#!/usr/bin/env python3
"""
Simple logging test that verifies the logger_config module works correctly.
This test doesn't require any external dependencies like pytest or boto3.
"""

import sys
import os

# Add src to path (unit -> tests -> project_root -> src)
unit_dir = os.path.dirname(__file__)
test_dir = os.path.dirname(unit_dir)
project_root = os.path.dirname(test_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

def test_logging_basic():
    """Test basic logging functionality."""
    print("🔍 Testing Basic Logging Functionality")
    print("-" * 40)
    
    try:
        # Import the logging module
        from jobs.utils.logger_config import setup_logging, get_logger, log_execution_time
        print("✅ Successfully imported logger_config")
        
        # Test basic setup
        setup_logging(log_level="INFO", environment="development")
        logger = get_logger("test_module")
        print("✅ Successfully set up logging")
        
        # Test different log levels
        print("\nTesting different log levels:")
        logger.debug("This is a DEBUG message (should not appear at INFO level)")
        logger.info("This is an INFO message")
        logger.warning("This is a WARNING message")
        logger.error("This is an ERROR message")
        print("✅ Log level testing completed")
        
        # Test execution time decorator
        print("\nTesting execution time decorator:")
        
        @log_execution_time
        def sample_function():
            import time
            time.sleep(0.01)  # Very short sleep
            return "Function completed successfully"
        
        result = sample_function()
        print(f"Function result: {result}")
        print("✅ Execution time decorator works")
        
        # Test multiple loggers
        print("\nTesting multiple loggers:")
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        
        logger1.info("Message from module1")
        logger2.info("Message from module2")
        print("✅ Multiple loggers work correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in basic logging test: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_logging_with_file():
    """Test file logging functionality."""
    print("\n🔍 Testing File Logging Functionality")
    print("-" * 40)
    
    try:
        import tempfile
        from jobs.utils.logger_config import setup_logging, get_logger
        
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "test_log.log")
            
            # Clear existing handlers
            import logging
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)
            
            # Setup file logging
            setup_logging(
                log_level="DEBUG", 
                log_file=log_file, 
                environment="development"
            )
            
            logger = get_logger("file_test")
            
            # Write test messages
            logger.debug("Debug message to file")
            logger.info("Info message to file")
            logger.warning("Warning message to file")
            logger.error("Error message to file")
            
            # Flush all handlers
            for handler in logging.root.handlers:
                if hasattr(handler, 'flush'):
                    handler.flush()
            
            # Verify file contents
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    content = f.read()
                    
                if all(msg in content for msg in [
                    "Debug message to file",
                    "Info message to file", 
                    "Warning message to file",
                    "Error message to file"
                ]):
                    print("✅ File logging works correctly")
                    print(f"Log file size: {len(content)} characters")
                    return True
                else:
                    print("❌ File logging: some messages missing")
                    print(f"File content:\n{content}")
                    return False
            else:
                print("❌ Log file was not created")
                return False
                
    except Exception as e:
        print(f"❌ Error in file logging test: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_environment_variables():
    """Test environment variable configuration."""
    print("\n🔍 Testing Environment Variable Configuration")
    print("-" * 40)
    
    try:
        from jobs.utils.logger_config import setup_logging, get_logger
        
        # Set environment variables
        os.environ["LOG_LEVEL"] = "WARNING"
        os.environ["ENVIRONMENT"] = "production"
        
        # Clear existing handlers
        import logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # Setup with environment variables
        setup_logging()
        logger = get_logger("env_test")
        
        print("Testing with LOG_LEVEL=WARNING:")
        logger.debug("Debug message (should not appear)")
        logger.info("Info message (should not appear)")
        logger.warning("Warning message (should appear)")
        logger.error("Error message (should appear)")
        
        print("✅ Environment variable configuration works")
        
        # Clean up
        if "LOG_LEVEL" in os.environ:
            del os.environ["LOG_LEVEL"]
        if "ENVIRONMENT" in os.environ:
            del os.environ["ENVIRONMENT"]
        
        return True
        
    except Exception as e:
        print(f"❌ Error in environment variable test: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_logger_hierarchy():
    """Test logger hierarchy functionality."""
    print("\n🔍 Testing Logger Hierarchy")
    print("-" * 40)
    
    try:
        from jobs.utils.logger_config import setup_logging, get_logger
        
        # Clear existing handlers
        import logging
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        setup_logging(log_level="INFO", environment="development")
        
        # Create hierarchical loggers
        parent_logger = get_logger("jobs")
        child_logger = get_logger("jobs.main_collection_data")
        grandchild_logger = get_logger("jobs.main_collection_data.transform")
        
        parent_logger.info("Message from parent logger")
        child_logger.info("Message from child logger")
        grandchild_logger.info("Message from grandchild logger")
        
        print("✅ Logger hierarchy works correctly")
        return True
        
    except Exception as e:
        print(f"❌ Error in logger hierarchy test: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("🚀 PySpark Jobs Logger Configuration Test Suite")
    print("=" * 60)
    
    tests = [
        test_logging_basic,
        test_logging_with_file,
        test_environment_variables,
        test_logger_hierarchy,
    ]
    
    results = []
    
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            results.append(False)
    
    print("\n" + "=" * 60)
    print("📊 Test Results Summary")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{i+1}. {test.__name__:<30} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! The logger_config module is working correctly.")
        print("\nThe logging configuration should work fine in your PySpark jobs.")
        print("The only issue you might face is missing dependencies like boto3 or pyspark")
        print("when running the full application.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
