"""
Test utilities for PySpark Jobs test suite.

This module provides common utilities and helpers for all test types.
"""

import os
import tempfile
import shutil
from pathlib import Path
from typing import Optional


def get_test_logs_dir() -> str:
    """
    Get the test logs directory path.
    
    Returns:
        str: Absolute path to the test logs directory
    """
    # Get the tests directory (parent of utils directory)
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    test_dir = os.path.dirname(utils_dir)
    logs_dir = os.path.join(test_dir, 'logs')
    
    # Ensure directory exists
    os.makedirs(logs_dir, exist_ok=True)
    
    return logs_dir


def get_test_log_file(test_name: str, suffix: str = 'log') -> str:
    """
    Get a test log file path with automatic naming.
    
    Args:
        test_name: Name of the test (usually __name__ or function name)
        suffix: File extension (default: 'log')
    
    Returns:
        str: Full path to the test log file
    """
    logs_dir = get_test_logs_dir()
    
    # Clean test name for filename
    clean_name = test_name.replace('.', '_').replace(':', '_')
    filename = f"{clean_name}.{suffix}"
    
    return os.path.join(logs_dir, filename)


def cleanup_test_logs(pattern: Optional[str] = None) -> int:
    """
    Clean up test log files.
    
    Args:
        pattern: Optional glob pattern to match specific files
                If None, cleans all .log files
    
    Returns:
        int: Number of files deleted
    """
    import glob
    
    logs_dir = get_test_logs_dir()
    
    if pattern:
        search_pattern = os.path.join(logs_dir, pattern)
    else:
        search_pattern = os.path.join(logs_dir, "*.log")
    
    files_to_delete = glob.glob(search_pattern)
    
    deleted_count = 0
    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            deleted_count += 1
        except OSError:
            pass  # File might already be deleted
    
    return deleted_count


def create_temp_log_file(prefix: str = "test_", suffix: str = ".log", 
                        in_test_logs: bool = True) -> str:
    """
    Create a temporary log file for testing.
    
    Args:
        prefix: Filename prefix
        suffix: Filename suffix 
        in_test_logs: If True, create in test logs directory.
                     If False, use system temp directory.
    
    Returns:
        str: Path to the created temporary file
    """
    if in_test_logs:
        logs_dir = get_test_logs_dir()
        fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=logs_dir)
    else:
        fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    
    # Close the file descriptor (we just want the path)
    os.close(fd)
    
    return path


class TestLogContext:
    """
    Context manager for test logging with automatic cleanup.
    
    Usage:
        with TestLogContext("my_test") as log_file:
            setup_logging(log_file=log_file)
            # ... run test ...
        # log file is automatically cleaned up
    """
    
    def __init__(self, test_name: str, cleanup_on_exit: bool = True):
        self.test_name = test_name
        self.cleanup_on_exit = cleanup_on_exit
        self.log_file = None
    
    def __enter__(self) -> str:
        self.log_file = get_test_log_file(self.test_name)
        return self.log_file
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cleanup_on_exit and self.log_file and os.path.exists(self.log_file):
            try:
                os.remove(self.log_file)
            except OSError:
                pass  # Ignore cleanup errors


def ensure_test_logs_gitignored() -> bool:
    """
    Verify that test logs are properly ignored by git.
    
    Returns:
        bool: True if test logs appear to be properly gitignored
    """
    # Get project root (grandparent of utils directory)
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    test_dir = os.path.dirname(utils_dir)
    project_root = os.path.dirname(test_dir)
    gitignore_path = os.path.join(project_root, '.gitignore')
    
    if not os.path.exists(gitignore_path):
        return False
    
    with open(gitignore_path, 'r') as f:
        gitignore_content = f.read()
    
    # Check for test logs patterns
    test_logs_patterns = [
        'tests/logs/',
        'tests/**/*.log'
    ]
    
    return all(pattern in gitignore_content for pattern in test_logs_patterns)


# Convenience functions for common test scenarios
def setup_test_file_logging(test_name: str, log_level: str = "DEBUG") -> tuple[str, object]:
    """
    Set up file logging for a test with automatic file path generation.
    
    Args:
        test_name: Name of the test
        log_level: Logging level
    
    Returns:
        tuple: (log_file_path, logger_instance)
    """
    import sys
    import os
    
    # Add src to path if not already there (utils -> tests -> project_root -> src)
    utils_dir = os.path.dirname(__file__)
    test_dir = os.path.dirname(utils_dir)
    project_root = os.path.dirname(test_dir)
    src_path = os.path.join(project_root, 'src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    from jobs.utils.logger_config import setup_logging, get_logger
    
    log_file = get_test_log_file(test_name)
    setup_logging(log_level=log_level, log_file=log_file, environment="development")
    logger = get_logger(test_name)
    
    return log_file, logger


if __name__ == "__main__":
    # Test the utilities
    print("Testing test utilities...")
    
    # Test directory creation
    logs_dir = get_test_logs_dir()
    print(f"Test logs directory: {logs_dir}")
    
    # Test log file creation
    test_file = get_test_log_file("test_utilities")
    print(f"Test log file: {test_file}")
    
    # Test gitignore check
    gitignored = ensure_test_logs_gitignored()
    print(f"Test logs properly gitignored: {gitignored}")
    
    # Test context manager
    with TestLogContext("context_test") as log_file:
        print(f"Context log file: {log_file}")
        # Write something to the file
        with open(log_file, 'w') as f:
            f.write("test content")
        print(f"File exists during context: {os.path.exists(log_file)}")
    
    print(f"File exists after context: {os.path.exists(log_file)}")
    
    print("Test utilities working correctly!")
