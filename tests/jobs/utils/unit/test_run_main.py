"""Unit tests for run_main module."""
import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))

@patch('jobs.run_main.main')
def test_parse_args_success(mock_main):
    """Test parse_args with valid arguments."""
    import jobs.run_main
    
    with patch('sys.argv', ['run_main.py', '--load_type', 'full', '--data_set', 'test', '--path', 's3://test', '--env', 'dev']):
        args = jobs.run_main.parse_args()
        assert args.load_type == 'full'
        assert args.data_set == 'test'
        assert args.path == 's3://test'
        assert args.env == 'dev'
        assert args.use_jdbc == False

def test_parse_args_with_jdbc():
    """Test parse_args with JDBC flag."""
    import jobs.run_main
    
    with patch('sys.argv', ['run_main.py', '--load_type', 'full', '--data_set', 'test', '--path', 's3://test', '--env', 'dev', '--use-jdbc']):
        args = jobs.run_main.parse_args()
        assert args.use_jdbc == True

def test_parse_args_missing_required():
    """Test parse_args with missing required arguments."""
    import jobs.run_main
    
    with patch('sys.argv', ['run_main.py']):
        with pytest.raises(SystemExit):
            jobs.run_main.parse_args()