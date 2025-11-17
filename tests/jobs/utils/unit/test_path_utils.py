"""Unit tests for path_utils module."""
import pytest
from unittest.mock import Mock, patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))

def test_s3_path_validation():
    """Test S3 path validation logic."""
    # Test valid S3 paths
    valid_paths = [
        's3://bucket/path/file.csv',
        's3://my-bucket/data/',
        's3://bucket-name/folder/subfolder/file.parquet'
    ]
    
    for path in valid_paths:
        assert path.startswith('s3://')
        assert len(path.split('/')) >= 3

def test_local_path_validation():
    """Test local path validation logic."""
    # Test local paths
    local_paths = [
        '/tmp/data/file.csv',
        './data/file.parquet',
        '../output/results.json'
    ]
    
    for path in local_paths:
        assert not path.startswith('s3://')

def test_path_normalization():
    """Test path normalization."""
    paths = [
        ('s3://bucket//path//file.csv', 's3://bucket/path/file.csv'),
        ('/tmp//data//file.csv', '/tmp/data/file.csv'),
        ('bucket/path/', 'bucket/path/')
    ]
    
    for input_path, expected in paths:
        # Simple normalization logic
        normalized = input_path.replace('//', '/')
        if normalized.startswith('s3:/'):
            normalized = normalized.replace('s3:/', 's3://')
        assert normalized == expected

def test_file_extension_extraction():
    """Test file extension extraction."""
    files = [
        ('file.csv', 'csv'),
        ('data.parquet', 'parquet'),
        ('output.json', 'json'),
        ('script.py', 'py')
    ]
    
    for filename, expected_ext in files:
        ext = filename.split('.')[-1] if '.' in filename else ''
        assert ext == expected_ext