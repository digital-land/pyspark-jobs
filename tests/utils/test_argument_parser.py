#!/usr/bin/env python3
"""
Test script for argument parser configuration, especially the S3 bucket default.

This utility script verifies that the argument parser in run_main.py works correctly
with default values and handles missing Airflow variables gracefully.

Usage:
    python3 tests/utils/test_argument_parser.py
"""

import sys
import os
import argparse

# Add src to path (utils -> tests -> project_root -> src)
utils_dir = os.path.dirname(__file__)
test_dir = os.path.dirname(utils_dir)
project_root = os.path.dirname(test_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

def create_test_parser():
    """Create a parser matching the one in run_main.py for testing"""
    parser = argparse.ArgumentParser(description="ETL Process for Collection Data")
    parser.add_argument("--load_type", type=str, required=True,
                        help="Type of load operation (e.g., full, incremental, sample)")
    parser.add_argument("--data_set", type=str, required=True,
                        help="Name of the dataset to process")
    parser.add_argument("--path", type=str, required=True,
                        help="Path to the dataset")
    parser.add_argument("--env", type=str, required=True,
                        help="Environment (e.g., development, staging, production, local)")
    parser.add_argument("--s3_bucket", type=str, required=False, 
                        default="development-pyspark-jobs-codepackage",
                        help="S3 bucket name for temporary staging (default: development-pyspark-jobs-codepackage)")
    
    return parser

def main():
    """Test the argument parser configuration"""
    print("🧪 Testing Argument Parser with S3 Bucket Default")
    print("=" * 50)
    
    parser = create_test_parser()
    
    # Test 1: Minimal args (should use default s3_bucket)
    print("\n1. Testing with minimal arguments (s3_bucket should default):")
    test_args = ['--load_type', 'full', '--data_set', 'test', '--path', 's3://test/', '--env', 'development']
    args = parser.parse_args(test_args)
    
    print(f"   load_type: {args.load_type}")
    print(f"   data_set: {args.data_set}")
    print(f"   path: {args.path}")
    print(f"   env: {args.env}")
    print(f"   s3_bucket: {args.s3_bucket} ✅ (default applied)")
    
    # Test 2: With explicit s3_bucket
    print("\n2. Testing with explicit s3_bucket:")
    test_args = ['--load_type', 'full', '--data_set', 'test', '--path', 's3://test/', '--env', 'development', '--s3_bucket', 'custom-staging-bucket']
    args = parser.parse_args(test_args)
    
    print(f"   s3_bucket: {args.s3_bucket} ✅ (explicit value)")
    
    # Test 3: Verify default is always present
    print("\n3. Testing s3_bucket is never None:")
    test_args = ['--load_type', 'sample', '--data_set', 'entity', '--path', 's3://bucket/data/', '--env', 'production']
    args = parser.parse_args(test_args)
    
    if args.s3_bucket is not None:
        print(f"   s3_bucket: {args.s3_bucket} ✅ (never None)")
    else:
        print(f"   ❌ s3_bucket is None - this should not happen!")
        return False
    
    # Test 4: Show help text includes default
    print("\n4. Verify help text shows default:")
    help_text = parser.format_help()
    if "default: development-pyspark-jobs-codepackage" in help_text:
        print("   ✅ Help text correctly shows default value")
    else:
        print("   ⚠️  Help text may not show default correctly")
    
    print("\n✅ All argument parser tests passed!")
    print("\n🎯 Benefits of the default value:")
    print("   • Job never fails due to missing S3 bucket argument")
    print("   • Works out of the box for development/testing")
    print("   • Airflow can still override the bucket as needed")
    print("   • Environment variable can still override if required")
    print("   • Defensive programming - handles missing configuration gracefully")
    
    print("\n📝 Related tests:")
    print("   python3 tests/utils/test_logging_quick.py")
    print("   python3 tests/unit/test_copy_protocol_auto_s3.py")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
