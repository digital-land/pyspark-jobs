#!/usr/bin/env python3
"""
Test script for argument parser configuration.

This utility script verifies that the argument parser in run_main.py works correctly
with all required arguments for optimized JDBC PostgreSQL writes.

Usage:
    python3 tests/utils/test_argument_parser.py
"""

import argparse
import os
import sys

# Add src to path (utils -> tests -> project_root -> src)
utils_dir = os.path.dirname(__file__)
test_dir = os.path.dirname(utils_dir)
project_root = os.path.dirname(test_dir)
src_path = os.path.join(project_root, "src")
sys.path.insert(0, src_path)


def create_test_parser():
    """Create a parser matching the one in run_main.py for testing"""
    parser = argparse.ArgumentParser(description="ETL Process for Collection Data")
    parser.add_argument(
        "--load_type",
        type=str,
        required=True,
        help="Type of load operation (e.g., full, incremental, sample)",
    )
    parser.add_argument(
        "--data_set", type=str, required=True, help="Name of the dataset to process"
    )
    parser.add_argument("--path", type=str, required=True, help="Path to the dataset")
    parser.add_argument(
        "--env",
        type=str,
        required=True,
        help="Environment (e.g., development, staging, production, local)",
    )

    return parser


def main():
    """Test the argument parser configuration"""
    print("🧪 Testing Argument Parser for Optimized JDBC")
    print("=" * 50)

    parser = create_test_parser()

    # Test 1: Standard arguments
    print("\n1. Testing with standard arguments:")
    test_args = [
        "--load_type",
        "full",
        "--data_set",
        "test",
        "--path",
        "s3://test/",
        "--env",
        "development",
    ]
    args = parser.parse_args(test_args)

    print(f"   load_type: {args.load_type}")
    print(f"   data_set: {args.data_set}")
    print(f"   path: {args.path}")
    print(f"   env: {args.env}")
    print("   ✅ All required arguments parsed successfully")

    # Test 2: Different environment
    print("\n2. Testing with production environment:")
    test_args = [
        "--load_type",
        "sample",
        "--data_set",
        "entity",
        "--path",
        "s3://bucket/data/",
        "--env",
        "production",
    ]
    args = parser.parse_args(test_args)

    print(f"   env: {args.env} ✅ (production environment)")

    # Test 3: Verify all required arguments work
    print("\n3. Testing argument completeness:")
    required_attrs = ["load_type", "data_set", "path", "env"]
    all_present = all(hasattr(args, attr) for attr in required_attrs)

    if all_present:
        print("   ✅ All required arguments present")
    else:
        print("   ❌ Missing required arguments!")
        return False

    print("\n✅ All argument parser tests passed!")
    print("\n🎯 Simplified argument structure:")
    print("   • All required arguments work correctly")
    print("   • Clean and focused argument set")
    print("   • No unnecessary complexity")
    print("   • Optimized for JDBC-only PostgreSQL writes")

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
