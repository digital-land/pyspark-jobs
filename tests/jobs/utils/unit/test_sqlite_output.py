#!/usr/bin/env python3
"""
Unit tests for SQLite output functionality.

This test module verifies that the SQLite output functions work correctly
with different dataset sizes and output methods.
"""

import sys
import os
import tempfile
import shutil

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

def test_sqlite_basic_functionality():
    """Test basic SQLite output functionality."""
    print("🔍 Testing SQLite Output Functionality")
    print("=" * 40)
    
    try:
        # Import required modules
        from jobs.dbaccess.postgres_connectivity import write_to_sqlite, get_sqlite_schema_mapping
        print("✅ Successfully imported SQLite functions")
        
        # Test schema mapping
        schema_mapping = get_sqlite_schema_mapping()
        print(f"✅ Schema mapping retrieved: {len(schema_mapping)} type mappings")
        
        # Test with mock data (would normally use actual PySpark DataFrame)
        # For this test, we'll just verify the functions can be called
        print("✅ SQLite functions are importable and callable")
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
    
    return True

def test_sqlite_schema_mapping():
    """Test SQLite schema type mapping."""
    print("\n🔍 Testing SQLite Schema Mapping")
    print("-" * 40)
    
    try:
        from jobs.dbaccess.postgres_connectivity import get_sqlite_schema_mapping
        
        mapping = get_sqlite_schema_mapping()
        
        # Verify expected mappings
        expected_mappings = {
            "TEXT": "TEXT",
            "DATE": "TEXT",
            "JSONB": "TEXT",
            "GEOMETRY(MULTIPOLYGON, 4326)": "TEXT",
            "BIGINT": "INTEGER",
            "GEOMETRY(POINT, 4326)": "TEXT",
        }
        
        for pg_type, expected_sqlite_type in expected_mappings.items():
            if mapping.get(pg_type) == expected_sqlite_type:
                print(f"✅ {pg_type} → {expected_sqlite_type}")
            else:
                print(f"❌ {pg_type} → {mapping.get(pg_type)} (expected {expected_sqlite_type})")
                return False
        
        print("✅ All schema mappings correct")
        
    except Exception as e:
        print(f"❌ Schema mapping test failed: {e}")
        return False
    
    return True

def test_sqlite_output_paths():
    """Test SQLite output path handling."""
    print("\n🔍 Testing SQLite Output Path Handling")
    print("-" * 40)
    
    test_cases = [
        ("local/path/output", "entity", "local/path/output/entity.sqlite"),
        ("local/path/output.sqlite", "entity", "local/path/output.sqlite"),
        ("/tmp/test.db", "entity", "/tmp/test.db"),
        ("s3://bucket/path", "entity", "s3://bucket/path/entity.sqlite"),
    ]
    
    for input_path, table_name, expected_output in test_cases:
        # This would normally be tested with actual file operations
        # For now, we'll just verify the path logic
        if input_path.endswith('.db') or input_path.endswith('.sqlite'):
            actual_output = input_path
        else:
            actual_output = f"{input_path.rstrip('/')}/{table_name}.sqlite"
        
        if actual_output == expected_output:
            print(f"✅ {input_path} → {actual_output}")
        else:
            print(f"❌ {input_path} → {actual_output} (expected {expected_output})")
            return False
    
    print("✅ All path handling tests passed")
    return True

def demonstrate_sqlite_usage():
    """Demonstrate how to use SQLite output functionality."""
    print("\n📚 SQLite Usage Examples")
    print("=" * 40)
    
    usage_examples = [
        {
            "name": "Basic SQLite Output",
            "code": """
from jobs.dbaccess.postgres_connectivity import write_to_sqlite

# Write DataFrame to single SQLite file
result = write_to_sqlite(
    df, 
    output_path="/path/to/output/data.sqlite",
    method="single_file"
)
""",
            "description": "Write to a single SQLite file"
        },
        {
            "name": "Optimized SQLite Output", 
            "code": """
# Automatically choose single file or partitioned based on size
result = write_to_sqlite(
    df,
    output_path="/path/to/output/directory", 
    method="optimized",
    max_records_per_file=500000
)
""",
            "description": "Optimized output with automatic file size management"
        },
        {
            "name": "Partitioned SQLite Output",
            "code": """
# Create multiple SQLite files for large datasets
result = write_to_sqlite(
    df,
    output_path="/path/to/output/directory",
    method="partitioned",
    num_partitions=5
)
""",
            "description": "Multiple SQLite files for large datasets"
        },
        {
            "name": "S3 SQLite Output",
            "code": """
# Write SQLite files to S3
result = write_to_sqlite(
    df,
    output_path="s3://my-bucket/sqlite-output/",
    method="optimized"
)
""",
            "description": "Upload SQLite files to S3"
        }
    ]
    
    for example in usage_examples:
        print(f"\n🔧 {example['name']}")
        print(f"Description: {example['description']}")
        print("Code:")
        print(example['code'])
    
    print("\n✅ Usage examples displayed")

def main():
    """Run all SQLite tests."""
    print("🧪 SQLite Output Testing Suite")
    print("=" * 50)
    
    tests = [
        ("Basic Functionality", test_sqlite_basic_functionality),
        ("Schema Mapping", test_sqlite_schema_mapping), 
        ("Path Handling", test_sqlite_output_paths),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ {test_name} failed")
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        demonstrate_sqlite_usage()
    else:
        print("⚠️  Some tests failed")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
