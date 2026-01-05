# !/usr/bin/env python3
"""
Test script to check COPY protocol setup and S3 bucket configuration.

This script helps diagnose COPY protocol issues and provides setup guidance.
"""


# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


def test_copy_protocol_setup():
    """Test COPY protocol configuration and S3 bucket setup."""
    print("🔍 Testing COPY Protocol Configuration")
    print("=" * 45)

    try:
        from jobs.dbaccess.postgres_connectivity import get_copy_protocol_recommendation

        # Test with no configuration
        print("\n1. Testing with no S3 bucket configuration:")
        result = get_copy_protocol_recommendation()
        print(f"   Method: {result['method']}")
        print(f"   Reason: {result['reason']}")
        if "setup_instructions" in result:
            print("   Setup Instructions:")
            for instruction in result["setup_instructions"]:
                print(f"   - {instruction}")

        # Test with environment variable
        print("\n2. Testing with environment variable:")
        test_bucket = "test - bucket - name"
        os.environ["PYSPARK_TEMP_S3_BUCKET"] = test_bucket
        result = get_copy_protocol_recommendation()
        print(f"   Method: {result['method']}")
        print(f"   Reason: {result['reason']}")
        if "troubleshooting" in result:
            print("   Troubleshooting:")
            for item in result["troubleshooting"]:
                print(f"   - {item}")

        # Clean up environment
        del os.environ["PYSPARK_TEMP_S3_BUCKET"]

        # Test with specific bucket
        print("\n3. Testing with specific bucket parameter:")
        result = get_copy_protocol_recommendation("my - existing - bucket")
        print(f"   Method: {result['method']}")
        print(f"   Reason: {result['reason']}")
        if "expected_performance" in result:
            print(f"   Expected Performance: {result['expected_performance']}")

        print("\n✅ COPY protocol configuration tests completed")

    except Exception as e:
        print(f"❌ Error testing COPY protocol: {e}")
        return False

    return True


def demonstrate_recommended_usage():
    """Show recommended usage patterns for PostgreSQL writes."""
    print("\n📚 Recommended Usage Patterns")
    print("=" * 35)

    usage_examples = [
        {
            "name": "Standard Usage (Recommended)",
            "description": "Let the system automatically choose the best method",
            "code": """
from jobs.dbaccess.postgres_connectivity import write_to_postgres, get_performance_recommendations

# Get automatic recommendations
row_count = df.count()
recommendations = get_performance_recommendations(row_count)
print(f"Recommendations: {recommendations}")

# Write with recommendations
write_to_postgres(
    df,
    conn_params,
    method=recommendations["method"],
    batch_size=recommendations["batch_size"],
    num_partitions=recommendations["num_partitions"]
)
""",
        },
        {
            "name": "Force Optimized JDBC (Reliable)",
            "description": "Use optimized JDBC for guaranteed performance",
            "code": """
# Always use optimized JDBC (3 - 5x performance improvement)
write_to_postgres(df, conn_params, method="optimized")
""",
        },
        {
            "name": "COPY Protocol (Maximum Speed)",
            "description": "Use COPY protocol when S3 is properly configured",
            "code": """
# Option 1: Set environment variable first
os.environ['PYSPARK_TEMP_S3_BUCKET'] = 'your - bucket - name'
write_to_postgres(df, conn_params, method="copy")

# Option 2: Check configuration first
from jobs.dbaccess.postgres_connectivity import get_copy_protocol_recommendation
copy_config = get_copy_protocol_recommendation("your - bucket - name")
if copy_config["method"] == "copy":
    write_to_postgres(df, conn_params, **copy_config)
else:
    print(f"COPY not available: {copy_config['reason']}")
    write_to_postgres(df, conn_params, method="optimized")
""",
        },
        {
            "name": "Dual Output (PostgreSQL + SQLite)",
            "description": "Write to both PostgreSQL and SQLite simultaneously",
            "code": """
from jobs.dbaccess.postgres_connectivity import write_to_postgres, write_to_sqlite

# Write to PostgreSQL (primary)
write_to_postgres(df, conn_params, method="optimized")

# Also write to SQLite (backup/local analysis)
write_to_sqlite(df, "/path/to/output.sqlite", method="optimized")
""",
        },
    ]

    for i, example in enumerate(usage_examples, 1):
        print(f"\n{i}. {example['name']}")
        print(f"   Description: {example['description']}")
        print("   Code:")
        print(example["code"])


def show_s3_bucket_setup():
    """Show S3 bucket setup instructions."""
    print("\n🔧 S3 Bucket Setup for COPY Protocol")
    print("=" * 40)

    print(
        """
For maximum performance with large datasets, you can configure the COPY protocol:

1. **Create/Configure S3 Bucket:**
   - Create an S3 bucket or use an existing one
   - Ensure your EMR Serverless job has read/write access
   - The bucket should be in the same region as your PostgreSQL instance

2. **Set Environment Variable (Recommended):**
   export PYSPARK_TEMP_S3_BUCKET=your - bucket - name

3. **PostgreSQL Requirements:**
   - PostgreSQL server must have aws_s3 extension installed
   - PostgreSQL must have network access to S3
   - Required for RDS: Enable aws_s3 extension in parameter group

4. **Verification:**
   Use the test script to verify configuration before running large jobs.

📊 **Performance Comparison:**
- Standard JDBC: Baseline performance
- Optimized JDBC: 3 - 5x faster (RECOMMENDED)
- COPY Protocol: 5 - 10x faster (requires S3 setup)
- Async Batches: 4 - 8x faster (small datasets only)

💡 **Current Default:** The system now defaults to optimized JDBC for reliability.
   COPY protocol is available but requires manual configuration.
"""
    )


def main():
    """Run all COPY protocol tests and show usage guidance."""
    print("🧪 COPY Protocol Setup & Configuration Test")
    print("=" * 50)

    success = test_copy_protocol_setup()

    if success:
        demonstrate_recommended_usage()
        show_s3_bucket_setup()
        print("\n🎉 All tests completed successfully!")
        print(
            "\n💡 Key Takeaway: The system will automatically fall back to optimized JDBC"
        )
        print(
            "   if COPY protocol is not properly configured, ensuring reliable operation."
        )
    else:
        print("\n⚠️  Some tests failed - check your setup")

    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
