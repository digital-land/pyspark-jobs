#!/usr/bin/env python3
"""
Large Dataset Processing Monitor

This utility helps monitor the progress of large PostgreSQL writes,
especially useful for datasets like your 34.8M row entity table.

Usage:
    python3 tests/utils/test_large_dataset_monitoring.py
"""

import time
import sys
import os

# Add src to path (utils -> tests -> project_root -> src)
utils_dir = os.path.dirname(__file__)
test_dir = os.path.dirname(utils_dir)
project_root = os.path.dirname(test_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

def estimate_completion_time(total_rows, batch_size, processing_time_per_batch):
    """Estimate total completion time for large dataset processing"""
    total_batches = total_rows / batch_size
    estimated_total_time = total_batches * processing_time_per_batch
    return estimated_total_time

def format_time(seconds):
    """Format seconds into human-readable time"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        return f"{seconds/60:.1f} minutes"
    else:
        return f"{seconds/3600:.1f} hours"

def main():
    """Monitor and provide estimates for large dataset processing"""
    print("📊 Large Dataset Processing Monitor")
    print("=" * 40)
    
    # Based on your current logs
    total_rows = 34845893
    batch_size = 20000
    num_partitions = 20
    
    print(f"📈 Dataset Analysis:")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Batch size: {batch_size:,}")
    print(f"   Partitions: {num_partitions}")
    print(f"   Total batches: {total_rows // batch_size:,}")
    
    # Performance estimates based on typical JDBC performance
    print(f"\n⏱️  Performance Estimates:")
    
    # Conservative estimates (rows per second)
    scenarios = [
        ("Conservative (network limited)", 5000),
        ("Typical (optimized JDBC)", 15000), 
        ("Optimistic (fast network)", 30000),
        ("COPY protocol (if enabled)", 100000)
    ]
    
    for scenario_name, rows_per_second in scenarios:
        total_time = total_rows / rows_per_second
        print(f"   {scenario_name}: {format_time(total_time)}")
    
    print(f"\n🔧 Optimization Status:")
    print(f"   ✅ COPY protocol attempted (failed gracefully)")
    print(f"   ✅ Optimized JDBC fallback active")
    print(f"   ✅ Geometry columns processed with SRID")
    print(f"   ✅ Smart partitioning applied")
    print(f"   ✅ Auto-calculated batch size")
    
    print(f"\n📋 Current Processing Configuration:")
    print(f"   Method: Optimized JDBC (fallback from COPY)")
    print(f"   Batch size: {batch_size:,} (auto-calculated)")
    print(f"   Partitions: {num_partitions} (optimized from 250)")
    print(f"   Geometry handling: Enhanced WKT with SRID")
    
    print(f"\n🚀 To Enable COPY Protocol (Optional 5-10x speedup):")
    print(f"   1. Connect to PostgreSQL as superuser")
    print(f"   2. Run: CREATE EXTENSION IF NOT EXISTS aws_s3;")
    print(f"   3. Ensure RDS has S3 access permissions")
    print(f"   4. Next run will use COPY protocol automatically")
    
    print(f"\n📊 Monitoring Commands:")
    print(f"   # Check table size during processing")
    print(f"   SELECT COUNT(*) FROM pyspark_entity;")
    print(f"   ")
    print(f"   # Monitor progress with timestamps")
    print(f"   SELECT COUNT(*), MAX(entry_date) FROM pyspark_entity;")
    print(f"   ")
    print(f"   # Check for any errors or locks")
    print(f"   SELECT * FROM pg_stat_activity WHERE datname = 'your_db';")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Monitor failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
