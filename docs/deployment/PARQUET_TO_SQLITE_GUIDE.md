# Parquet to SQLite Conversion Guide

## Overview

The `parquet_to_sqlite.py` script provides a dedicated tool for converting parquet files to SQLite databases. This approach offers better separation of concerns and allows SQLite conversion to run independently from the main data processing pipeline.

## Key Benefits

- **🔄 Separate Process**: Runs independently from main data pipeline
- **📁 Reads from Parquet**: Uses existing parquet outputs as input
- **🚀 High Performance**: Optimized for large dataset conversion
- **📦 Portable Output**: Creates self-contained SQLite databases
- **☁️ S3 Support**: Can read from and write to S3
- **🔧 Flexible**: Multiple output modes for different use cases

## Usage

### Basic Command Line Usage

#### Using the Convenient Shell Script (Recommended)

```bash
# Convert S3 parquet to local SQLite
./bin/convert_parquet_to_sqlite.sh \
  --input s3://my-bucket/output-parquet-entity/ \
  --output ./entity.sqlite

# Create multiple SQLite files for large datasets
./bin/convert_parquet_to_sqlite.sh \
  --input s3://my-bucket/output-parquet-entity/ \
  --output ./sqlite-output/ \
  --method partitioned

# Custom table name with verbose output
./bin/convert_parquet_to_sqlite.sh \
  --input s3://my-bucket/parquet/ \
  --output ./entity.sqlite \
  --table-name entity \
  --verbose
```

#### Using the Python Script Directly

```bash
# Convert S3 parquet to local SQLite
python src/jobs/parquet_to_sqlite.py \
  --input s3://my-bucket/output-parquet-entity/ \
  --output ./entity.sqlite

# Convert to multiple SQLite files for large datasets
python src/jobs/parquet_to_sqlite.py \
  --input s3://my-bucket/output-parquet-entity/ \
  --output ./sqlite-output/ \
  --method partitioned

# Upload SQLite to S3
python src/jobs/parquet_to_sqlite.py \
  --input s3://my-bucket/output-parquet-entity/ \
  --output s3://my-bucket/sqlite-backup/entity.sqlite

# Custom table name and verbose output
python src/jobs/parquet_to_sqlite.py \
  --input s3://my-bucket/output-parquet-entity/ \
  --output ./entity.sqlite \
  --table-name pyspark_entity \
  --verbose
```

### Command Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Input path to parquet files (local or S3) | Required |
| `--output` | `-o` | Output path for SQLite file(s) (local or S3) | Required |
| `--table-name` | `-t` | Name of table in SQLite database | `data` |
| `--method` | `-m` | Conversion method | `optimized` |
| `--max-records` | | Maximum records per SQLite file | `500000` |
| `--verbose` | `-v` | Enable verbose logging | `false` |

### Conversion Methods

#### 1. **Optimized (Recommended)**
Automatically chooses the best approach based on dataset size:
- **< 500k rows**: Single SQLite file
- **> 500k rows**: Multiple SQLite files

```bash
python src/jobs/parquet_to_sqlite.py \
  --input s3://bucket/parquet/ \
  --output ./output.sqlite \
  --method optimized
```

#### 2. **Single File**
Creates one SQLite database file regardless of size:

```bash
python src/jobs/parquet_to_sqlite.py \
  --input s3://bucket/parquet/ \
  --output ./output.sqlite \
  --method single_file
```

#### 3. **Partitioned**
Creates multiple SQLite files for better manageability:

```bash
python src/jobs/parquet_to_sqlite.py \
  --input s3://bucket/parquet/ \
  --output ./sqlite-output/ \
  --method partitioned \
  --max-records 250000
```

## Integration with Main Pipeline

### Current Workflow

1. **Main Pipeline**: Processes data and writes to parquet
2. **SQLite Conversion**: Separate step reads parquet and creates SQLite

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Raw Data (CSV)  │ => │ Main Pipeline    │ => │ Parquet Files   │
└─────────────────┘    │ main_collection_ │    │ (S3 Storage)    │
                       │ data.py          │    └─────────────────┘
                       └──────────────────┘               │
                                                          │
                       ┌──────────────────┐               │
                       │ SQLite Converter │ <─────────────┘
                       │ parquet_to_      │
                       │ sqlite.py        │
                       └──────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │ SQLite Files    │
                       │ (Local/S3)      │
                       └─────────────────┘
```

### Recommended Usage Patterns

#### For Development/Testing
```bash
# Quick local conversion for analysis
python src/jobs/parquet_to_sqlite.py \
  --input s3://my-bucket/output-parquet-entity/ \
  --output ./local_analysis.sqlite
```

#### For Production Backup
```bash
# Create SQLite backup on S3
python src/jobs/parquet_to_sqlite.py \
  --input s3://my-bucket/output-parquet-entity/ \
  --output s3://my-bucket/sqlite-backup/ \
  --method optimized
```

#### For Large Dataset Distribution
```bash
# Create multiple manageable SQLite files
python src/jobs/parquet_to_sqlite.py \
  --input s3://my-bucket/output-parquet-entity/ \
  --output ./distribution/ \
  --method partitioned \
  --max-records 100000
```

## Data Type Handling

The converter automatically handles data type mapping from Spark/Parquet to SQLite:

| Spark/Parquet Type | SQLite Type | Notes |
|-------------------|-------------|-------|
| String/Text | TEXT | Direct mapping |
| Date/Timestamp | TEXT | ISO format (YYYY-MM-DD) |
| JSON/JSONB | TEXT | Serialized JSON strings |
| Geometry/Point | TEXT | WKT format for GIS compatibility |
| Integer/Long | INTEGER | Direct mapping |
| Float/Double | REAL | Direct mapping |
| Boolean | INTEGER | 0/1 representation |

## Performance Characteristics

### Conversion Speed
- **Small datasets** (< 100k rows): ~1-2 minutes
- **Medium datasets** (100k-1M rows): ~5-15 minutes  
- **Large datasets** (> 1M rows): ~20-60 minutes

### Output File Sizes
- **Text-heavy data**: ~50-70% of parquet size
- **Numeric data**: ~30-50% of parquet size
- **Mixed data**: ~40-60% of parquet size

### Memory Usage
- **Single file mode**: Requires enough memory for full dataset
- **Partitioned mode**: Memory usage scales with partition size
- **Recommended**: Use partitioned mode for datasets > 1M rows

## Examples

### Example 1: Daily Entity Processing
```bash
#!/bin/bash
# Daily entity SQLite backup script

DATE=$(date +%Y-%m-%d)
S3_INPUT="s3://my-bucket/output-parquet-entity/"
S3_OUTPUT="s3://my-bucket/sqlite-backup/entity-${DATE}.sqlite"

python src/jobs/parquet_to_sqlite.py \
  --input "$S3_INPUT" \
  --output "$S3_OUTPUT" \
  --table-name entity \
  --method optimized \
  --verbose

echo "Entity SQLite backup completed: $S3_OUTPUT"
```

### Example 2: Programmatic Usage
```python
#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from jobs.parquet_to_sqlite import convert_parquet_to_sqlite

# Convert parquet to SQLite programmatically
result = convert_parquet_to_sqlite(
    input_path="s3://my-bucket/output-parquet-entity/",
    output_path="./entity_data.sqlite",
    table_name="entity",
    method="optimized"
)

print(f"SQLite database created: {result}")
```

### Example 3: Batch Processing Multiple Tables
```bash
#!/bin/bash
# Convert multiple parquet tables to SQLite

TABLES=("entity" "fact" "fact_res")
S3_BASE="s3://my-bucket"
LOCAL_OUTPUT="./sqlite-output"

mkdir -p "$LOCAL_OUTPUT"

for table in "${TABLES[@]}"; do
    echo "Converting $table..."
    python src/jobs/parquet_to_sqlite.py \
        --input "${S3_BASE}/output-parquet-${table}/" \
        --output "${LOCAL_OUTPUT}/${table}.sqlite" \
        --table-name "$table" \
        --method optimized
done

echo "All conversions completed in $LOCAL_OUTPUT"
```

## Troubleshooting

### Common Issues

#### 1. **Memory Errors**
```
Error: Java heap space / Out of memory
```
**Solution**: Use partitioned method or increase Spark memory:
```bash
export SPARK_DRIVER_MEMORY=8g
export SPARK_EXECUTOR_MEMORY=8g
python src/jobs/parquet_to_sqlite.py --method partitioned
```

#### 2. **S3 Access Issues**
```
Error: Access Denied / NoSuchBucket
```
**Solution**: Check AWS credentials and bucket permissions:
```bash
aws s3 ls s3://your-bucket/  # Test access
aws configure list         # Check credentials
```

#### 3. **Large File Warnings**
```
Warning: Large dataset may cause memory issues
```
**Solution**: Use partitioned method:
```bash
python src/jobs/parquet_to_sqlite.py \
  --method partitioned \
  --max-records 250000
```

#### 4. **Schema Issues**
```
Error: Column type not supported
```
**Solution**: The converter handles most types automatically. Check logs for specific column issues.

### Performance Tuning

#### For Large Datasets
```bash
# Increase partition size for better performance
python src/jobs/parquet_to_sqlite.py \
  --method partitioned \
  --max-records 1000000
```

#### For Memory-Constrained Environments
```bash
# Use smaller partitions
python src/jobs/parquet_to_sqlite.py \
  --method partitioned \
  --max-records 100000
```

## Monitoring and Logging

The script provides comprehensive logging:

```bash
# Enable verbose logging
python src/jobs/parquet_to_sqlite.py \
  --input s3://bucket/parquet/ \
  --output ./output.sqlite \
  --verbose

# Redirect logs to file
python src/jobs/parquet_to_sqlite.py \
  --input s3://bucket/parquet/ \
  --output ./output.sqlite \
  --verbose 2>&1 | tee conversion.log
```

## Best Practices

1. **Use Optimized Method**: Start with `--method optimized` for automatic sizing
2. **Monitor Memory**: Watch memory usage for large datasets
3. **Test Small First**: Try with a subset before processing full datasets
4. **Backup Strategy**: Use S3 output for important data preservation
5. **Partition Large Data**: Use partitioned method for datasets > 500k rows
6. **Validate Output**: Spot-check SQLite files after conversion

## Summary

The dedicated `parquet_to_sqlite.py` script provides:

- ✅ **Clean Architecture**: Separate from main pipeline
- ✅ **Flexible Input**: Reads any parquet files
- ✅ **Multiple Output Modes**: Single file, partitioned, or optimized
- ✅ **Production Ready**: Handles large datasets with proper error handling
- ✅ **S3 Integration**: Full support for cloud storage
- ✅ **Easy Integration**: Simple command-line interface

This approach gives you maximum flexibility for creating SQLite databases from your parquet data while keeping the main processing pipeline focused and efficient.
