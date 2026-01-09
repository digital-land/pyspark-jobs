# Polars Implementation for GML Data Processing

This folder contains Polars-based implementations converted from the DuckDB scripts. Polars is a fast DataFrame library that provides similar functionality to DuckDB but with a more Pythonic API.

## Files

### 1. `flatten_gml_to_parquet.py`
Converts GML files to Parquet format using Polars instead of DuckDB.

**Key Changes from DuckDB version:**
- Uses `polars.DataFrame()` instead of `pandas.DataFrame()` + DuckDB
- Uses `df.write_parquet()` instead of DuckDB's COPY command
- Uses `pl.lit()` and `.with_columns()` for adding council_name column
- More memory efficient with lazy evaluation support

**Usage:**
```bash
python flatten_gml_to_parquet.py
```

**Output:**
- Parquet files in `parquet_flattened_polars/` directory
- One parquet file per council

### 2. `get_gml_volume.py`
Downloads and analyzes GML data volume, saving results in multiple formats.

**Key Changes from DuckDB version:**
- Uses `polars.DataFrame()` to store analysis results
- Saves output as CSV, Parquet, and text using Polars methods
- Uses `df.write_csv()` and `df.write_parquet()` for efficient file writing
- Uses `df.iter_rows(named=True)` for iterating over data

**Usage:**
```bash
python get_gml_volume.py
```

**Output:**
- `gml_volume_report.txt` - Human-readable text report
- `gml_volume_report.csv` - CSV format for analysis
- `gml_volume_report.parquet` - Parquet format for efficient storage

## Installation

```bash
pip install -r requirements.txt
```

## Key Differences: Polars vs DuckDB

| Feature | DuckDB | Polars |
|---------|--------|--------|
| **API Style** | SQL-based | DataFrame-based (Pythonic) |
| **Memory** | In-process SQL database | Pure DataFrame library |
| **Performance** | Excellent for SQL queries | Excellent for DataFrame operations |
| **File I/O** | COPY command | Native write methods |
| **Lazy Evaluation** | Yes (via SQL) | Yes (via scan_* methods) |

## Advantages of Polars

1. **Pythonic API**: More intuitive for Python developers
2. **Memory Efficient**: Optimized memory usage with lazy evaluation
3. **Fast**: Written in Rust, comparable speed to DuckDB
4. **No SQL Required**: Pure Python DataFrame operations
5. **Better Type System**: Strong typing with automatic schema inference

## Example Queries

### Reading Parquet Files
```python
import polars as pl

# Lazy reading (memory efficient)
df = pl.scan_parquet('parquet_flattened_polars/*.parquet')

# Eager reading
df = pl.read_parquet('parquet_flattened_polars/*.parquet')
```

### Aggregations
```python
# Count records by council
result = df.group_by('council_name').agg(pl.count())

# Multiple aggregations
result = df.group_by('council_name').agg([
    pl.count().alias('record_count'),
    pl.col('INSPIREID').n_unique().alias('unique_ids')
])
```

### Filtering
```python
# Filter by council
df_filtered = df.filter(pl.col('council_name') == 'Birmingham_City_Council')

# Multiple conditions
df_filtered = df.filter(
    (pl.col('council_name').str.contains('London')) &
    (pl.col('VALIDFROM') > '2020-01-01')
)
```

## Performance Comparison

Both Polars and DuckDB offer excellent performance. Choose based on your preference:

- **Use Polars if**: You prefer DataFrame-style operations and Pythonic syntax
- **Use DuckDB if**: You prefer SQL queries and need database-like features

## Data Flow

```
GML Files (extracted_gml/)
    ↓
flatten_gml_to_parquet.py (Polars processing)
    ↓
Parquet Files (parquet_flattened_polars/)
    ↓
Analysis with Polars DataFrame operations
```

## Notes

- Reads GML files from `../Duck_DB/extracted_gml/` directory
- Outputs to `./parquet_flattened_polars/` directory
- Parallel processing using ProcessPoolExecutor
- Automatic schema inference from GML data
- Council name extracted from filename and added as column
