# Data Reconciliation Tools

## Overview

The data reconciliation tools compare two versions of dataset files (CSV or JSON) to identify differences in records, fields, and values. These tools are particularly useful for validating data transformations and ensuring data integrity between original and processed datasets.

## Tools

### 1. CSV Reconciliation (`reconcile_csv.py`)

Compares two CSV files and generates a detailed reconciliation report.

**Location:** `unit_tests/data_recon/reconcile_csv.py`

**Usage:**
```bash
cd unit_tests/data_recon
python3 reconcile_csv.py
```

**Configuration:**
Edit the file paths in the script:
```python
file1 = "data/central-activities-zone_new.csv"
file2 = "data/central-activities-zone_original.csv"
```

### 2. JSON Reconciliation (`reconcile_json.py`)

Compares two JSON files and generates a detailed reconciliation report.

**Location:** `unit_tests/data_recon/reconcile_json.py`

**Usage:**
```bash
cd unit_tests/data_recon
python3 reconcile_json.py
```

**Configuration:**
Edit the file paths in the script:
```python
file1 = "data/central-activities-zone_new.json"
file2 = "data/central-activities-zone_original.json"
```

## Features

### Comparison Capabilities

- **Header/Field Comparison**: Identifies fields present in one file but not the other
- **Record Comparison**: Detects records that exist in only one file
- **Value Comparison**: Finds differences in field values for common records
- **Geometry Analysis**: Special handling for point geometry fields with distance calculations

### Point Geometry Handling

The tools include intelligent point geometry comparison:

- Calculates actual geometric distance between points (not just string comparison)
- Ignores insignificant differences due to decimal precision variations
- **Threshold**: 0.0001 degrees (~11 meters)
- Only reports differences exceeding the threshold as significant

This prevents false positives when comparing files with different decimal precision in WKT point data.

## Output Files

Both tools generate timestamped output files:

### Main Report
- **Format**: `YYYYMMDD_HHMMSS_reconciliation_report[_json].txt`
- **Contents**:
  - File comparison summary
  - Header/field differences
  - Missing records
  - Field value differences
  - Point geometry analysis (if applicable)
  - Overall summary

### Significant Point Differences
- **CSV**: `YYYYMMDD_HHMMSS_significant_point_differences.csv`
- **JSON**: `YYYYMMDD_HHMMSS_significant_point_differences.json`
- **Contents**: Detailed list of point geometries with significant distance differences
- **Only generated if**: Significant point differences are found

## Report Sections

### 1. Header Comparison
Lists fields/columns present in each file and identifies differences.

### 2. Missing Records
Shows entity IDs that exist in only one file.

### 3. Field Differences
Counts mismatches for each field across common records.

### 4. Point Geometry Analysis
- Total point comparisons
- Distance statistics (max, min, average)
- Count of significant differences
- Explanation of threshold

### 5. Summary
Quick overview of whether files are identical or have differences.

## Requirements

```bash
pip install shapely
```

## Example Output

```
================================================================================
CSV DATA RECONCILIATION REPORT
================================================================================

File 1: data/central-activities-zone_new.csv
File 2: data/central-activities-zone_original.csv

Total records in File 1: 150
Total records in File 2: 150
Common entities: 150

--------------------------------------------------------------------------------
HEADER COMPARISON
--------------------------------------------------------------------------------
Columns in File 1: 25
Columns in File 2: 25
Common columns: 25

--------------------------------------------------------------------------------
MISSING RECORDS
--------------------------------------------------------------------------------
Only in File 1: 0
Only in File 2: 0

--------------------------------------------------------------------------------
FIELD DIFFERENCES IN COMMON RECORDS
--------------------------------------------------------------------------------
Records with differences: 0

Point Geometry Analysis:
  Total point comparisons: 150
  Max distance: 0.0000000500 degrees
  Min distance: 0.0000000000 degrees
  Avg distance: 0.0000000125 degrees
  Significant differences (>=0.0001): 0

  Note: A significant difference is defined as a distance >= 0.0001 degrees
  (~11 meters). Differences below this threshold are considered negligible and
  likely due to floating-point precision variations during data processing.

================================================================================
SUMMARY
================================================================================
✓ Files are identical
```

## Best Practices

1. **Place data files** in the `data/` subdirectory
2. **Review the threshold** for point geometry if your use case requires different precision
3. **Check both reports**: Main report for overview, detailed CSV/JSON for investigation
4. **Archive reports** with timestamps for historical comparison
5. **Update file paths** in scripts before running

## Troubleshooting

### Large Geometry Fields Error
If you encounter field size limit errors with large WKT geometries:
- The scripts already set `csv.field_size_limit(10485760)` (10MB)
- Increase this value if needed for larger geometries

### Memory Issues
For very large files:
- Consider processing in chunks
- Use sampling for initial validation
- Increase available memory

## Integration

These tools can be integrated into:
- CI/CD pipelines for data validation
- Post-processing verification steps
- Data quality monitoring workflows
- Regression testing for data transformations
