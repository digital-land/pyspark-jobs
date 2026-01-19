# DuckDB Implementation for GML Data Processing

This folder contains DuckDB-based implementations for processing GML (Geography Markup Language) cadastral data from UK Land Registry.

## Files

### 1. `get_gml_volume.py`
Downloads and analyzes GML data volume from UK Land Registry INSPIRE datasets.

**Features:**
- Scrapes zip file links from the download page
- Downloads all zip files to `downloads/` directory
- Analyzes GML file sizes within each zip
- Generates comprehensive volume report

**Usage:**
```bash
python get_gml_volume.py
```

**Output:**
- `downloads/` - Downloaded zip files (318 councils)
- `gml_volume_report.txt` - Detailed volume analysis report

### 2. `gml_extractor.py`
Extracts GML files from downloaded zip archives with parallel processing.

**Features:**
- Parallel extraction using ProcessPoolExecutor
- Prefixes each GML file with council name for uniqueness
- Progress tracking and error handling

**Usage:**
```bash
python gml_extractor.py
```

**Output:**
- `extracted_gml/` - Extracted GML files (one per council)

### 3. `flatten_gml_to_parquet.py`
Converts GML files to Parquet format using DuckDB for efficient storage and querying.

**Features:**
- Parses XML/GML structure and flattens to tabular format
- Parallel processing for faster conversion
- Uses DuckDB's COPY command for efficient Parquet writing
- Adds council_name column for identification

**Usage:**
```bash
python flatten_gml_to_parquet.py
```

**Output:**
- `parquet_flattened_duckdb/` - Parquet files (one per council)

**DuckDB Queries:**
```sql
-- Read all parquet files
SELECT * FROM 'parquet_flattened_duckdb/*.parquet' LIMIT 100;

-- Count records by council
SELECT council_name, COUNT(*) as record_count 
FROM 'parquet_flattened_duckdb/*.parquet' 
GROUP BY council_name 
ORDER BY record_count DESC;

-- Total records
SELECT COUNT(*) FROM 'parquet_flattened_duckdb/*.parquet';
```

## Installation

```bash
pip install -r requirements.txt
```

**Dependencies:**
- duckdb
- pandas
- lxml
- requests
- beautifulsoup4

## Data Flow

```
UK Land Registry INSPIRE Download
    ↓
get_gml_volume.py (Download & Analyze)
    ↓
downloads/ (318 zip files)
    ↓
gml_extractor.py (Extract)
    ↓
extracted_gml/ (318 GML files)
    ↓
flatten_gml_to_parquet.py (Convert with DuckDB)
    ↓
parquet_flattened_duckdb/ (318 Parquet files)
    ↓
DuckDB SQL Queries
```

## DuckDB Advantages

1. **SQL Interface**: Query data using familiar SQL syntax
2. **Efficient Parquet I/O**: Optimized reading/writing of Parquet files
3. **In-Process Database**: No server setup required
4. **Columnar Storage**: Fast analytical queries
5. **Zero-Copy Integration**: Works seamlessly with Pandas DataFrames

## Example Workflow

```bash
# Step 1: Download data
python get_gml_volume.py

# Step 2: Extract GML files
python gml_extractor.py

# Step 3: Convert to Parquet
python flatten_gml_to_parquet.py

# Step 4: Query with DuckDB
duckdb
```

```sql
-- In DuckDB CLI
D SELECT council_name, COUNT(*) as parcels 
  FROM 'parquet_flattened_duckdb/*.parquet' 
  GROUP BY council_name 
  ORDER BY parcels DESC 
  LIMIT 10;
```

## Performance

- **Parallel Processing**: Utilizes multiple CPU cores
- **Memory Efficient**: Streams data without loading everything into memory
- **Fast Parquet I/O**: DuckDB's optimized Parquet reader/writer
- **Typical Processing Time**: ~2-5 minutes for 318 councils (depends on CPU)

## Data Schema

Each Parquet file contains flattened GML data with columns:
- `council_name` - Council identifier
- `INSPIREID` - Unique parcel ID
- `LABEL` - Parcel label
- `NATIONALCADASTRALREFERENCE` - National reference
- `VALIDFROM` - Valid from date
- `BEGINLIFESPANVERSION` - Lifespan version timestamp
- `GEOMETRY` - Geometry coordinates (as text)
- Additional XML attributes as needed

## Archive Folder

The `archive/` folder contains older versions and experimental scripts:
- `calculate_gml_volume.py` - Legacy volume calculation
- `convert_gml_to_parquet.py` - Earlier conversion approach
- `create_duckdb_catalog.py` - Database catalog creation
- `load_gml_to_duckdb.py` - Direct DuckDB loading attempts

## Notes

- GML files are large XML files containing cadastral parcel data
- Each council has one GML file with multiple parcels
- Parquet format reduces storage by ~70-80% compared to GML
- DuckDB enables fast SQL queries on Parquet files without loading into memory

## Troubleshooting

**Issue**: No zip files found
- Check internet connection
- Verify URL is accessible: https://use-land-property-data.service.gov.uk/datasets/inspire/download

**Issue**: Out of memory during conversion
- Reduce `max_workers` parameter in scripts
- Process councils in batches

**Issue**: DuckDB import error
- Ensure DuckDB is installed: `pip install duckdb`
- Check Python version compatibility (3.8+)

## Related

See `../Polars/` folder for Polars-based implementation (alternative to DuckDB).
