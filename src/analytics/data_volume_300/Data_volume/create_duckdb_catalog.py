#!/usr/bin/env python3
import duckdb
from pathlib import Path

base_dir = Path(__file__).parent
parquet_dir = base_dir / "parquet_output"
db_path = base_dir / "parquet_catalog.duckdb"

con = duckdb.connect(str(db_path))

# Create view pointing to all parquet files
con.execute(f"""
    CREATE OR REPLACE VIEW all_gml_data AS 
    SELECT * FROM '{parquet_dir}/*.parquet'
""")

print(f"Database created: {db_path}")
print(f"\nConnect in DBeaver to: {db_path}")
print(f"\nQuery with: SELECT * FROM all_gml_data;")

con.close()
