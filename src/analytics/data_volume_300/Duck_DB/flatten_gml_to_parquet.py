#!/usr/bin/env python3
from pathlib import Path
from lxml import etree
import duckdb
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

def flatten_gml(gml_file):
    """Parse GML and flatten to records"""
    tree = etree.parse(str(gml_file))
    root = tree.getroot()
    
    for elem in root.iter():
        if not hasattr(elem.tag, 'find'): continue
        elem.tag = etree.QName(elem).localname
    
    records = []
    for feature in root.findall('.//featureMember') or root.findall('.//member'):
        record = {}
        for elem in feature.iter():
            tag = etree.QName(elem).localname
            if elem.text and elem.text.strip():
                record[tag] = elem.text.strip()
            for attr, val in elem.attrib.items():
                record[f"{tag}_{etree.QName(attr).localname}"] = val
        if record:
            records.append(record)
    
    return records

def process_single_file(args):
    """Process a single GML file to Parquet"""
    gml_file, parquet_dir = args
    council = gml_file.parent.name
    parquet_file = parquet_dir / f"{council}.parquet"
    
    try:
        records = flatten_gml(gml_file)
        if records:
            df = pd.DataFrame(records)
            df['council_name'] = council
            duckdb.sql(f"COPY (SELECT * FROM df) TO '{parquet_file}' (FORMAT PARQUET)")
            size = parquet_file.stat().st_size
            return (council, len(records), size, None)
        return (council, 0, 0, "No records")
    except Exception as e:
        return (council, 0, 0, str(e))

def convert_to_parquet(extracted_dir, parquet_dir, max_workers=None):
    """Convert GML files to Parquet using DuckDB with parallel processing"""
    parquet_dir.mkdir(exist_ok=True)
    
    gml_files = list(extracted_dir.rglob("*.gml"))
    if not max_workers:
        max_workers = min(os.cpu_count() or 4, len(gml_files))
    
    print(f"Converting {len(gml_files)} GML files with {max_workers} workers...\n")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_file, (gml_file, parquet_dir)): gml_file 
                   for gml_file in gml_files}
        
        for i, future in enumerate(as_completed(futures), 1):
            council, count, size, error = future.result()
            print(f"[{i}/{len(gml_files)}] {council}")
            if error:
                print(f"  ✗ Error: {error}")
            elif count:
                print(f"  ✓ {count} records, {size:,} bytes")
            else:
                print(f"  ⚠ No records found")

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    extracted_dir = base_dir / "extracted_gml"
    parquet_dir = base_dir / "parquet_flattened_duckdb"
    
    import time
    start = time.time()
    convert_to_parquet(extracted_dir, parquet_dir)
    elapsed = time.time() - start
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    total_size = sum(f.stat().st_size for f in parquet_files)
    
    print(f"\n{'='*60}")
    print(f"Output: {parquet_dir}")
    print(f"Files: {len(parquet_files)}")
    print(f"Size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    print(f"Time: {elapsed:.2f}s")
    print(f"\nDuckDB queries:")
    print(f"  SELECT * FROM '{parquet_dir}/*.parquet' LIMIT 100;")
    print(f"  SELECT council_name, COUNT(*) FROM '{parquet_dir}/*.parquet' GROUP BY council_name;")
    print(f"{'='*60}")
