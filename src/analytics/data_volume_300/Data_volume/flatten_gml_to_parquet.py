#!/usr/bin/env python3
from pathlib import Path
from lxml import etree
import pandas as pd

def flatten_gml(gml_file):
    """Parse GML and flatten to records"""
    tree = etree.parse(str(gml_file))
    root = tree.getroot()
    
    # Remove namespace for easier parsing
    for elem in root.iter():
        if not hasattr(elem.tag, 'find'): continue
        elem.tag = etree.QName(elem).localname
    
    records = []
    
    # Find all feature members (parcels)
    for feature in root.findall('.//featureMember') or root.findall('.//member'):
        record = {}
        
        # Extract all child elements
        for elem in feature.iter():
            tag = etree.QName(elem).localname
            if elem.text and elem.text.strip():
                record[tag] = elem.text.strip()
            
            # Extract attributes
            for attr, val in elem.attrib.items():
                attr_name = f"{tag}_{etree.QName(attr).localname}"
                record[attr_name] = val
        
        if record:
            records.append(record)
    
    return pd.DataFrame(records) if records else pd.DataFrame()

def convert_to_parquet(extracted_dir, parquet_dir):
    """Convert GML files to flattened Parquet"""
    parquet_dir.mkdir(exist_ok=True)
    
    gml_files = list(extracted_dir.rglob("*.gml"))
    print(f"Converting {len(gml_files)} GML files to flattened Parquet...\n")
    
    for i, gml_file in enumerate(gml_files, 1):
        council = gml_file.parent.name
        parquet_file = parquet_dir / f"{council}.parquet"
        
        print(f"[{i}/{len(gml_files)}] {council}")
        
        try:
            df = flatten_gml(gml_file)
            
            if not df.empty:
                df['council_name'] = council
                df.to_parquet(parquet_file, index=False)
                print(f"  ✓ {len(df)} records, {parquet_file.stat().st_size:,} bytes")
            else:
                print(f"  ⚠ No records found")
        except Exception as e:
            print(f"  ✗ Error: {e}")

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    extracted_dir = base_dir / "extracted_gml"
    parquet_dir = base_dir / "parquet_flattened"
    
    convert_to_parquet(extracted_dir, parquet_dir)
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    total_size = sum(f.stat().st_size for f in parquet_files)
    
    print(f"\n{'='*60}")
    print(f"Output: {parquet_dir}")
    print(f"Files: {len(parquet_files)}")
    print(f"Size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    print(f"\nDBeaver connection:")
    print(f"  Database: {base_dir}/parquet_catalog.duckdb")
    print(f"  Query: SELECT * FROM '{parquet_dir}/*.parquet' LIMIT 100;")
    print(f"{'='*60}")
