#!/usr/bin/env python3
from pathlib import Path
import duckdb

def convert_gml_to_parquet(extracted_dir, parquet_dir):
    """Convert GML files to Parquet format"""
    parquet_dir.mkdir(exist_ok=True)
    
    gml_files = list(extracted_dir.rglob("*.gml"))
    print(f"Converting {len(gml_files)} GML files to Parquet...\n")
    
    con = duckdb.connect()
    
    # Try spatial extension
    try:
        con.execute("INSTALL spatial FROM core;")
        con.execute("LOAD spatial;")
        use_spatial = True
        print("Using spatial extension\n")
    except:
        use_spatial = False
        print("Spatial extension unavailable, using XML parsing\n")
    
    for i, gml_file in enumerate(gml_files, 1):
        council = gml_file.parent.name
        parquet_file = parquet_dir / f"{council}.parquet"
        
        print(f"[{i}/{len(gml_files)}] {council} -> {parquet_file.name}")
        
        try:
            if use_spatial:
                con.execute(f"COPY (SELECT * FROM ST_Read('{gml_file}')) TO '{parquet_file}' (FORMAT PARQUET);")
            else:
                # Fallback: read as XML text
                with open(gml_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                con.execute(f"""
                    COPY (SELECT 
                        '{council}' as council_name,
                        '{gml_file.name}' as file_name,
                        ? as xml_content
                    ) TO '{parquet_file}' (FORMAT PARQUET);
                """, [content])
            
            print(f"  ✓ {parquet_file.stat().st_size:,} bytes")
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    con.close()

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    extracted_dir = base_dir / "extracted_gml"
    parquet_dir = base_dir / "parquet_output"
    
    convert_gml_to_parquet(extracted_dir, parquet_dir)
    
    # Summary
    parquet_files = list(parquet_dir.glob("*.parquet"))
    total_size = sum(f.stat().st_size for f in parquet_files)
    
    print(f"\n{'='*60}")
    print(f"Conversion complete!")
    print(f"Output directory: {parquet_dir}")
    print(f"Parquet files: {len(parquet_files)}")
    print(f"Total size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    print(f"\nQuery with DuckDB:")
    print(f"  con = duckdb.connect()")
    print(f"  con.execute(\"SELECT * FROM '{parquet_dir}/*.parquet'\").df()")
    print(f"{'='*60}")
