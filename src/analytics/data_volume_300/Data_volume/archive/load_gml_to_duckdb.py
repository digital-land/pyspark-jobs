#!/usr/bin/env python3
import duckdb
from pathlib import Path
import zipfile
from lxml import etree

def extract_gml_files(download_dir, extract_dir):
    """Extract all .gml files from zip archives"""
    extract_dir.mkdir(exist_ok=True)
    gml_files = []
    
    for zip_path in download_dir.glob("*.zip"):
        print(f"Extracting {zip_path.name}...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for file in zf.namelist():
                if file.lower().endswith('.gml'):
                    zf.extract(file, extract_dir)
                    gml_files.append(extract_dir / file)
    
    return gml_files

def parse_gml_to_dict(gml_file):
    """Parse GML file and extract basic info"""
    tree = etree.parse(str(gml_file))
    root = tree.getroot()
    
    data = []
    for elem in root.iter():
        if elem.text and elem.text.strip():
            data.append({
                'tag': etree.QName(elem).localname,
                'value': elem.text.strip(),
                'attributes': dict(elem.attrib)
            })
    return data

def load_gml_to_duckdb(gml_files, db_path):
    """Load GML files into DuckDB"""
    con = duckdb.connect(str(db_path))
    
    # Create metadata table
    con.execute("""
        CREATE TABLE IF NOT EXISTS gml_files (
            file_name VARCHAR,
            file_path VARCHAR,
            file_size BIGINT,
            element_count INTEGER
        )
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS gml_elements (
            file_name VARCHAR,
            tag VARCHAR,
            value VARCHAR,
            attributes VARCHAR
        )
    """)
    
    print(f"\nLoading {len(gml_files)} GML files into DuckDB...")
    
    for i, gml_file in enumerate(gml_files, 1):
        try:
            print(f"[{i}/{len(gml_files)}] Loading {gml_file.name}")
            
            elements = parse_gml_to_dict(gml_file)
            file_size = gml_file.stat().st_size
            
            con.execute(
                "INSERT INTO gml_files VALUES (?, ?, ?, ?)",
                [gml_file.name, str(gml_file), file_size, len(elements)]
            )
            
            for elem in elements:
                con.execute(
                    "INSERT INTO gml_elements VALUES (?, ?, ?, ?)",
                    [gml_file.name, elem['tag'], elem['value'], str(elem['attributes'])]
                )
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    return con

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    download_dir = base_dir / "downloads"
    extract_dir = base_dir / "extracted_gml"
    db_path = base_dir / "inspire_gml_data.duckdb"
    
    # Extract GML files
    print("Extracting GML files from zip archives...")
    gml_files = extract_gml_files(download_dir, extract_dir)
    print(f"Extracted {len(gml_files)} GML files\n")
    
    # Load into DuckDB
    con = load_gml_to_duckdb(gml_files, db_path)
    
    # Get connection details
    tables = con.execute("SHOW TABLES;").fetchall()
    
    print(f"\n{'='*60}")
    print("DUCKDB CONNECTION DETAILS")
    print(f"{'='*60}")
    print(f"Database Path: {db_path}")
    print(f"Database Size: {db_path.stat().st_size:,} bytes")
    print(f"Total Tables: {len(tables)}")
    print(f"\nConnection String: duckdb:///{db_path}")
    print(f"\nPython Connection:")
    print(f"  import duckdb")
    print(f"  con = duckdb.connect('{db_path}')")
    print(f"{'='*60}")
    
    if tables:
        print(f"\nTables loaded:")
        for table in tables[:10]:
            print(f"  - {table[0]}")
        if len(tables) > 10:
            print(f"  ... and {len(tables) - 10} more")
    
    # Save connection details
    details_file = base_dir / "duckdb_connection_details.txt"
    with open(details_file, 'w') as f:
        f.write("DuckDB Connection Details\n")
        f.write("="*60 + "\n\n")
        f.write(f"Database Path: {db_path}\n")
        f.write(f"Database Size: {db_path.stat().st_size:,} bytes\n")
        f.write(f"Total Tables: {len(tables)}\n\n")
        f.write(f"Connection String: duckdb:///{db_path}\n\n")
        f.write("Python Connection:\n")
        f.write(f"  import duckdb\n")
        f.write(f"  con = duckdb.connect('{db_path}')\n\n")
        f.write("Tables:\n")
        for table in tables:
            f.write(f"  - {table[0]}\n")
    
    print(f"\nConnection details saved to: {details_file}")
    con.close()
