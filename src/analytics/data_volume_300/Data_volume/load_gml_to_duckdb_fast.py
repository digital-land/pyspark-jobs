#!/usr/bin/env python3
import duckdb
from pathlib import Path
import zipfile

def extract_gml_files(download_dir, extract_dir):
    """Extract all .gml files from zip archives"""
    extract_dir.mkdir(exist_ok=True)
    gml_files = []
    
    for zip_path in download_dir.glob("*.zip"):
        # Create subfolder for each zip to avoid overwrites
        zip_folder = extract_dir / zip_path.stem
        zip_folder.mkdir(exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for file in zf.namelist():
                if file.lower().endswith('.gml'):
                    zf.extract(file, zip_folder)
                    gml_files.append(zip_folder / file)
    
    return gml_files

def load_gml_to_duckdb(gml_files, db_path):
    """Load GML file metadata into DuckDB (fast)"""
    con = duckdb.connect(str(db_path))
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS gml_files (
            file_name VARCHAR,
            file_path VARCHAR,
            file_size BIGINT,
            zip_source VARCHAR
        )
    """)
    
    print(f"\nLoading {len(gml_files)} GML file metadata...")
    
    # Batch insert all files at once
    data = []
    for gml_file in gml_files:
        data.append({
            'file_name': gml_file.name,
            'file_path': str(gml_file),
            'file_size': gml_file.stat().st_size,
            'zip_source': gml_file.parent.name
        })
    
    con.executemany("INSERT INTO gml_files VALUES (?, ?, ?, ?)", 
                    [(d['file_name'], d['file_path'], d['file_size'], d['zip_source']) for d in data])
    
    return con

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    download_dir = base_dir / "downloads"
    extract_dir = base_dir / "extracted_gml"
    db_path = base_dir / "inspire_gml_data.duckdb"
    
    print("Extracting GML files from zip archives...")
    gml_files = extract_gml_files(download_dir, extract_dir)
    print(f"Extracted {len(gml_files)} GML files\n")
    
    con = load_gml_to_duckdb(gml_files, db_path)
    
    # Get stats
    stats = con.execute("""
        SELECT 
            COUNT(*) as total_files,
            SUM(file_size) as total_size,
            AVG(file_size) as avg_size,
            MAX(file_size) as max_size
        FROM gml_files
    """).fetchone()
    
    print(f"\n{'='*60}")
    print("DUCKDB CONNECTION DETAILS")
    print(f"{'='*60}")
    print(f"Database: {db_path}")
    print(f"Total GML files: {stats[0]:,}")
    print(f"Total size: {stats[1]:,} bytes ({stats[1]/1024/1024:.2f} MB)")
    print(f"Average file size: {stats[2]:,.0f} bytes")
    print(f"Largest file: {stats[3]:,} bytes")
    print(f"\nConnection:")
    print(f"  import duckdb")
    print(f"  con = duckdb.connect('{db_path}')")
    print(f"\nQuery example:")
    print(f"  con.execute('SELECT * FROM gml_files LIMIT 10').df()")
    print(f"{'='*60}")
    
    # Save connection details
    details_file = base_dir / "duckdb_connection_details.txt"
    with open(details_file, 'w') as f:
        f.write("DuckDB Connection Details\n")
        f.write("="*60 + "\n\n")
        f.write(f"Database Path: {db_path}\n")
        f.write(f"Total GML files: {stats[0]:,}\n")
        f.write(f"Total size: {stats[1]:,} bytes ({stats[1]/1024/1024:.2f} MB)\n\n")
        f.write(f"Connection String: duckdb:///{db_path}\n\n")
        f.write("Python Connection:\n")
        f.write(f"  import duckdb\n")
        f.write(f"  con = duckdb.connect('{db_path}')\n\n")
        f.write("Query Examples:\n")
        f.write("  con.execute('SELECT * FROM gml_files').df()\n")
        f.write("  con.execute('SELECT COUNT(*) FROM gml_files').fetchone()\n")
    
    print(f"\nConnection details saved to: {details_file}")
    con.close()
