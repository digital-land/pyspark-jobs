#!/usr/bin/env python3
import duckdb
from pathlib import Path

def load_gml_data(db_path, extracted_dir):
    """Load actual GML data into DuckDB"""
    con = duckdb.connect(str(db_path))
    
    # Try to install spatial extension with fallback
    try:
        con.execute("INSTALL spatial FROM core;")
        con.execute("LOAD spatial;")
        use_spatial = True
    except:
        print("Spatial extension not available, loading as raw XML")
        use_spatial = False
    
    gml_files = list(extracted_dir.rglob("*.gml"))
    print(f"Loading {len(gml_files)} GML files...\n")
    
    if use_spatial:
        # Load using spatial extension
        for i, gml_file in enumerate(gml_files, 1):
            table_name = f"gml_{gml_file.parent.name}".replace('-', '_').replace(' ', '_')
            print(f"[{i}/{len(gml_files)}] {table_name}")
            try:
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM ST_Read('{gml_file}');")
            except Exception as e:
                print(f"  ✗ {e}")
    else:
        # Load as text/blob
        con.execute("""
            CREATE TABLE gml_data (
                council_name VARCHAR,
                file_path VARCHAR,
                xml_content VARCHAR
            )
        """)
        
        for i, gml_file in enumerate(gml_files, 1):
            council = gml_file.parent.name
            print(f"[{i}/{len(gml_files)}] {council}")
            try:
                with open(gml_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                con.execute("INSERT INTO gml_data VALUES (?, ?, ?)", 
                           [council, str(gml_file), content])
            except Exception as e:
                print(f"  ✗ {e}")
    
    return con

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    extracted_dir = base_dir / "extracted_gml"
    db_path = base_dir / "gml_full_data.duckdb"
    
    con = load_gml_data(db_path, extracted_dir)
    
    tables = con.execute("SHOW TABLES;").fetchall()
    
    print(f"\n{'='*60}")
    print(f"Database: {db_path}")
    print(f"Tables created: {len(tables)}")
    print(f"Connection: con = duckdb.connect('{db_path}')")
    print(f"{'='*60}")
    
    con.close()
