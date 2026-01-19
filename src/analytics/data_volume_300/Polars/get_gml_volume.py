#!/usr/bin/env python3
import io
import sys
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import polars as pl
import requests
from bs4 import BeautifulSoup


def get_zip_files(url):
    """Scrape .zip file links from the download page"""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    zip_files = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if ".zip" in href.lower():
            full_url = urljoin(url, href)
            zip_files.append(full_url)

    return list(set(zip_files))


def download_and_analyze_zip(zip_url, download_dir):
    """Download zip and calculate size of .gml files inside"""
    try:
        zip_name = Path(zip_url).name
        zip_path = download_dir / zip_name

        # Download zip file
        response = requests.get(zip_url, timeout=60)
        response.raise_for_status()

        # Save to downloads folder
        with open(zip_path, "wb") as f:
            f.write(response.content)

        # Analyze .gml files
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            gml_files = [f for f in zf.namelist() if f.lower().endswith(".gml")]
            total_size = sum(zf.getinfo(f).file_size for f in gml_files)
            return total_size, len(gml_files), gml_files
    except Exception as e:
        print(f"✗ Error: {e}")
        return 0, 0, []


def format_size(bytes_size):
    """Format bytes to human readable format"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0


if __name__ == "__main__":
    base_dir = Path(__file__).parent.parent / "Duck_DB"
    download_dir = base_dir / "downloads"
    download_dir.mkdir(exist_ok=True)

    url = "https://use-land-property-data.service.gov.uk/datasets/inspire/download"

    print(f"Fetching .zip files from: {url}\n")
    zip_files = get_zip_files(url)
    print(f"Found {len(zip_files)} .zip files\n")

    if not zip_files:
        print("No .zip files found!")
        sys.exit(1)

    print(f"Downloading to: {download_dir}")
    print("Analyzing .gml files in zip archives...\n")

    data = []
    total_gml_size = 0
    total_gml_count = 0

    for i, zip_url in enumerate(zip_files, 1):
        zip_name = Path(zip_url).name
        print(f"[{i}/{len(zip_files)}] {zip_name}...", end=" ")

        size, count, gml_list = download_and_analyze_zip(zip_url, download_dir)
        total_gml_size += size
        total_gml_count += count

        data.append(
            {
                "zip_name": zip_name,
                "gml_count": count,
                "gml_size": size,
                "gml_files": ", ".join(gml_list) if gml_list else "",
            }
        )

        print(f"✓ {count} .gml files, {format_size(size)}")

    # Create Polars DataFrame
    df = pl.DataFrame(data)

    print(f"\n{'='*60}")
    print(f"Total .zip files: {len(data)}")
    print(f"Total .gml files: {total_gml_count}")
    print(
        f"Total .gml data volume: {total_gml_size:,} bytes ({format_size(total_gml_size)})"
    )
    print(f"{'='*60}")

    # Save report as CSV using Polars
    output_csv = Path(__file__).parent / "gml_volume_report.csv"
    df.write_csv(output_csv)

    # Save report as Parquet using Polars
    output_parquet = Path(__file__).parent / "gml_volume_report.parquet"
    df.write_parquet(output_parquet)

    # Save text report
    output_file = Path(__file__).parent / "gml_volume_report.txt"
    with open(output_file, "w") as f:
        f.write("GML Files Data Volume Report\n")
        f.write(f"Source: {url}\n")
        f.write(f"{'='*60}\n\n")

        for row in df.iter_rows(named=True):
            f.write(f"\n{row['zip_name']}:\n")
            f.write(f"  GML files: {row['gml_count']}\n")
            f.write(
                f"  Size: {row['gml_size']:,} bytes ({format_size(row['gml_size'])})\n"
            )
            if row["gml_files"]:
                f.write(f"  Files: {row['gml_files']}\n")

        f.write(f"\n{'='*60}\n")
        f.write("SUMMARY\n")
        f.write(f"{'='*60}\n")
        f.write(f"Total .zip files: {len(data)}\n")
        f.write(f"Total .gml files: {total_gml_count}\n")
        f.write(
            f"Total data volume: {total_gml_size:,} bytes ({format_size(total_gml_size)})\n"
        )

    print("\nReports saved to:")
    print(f"  Text: {output_file}")
    print(f"  CSV: {output_csv}")
    print(f"  Parquet: {output_parquet}")
    print(f"Downloads saved to: {download_dir}")
