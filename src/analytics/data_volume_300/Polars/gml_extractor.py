#!/usr/bin/env python3
import logging
import os
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path


class GMLFileExtractor:
    def __init__(self, downloads_dir=None, extracted_dir=None):
        base_dir = Path(__file__).parent.parent / "Duck_DB"
        self.downloads_dir = (
            Path(downloads_dir) if downloads_dir else base_dir / "downloads"
        )
        self.extracted_dir = (
            Path(extracted_dir)
            if extracted_dir
            else Path(__file__).parent / "extracted_gml"
        )
        self.extracted_dir.mkdir(exist_ok=True)

        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger(__name__)

    def extract_all(self, max_workers=None):
        """Extract all .gml files from zip files in downloads directory with parallel processing"""
        zip_files = sorted(self.downloads_dir.glob("*.zip"))

        if not zip_files:
            self.logger.warning(f"No zip files found in {self.downloads_dir}")
            return

        if not max_workers:
            max_workers = max(1, min(os.cpu_count() or 4, len(zip_files)))

        self.logger.info(
            f"Found {len(zip_files)} zip files to extract with {max_workers} workers"
        )

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._extract_gml_static, zip_file, self.extracted_dir
                ): zip_file
                for zip_file in zip_files
            }

            extracted_count = 0
            for i, future in enumerate(as_completed(futures), 1):
                count, msg = future.result()
                extracted_count += count
                self.logger.info(f"[{i}/{len(zip_files)}] {msg}")

        self.logger.info(f"Total extracted: {extracted_count} .gml files")

    @staticmethod
    def _extract_gml_static(zip_path, extracted_dir):
        """Static method to extract .gml files from a single zip file (for parallel processing)"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                gml_files = [f for f in zf.namelist() if f.lower().endswith(".gml")]

                if not gml_files:
                    return 0, f"No .gml files in {zip_path.name}"

                council_name = zip_path.stem
                for gml_file in gml_files:
                    gml_filename = Path(gml_file).name
                    new_filename = f"{council_name}_{gml_filename}"

                    with zf.open(gml_file) as source:
                        target_path = extracted_dir / new_filename
                        with open(target_path, "wb") as target:
                            target.write(source.read())

                return len(gml_files), f"✓ {zip_path.name} -> {len(gml_files)} file(s)"

        except Exception as e:
            return 0, f"✗ Error extracting {zip_path.name}: {e}"


if __name__ == "__main__":
    extractor = GMLFileExtractor()
    extractor.extract_all()
