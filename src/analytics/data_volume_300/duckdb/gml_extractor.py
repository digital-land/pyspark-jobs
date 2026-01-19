#!/usr/bin/env python3
import logging
import zipfile
from pathlib import Path


class GMLFileExtractor:
    def __init__(self, downloads_dir=None, extracted_dir=None):
        base_dir = Path(__file__).parent
        self.downloads_dir = (
            Path(downloads_dir) if downloads_dir else base_dir / "downloads"
        )
        self.extracted_dir = (
            Path(extracted_dir) if extracted_dir else base_dir / "extracted_gml"
        )
        self.extracted_dir.mkdir(exist_ok=True)

        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger(__name__)

    def extract_all(self):
        """Extract all .gml files from zip files in downloads directory"""
        zip_files = sorted(self.downloads_dir.glob("*.zip"))

        if not zip_files:
            self.logger.warning(f"No zip files found in {self.downloads_dir}")
            return

        self.logger.info(f"Found {len(zip_files)} zip files to extract")

        extracted_count = 0
        for zip_file in zip_files:
            count = self._extract_gml(zip_file)
            extracted_count += count

        self.logger.info(f"Total extracted: {extracted_count} .gml files")

    def _extract_gml(self, zip_path):
        """Extract .gml files from a single zip file"""
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                gml_files = [f for f in zf.namelist() if f.lower().endswith(".gml")]

                if not gml_files:
                    self.logger.info(f"No .gml files in {zip_path.name}")
                    return 0

                council_name = zip_path.stem
                for gml_file in gml_files:
                    gml_filename = Path(gml_file).name
                    new_filename = f"{council_name}_{gml_filename}"

                    with zf.open(gml_file) as source:
                        target_path = self.extracted_dir / new_filename
                        with open(target_path, "wb") as target:
                            target.write(source.read())

                self.logger.info(f"✓ {zip_path.name} -> {len(gml_files)} file(s)")
                return len(gml_files)

        except Exception as e:
            self.logger.error(f"✗ Error extracting {zip_path.name}: {e}")
            return 0


if __name__ == "__main__":
    extractor = GMLFileExtractor()
    extractor.extract_all()
