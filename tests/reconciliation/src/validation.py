import csv
import json
import geojson
from typing import Dict, Any, Set, List
from datetime import datetime
import os


class FileProcessor:
    def __init__(self):
        pass

    def process_file(self, file_path: str) -> Dict[str, Any]:
        """Process file and return record count and validation status"""
        if file_path.lower().endswith(".csv"):
            return self._process_csv(file_path)
        elif file_path.lower().endswith(".json"):
            return self._process_json(file_path)
        elif file_path.lower().endswith(".geojson"):
            return self._process_geojson(file_path)
        else:
            return {"error": "Unsupported file format"}

    def _process_csv(self, file_path: str) -> Dict[str, Any]:
        try:
            csv.field_size_limit(10485760)  # 10MB limit
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                columns = list(reader.fieldnames) if reader.fieldnames else []
                entities = [row.get("entity", "") for row in rows if row.get("entity")]
                unique_entities = set(entities)
                return {
                    "file_type": "CSV",
                    "record_count": len(rows),
                    "is_valid": len(rows) > 0,
                    "columns": columns,
                    "column_count": len(columns),
                    "entities": unique_entities,
                    "total_entities": len(entities),
                    "unique_entities": len(unique_entities),
                    "duplicate_count": len(entities) - len(unique_entities),
                }
        except Exception as e:
            return {
                "file_type": "CSV",
                "record_count": 0,
                "is_valid": False,
                "error": str(e),
            }

    def _process_json(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                records = []
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, list):
                            records.extend(v)
                columns = set()
                for r in records:
                    if isinstance(r, dict):
                        columns.update(r.keys())
                columns = sorted(list(columns))
                entities = [
                    r.get("entity", "")
                    for r in records
                    if isinstance(r, dict) and r.get("entity")
                ]
                unique_entities = set(entities)
                return {
                    "file_type": "JSON",
                    "record_count": len(records),
                    "is_valid": True,
                    "columns": columns,
                    "column_count": len(columns),
                    "entities": unique_entities,
                    "total_entities": len(entities),
                    "unique_entities": len(unique_entities),
                    "duplicate_count": len(entities) - len(unique_entities),
                }
        except Exception as e:
            return {
                "file_type": "JSON",
                "record_count": 0,
                "is_valid": False,
                "error": str(e),
            }

    def _process_geojson(self, file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Validate using geojson library (RFC 7946 compliant)
            geojson_obj = geojson.loads(json.dumps(data))

            if not geojson_obj.is_valid:
                errors = geojson_obj.errors()
                return {
                    "file_type": "GeoJSON",
                    "record_count": 0,
                    "is_valid": False,
                    "error": f"Invalid GeoJSON: {errors}",
                    "validation_standard": "RFC 7946",
                }

            features = []
            if data.get("type") == "FeatureCollection":
                features = data.get("features", [])
            else:
                features = [data]

            columns = set()
            for f in features:
                if f.get("properties"):
                    columns.update(f["properties"].keys())
            columns = sorted(list(columns))

            entities = [
                f.get("properties", {}).get("entity", "")
                for f in features
                if f.get("properties", {}).get("entity")
            ]
            unique_entities = set(entities)

            return {
                "file_type": "GeoJSON",
                "record_count": len(features),
                "is_valid": True,
                "validation_standard": "RFC 7946",
                "columns": columns,
                "column_count": len(columns),
                "entities": unique_entities,
                "total_entities": len(entities),
                "unique_entities": len(unique_entities),
                "duplicate_count": len(entities) - len(unique_entities),
            }
        except Exception as e:
            return {
                "file_type": "GeoJSON",
                "record_count": 0,
                "is_valid": False,
                "error": str(e),
                "validation_standard": "RFC 7946",
            }


if __name__ == "__main__":
    processor = FileProcessor()
    base_path = "pyspark-jobs/tests/reconciliation/data/title-boundary_testdata"

    files = [
        f"{base_path}/title-boundary.csv",
        f"{base_path}/title-boundary.json",
        f"{base_path}/title-boundary.geojson",
    ]

    results = []
    entity_sets = {}
    column_sets = {}

    for file_path in files:
        result = processor.process_file(file_path)
        result["file_path"] = file_path

        if "entities" in result:
            entity_sets[result["file_type"]] = result["entities"]
            del result["entities"]  # Remove entities from report

        if "columns" in result:
            column_sets[result["file_type"]] = set(result["columns"])

        results.append(result)
        print(f"File: {file_path}")
        print(f"Type: {result.get('file_type')}")
        print(f"Records: {result.get('record_count')}")
        print(f"Columns: {result.get('column_count')}")
        print(f"Unique Entities: {result.get('unique_entities')}")
        print(f"Duplicates: {result.get('duplicate_count')}")
        print(f"Valid: {result.get('is_valid')}")
        if "error" in result:
            print(f"Error: {result['error']}")
        print("-" * 80)

    # Cross-file column comparison
    column_comparison = {}
    if len(column_sets) == 3:
        csv_cols = column_sets.get("CSV", set())
        json_cols = column_sets.get("JSON", set())
        geojson_cols = column_sets.get("GeoJSON", set())

        all_columns = sorted(csv_cols | json_cols | geojson_cols)
        common_columns = sorted(csv_cols & json_cols & geojson_cols)

        column_comparison = {
            "all_columns": all_columns,
            "common_columns": common_columns,
            "all_files_match": csv_cols == json_cols == geojson_cols,
            "only_in_csv": sorted(list(csv_cols - json_cols - geojson_cols)),
            "only_in_json": sorted(list(json_cols - csv_cols - geojson_cols)),
            "only_in_geojson": sorted(list(geojson_cols - csv_cols - json_cols)),
        }

        print("\n" + "=" * 80)
        print("COLUMN COMPARISON ACROSS FILES")
        print("=" * 80)
        print(f"Total unique columns: {len(all_columns)}")
        print(f"Common columns: {len(common_columns)}")
        print(f"All files have same columns: {column_comparison['all_files_match']}")
        if column_comparison["only_in_csv"]:
            print(f"Only in CSV: {column_comparison['only_in_csv']}")
        if column_comparison["only_in_json"]:
            print(f"Only in JSON: {column_comparison['only_in_json']}")
        if column_comparison["only_in_geojson"]:
            print(f"Only in GeoJSON: {column_comparison['only_in_geojson']}")

    # Cross-file entity comparison
    comparison = {}
    if len(entity_sets) == 3:
        csv_entities = entity_sets.get("CSV", set())
        json_entities = entity_sets.get("JSON", set())
        geojson_entities = entity_sets.get("GeoJSON", set())

        all_match = csv_entities == json_entities == geojson_entities

        comparison = {
            "all_files_match": all_match,
            "csv_vs_json": {
                "match": csv_entities == json_entities,
                "only_in_csv": list(csv_entities - json_entities)[:10],
                "only_in_json": list(json_entities - csv_entities)[:10],
            },
            "csv_vs_geojson": {
                "match": csv_entities == geojson_entities,
                "only_in_csv": list(csv_entities - geojson_entities)[:10],
                "only_in_geojson": list(geojson_entities - csv_entities)[:10],
            },
            "json_vs_geojson": {
                "match": json_entities == geojson_entities,
                "only_in_json": list(json_entities - geojson_entities)[:10],
                "only_in_geojson": list(geojson_entities - json_entities)[:10],
            },
        }

        print("\n" + "=" * 80)
        print("ENTITY COMPARISON ACROSS FILES")
        print("=" * 80)
        print(f"All files have matching entities: {all_match}")
        print(f"CSV vs JSON match: {comparison['csv_vs_json']['match']}")
        print(f"CSV vs GeoJSON match: {comparison['csv_vs_geojson']['match']}")
        print(f"JSON vs GeoJSON match: {comparison['json_vs_geojson']['match']}")

    # Generate report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = "/Users/399182/MHCLG-githib/pyspark-jobs/tests/reconciliation/reports"
    os.makedirs(report_dir, exist_ok=True)
    report_path = f"{report_dir}/validation_report_{timestamp}.json"

    report = {
        "timestamp": datetime.now().isoformat(),
        "description": {
            "is_valid": "Indicates if the file format is valid according to global standards (CSV structure, JSON syntax, GeoJSON RFC 7946)",
            "record_count": "Total number of records in the file",
            "column_count": "Number of columns/fields in the file",
            "unique_entities": "Number of unique entity values",
            "duplicate_count": "Number of duplicate entity values found",
            "validation_standard": "GeoJSON validation follows RFC 7946 specification",
        },
        "results": results,
        "column_comparison": column_comparison,
        "entity_comparison": comparison,
    }

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nReport saved to: {report_path}")
