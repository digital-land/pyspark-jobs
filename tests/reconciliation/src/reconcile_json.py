#!/usr/bin/env python3
# python3 reconcile_json.py
"""JSON Data Reconciliation Script"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from shapely import wkt

# Increase CSV field size limit for large geometry fields
csv.field_size_limit(10485760)  # 10MB

# Create reports directory if it doesn't exist
os.makedirs("reports", exist_ok=True)

# File paths
file1 = "data/central-activities-zone_new.json"
file2 = "data/central-activities-zone_original.json"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output = f"reports/{timestamp}_reconciliation_report_json.txt"


# Read JSON files
def read_json(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
        return (
            data["entities"] if isinstance(data, dict) and "entities" in data else data
        )


data1 = read_json(file1)
data2 = read_json(file2)

# Get field names
fields1 = set(data1[0].keys()) if data1 and len(data1) > 0 else set()
fields2 = set(data2[0].keys()) if data2 and len(data2) > 0 else set()

# Create entity lookup
entities1 = {row["entity"]: row for row in data1}
entities2 = {row["entity"]: row for row in data2}

# Reconciliation
only_in_file1 = set(entities1.keys()) - set(entities2.keys())
only_in_file2 = set(entities2.keys()) - set(entities1.keys())
common = set(entities1.keys()) & set(entities2.keys())

# Check differences in common entities
differences = defaultdict(list)
field_mismatch_counts = defaultdict(int)
point_distances = []
significant_point_diffs = []
SIGNIFICANT_DISTANCE_THRESHOLD = 0.0001  # ~11 meters

for entity in common:
    for field in entities1[entity].keys():
        val1 = entities1[entity][field]
        val2 = entities2[entity][field]

        # Special handling for point field - compare by geometric distance, not string
        if field == "point" and val1 and val2:
            try:
                geom1 = wkt.loads(val1)
                geom2 = wkt.loads(val2)
                distance = geom1.distance(geom2)
                point_distances.append(distance)
                if distance >= SIGNIFICANT_DISTANCE_THRESHOLD:
                    differences[entity].append((field, val1, val2))
                    field_mismatch_counts[field] += 1
                    significant_point_diffs.append(
                        {
                            "entity": entity,
                            "distance_degrees": distance,
                            "distance_meters": distance * 111320,
                            "point_file1": val1,
                            "point_file2": val2,
                        }
                    )
            except Exception as e:
                differences[entity].append((field, val1, val2))
                field_mismatch_counts[field] += 1
        elif val1 != val2:
            differences[entity].append((field, val1, val2))
            field_mismatch_counts[field] += 1

# Generate report
with open(output, "w") as f:
    f.write("=" * 80 + "\n")
    f.write("JSON DATA RECONCILIATION REPORT\n")
    f.write("=" * 80 + "\n\n")

    f.write(f"File 1: {file1}\n")
    f.write(f"File 2: {file2}\n\n")

    f.write(f"Total records in File 1: {len(data1)}\n")
    f.write(f"Total records in File 2: {len(data2)}\n")
    f.write(f"Common entities: {len(common)}\n\n")

    f.write("-" * 80 + "\n")
    f.write("HEADER COMPARISON\n")
    f.write("-" * 80 + "\n")
    only_in_fields1 = fields1 - fields2
    only_in_fields2 = fields2 - fields1
    common_fields = fields1 & fields2
    f.write(f"Fields in File 1: {len(fields1)}\n")
    f.write(f"Fields in File 2: {len(fields2)}\n")
    f.write(f"Common fields: {len(common_fields)}\n")
    if only_in_fields1:
        f.write(f"\nOnly in File 1: {sorted(only_in_fields1)}\n")
    if only_in_fields2:
        f.write(f"Only in File 2: {sorted(only_in_fields2)}\n")
    f.write("\n")

    f.write("-" * 80 + "\n")
    f.write("MISSING RECORDS\n")
    f.write("-" * 80 + "\n")
    f.write(f"Only in File 1: {len(only_in_file1)}\n")
    if only_in_file1:
        f.write(f"  Entities: {sorted(only_in_file1)}\n")
    f.write(f"\nOnly in File 2: {len(only_in_file2)}\n")
    if only_in_file2:
        f.write(f"  Entities: {sorted(only_in_file2)}\n")

    f.write("-" * 80 + "\n")
    f.write("FIELD DIFFERENCES IN COMMON RECORDS\n")
    f.write("-" * 80 + "\n")
    f.write(f"Records with differences: {len(differences)}\n\n")

    if field_mismatch_counts:
        for field in sorted(field_mismatch_counts.keys()):
            f.write(f"  {field}: {field_mismatch_counts[field]} mismatches\n")

    if point_distances:
        f.write(f"\nPoint Geometry Analysis:\n")
        f.write(f"  Total point comparisons: {len(point_distances)}\n")
        f.write(f"  Max distance: {max(point_distances):.10f} degrees\n")
        f.write(f"  Min distance: {min(point_distances):.10f} degrees\n")
        f.write(
            f"  Avg distance: {sum(point_distances)/len(point_distances):.10f} degrees\n"
        )
        f.write(
            f"  Significant differences (>={SIGNIFICANT_DISTANCE_THRESHOLD}): {field_mismatch_counts.get('point', 0)}\n"
        )
        f.write(
            f"\n  Note: A significant difference is defined as a distance >= {SIGNIFICANT_DISTANCE_THRESHOLD} degrees\n"
        )
        f.write(
            f"  (~11 meters). Differences below this threshold are considered negligible and\n"
        )
        f.write(
            f"  likely due to floating-point precision variations during data processing.\n"
        )

    f.write("\n" + "=" * 80 + "\n")
    f.write("SUMMARY\n")
    f.write("=" * 80 + "\n")
    if not only_in_file1 and not only_in_file2 and not differences:
        f.write("✓ Files are identical\n")
    else:
        f.write(f"✗ Files have differences:\n")
        f.write(f"  - {len(only_in_file1)} records only in File 1\n")
        f.write(f"  - {len(only_in_file2)} records only in File 2\n")
        f.write(f"  - {len(differences)} records with field differences\n")

# Write significant point differences to JSON
if significant_point_diffs:
    sig_output = f"reports/{timestamp}_significant_point_differences.json"
    with open(sig_output, "w") as f:
        json.dump(significant_point_diffs, f, indent=2)
    print(f"Reconciliation complete. Report saved to: {output}")
    print(
        f"Significant point differences saved to: {sig_output} ({len(significant_point_diffs)} records)"
    )
else:
    print(f"Reconciliation complete. Report saved to: {output}")
