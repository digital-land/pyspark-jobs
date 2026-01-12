# !/usr/bin/env python3
# python3 reconcile_csv.py
"""CSV Data Reconciliation Script"""

from collections import defaultdict

from shapely import wkt

# Increase CSV field size limit for large geometry fields
csv.field_size_limit(10485760)  # 10MB

# Create reports directory if it doesn't exist
os.makedirs("reports", exist_ok=True)

# File paths
file1 = "data/title - boundary_target.csv"
file2 = "data/title - boundary_original.csv"
# file1 = "data/central_activities_zone_entity.csv"
# file2 = "data/central_activities_zone_pyspark_entity.csv"
# file1 = "data/title_boundary_entity.csv"
# file2 = "data/title_boundary_pyspark_entity.csv"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output = f"reports/{timestamp}_reconciliation_report.txt"


# Read CSV files
def read_csv(filepath):
    with open(filepath, "r") as f:
        return list(csv.DictReader(f))


data1 = read_csv(file1)
data2 = read_csv(file2)

# Get column names
columns1 = set(data1[0].keys()) if data1 else set()
columns2 = set(data2[0].keys()) if data2 else set()

# Create entity lookup and detect duplicates
entities1 = {}
duplicates1 = defaultdict(int)
for row in data1:
    entity = row["entity"]
    if entity in entities1:
        duplicates1[entity] += 1
    else:
        entities1[entity] = row
        duplicates1[entity] = 1

entities2 = {}
duplicates2 = defaultdict(int)
for row in data2:
    entity = row["entity"]
    if entity in entities2:
        duplicates2[entity] += 1
    else:
        entities2[entity] = row
        duplicates2[entity] = 1

# Filter to only actual duplicates (count > 1)
duplicates1 = {k: v for k, v in duplicates1.items() if v > 1}
duplicates2 = {k: v for k, v in duplicates2.items() if v > 1}

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
        if val1 != val2:
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
            else:
                differences[entity].append((field, val1, val2))
                field_mismatch_counts[field] += 1

# Generate report
with open(output, "w") as f:
    f.write("=" * 80 + "\n")
    f.write("CSV DATA RECONCILIATION REPORT\n")
    f.write("=" * 80 + "\n\n")

    f.write(f"File 1: {file1}\n")
    f.write(f"File 2: {file2}\n\n")

    f.write(f"Total records in File 1: {len(data1)}\n")
    f.write(f"Total records in File 2: {len(data2)}\n")
    f.write(f"Unique entities in File 1: {len(entities1)}\n")
    f.write(f"Unique entities in File 2: {len(entities2)}\n")
    f.write(f"Common entities: {len(common)}\n\n")

    f.write("-" * 80 + "\n")
    f.write("HEADER COMPARISON\n")
    f.write("-" * 80 + "\n")
    only_in_cols1 = columns1 - columns2
    only_in_cols2 = columns2 - columns1
    common_cols = columns1 & columns2
    f.write(f"Columns in File 1: {len(columns1)}\n")
    f.write(f"Columns in File 2: {len(columns2)}\n")
    f.write(f"Common columns: {len(common_cols)}\n")
    if only_in_cols1:
        f.write(f"\nOnly in File 1: {sorted(only_in_cols1)}\n")
    if only_in_cols2:
        f.write(f"Only in File 2: {sorted(only_in_cols2)}\n")
    f.write("\n")

    f.write("-" * 80 + "\n")
    f.write("DUPLICATE ENTITIES\n")
    f.write("-" * 80 + "\n")
    f.write(f"Duplicate entities in File 1: {len(duplicates1)}\n")
    if duplicates1:
        f.write("  Entity (Count):\n")
        for entity, count in sorted(duplicates1.items()):
            f.write(f"    {entity}: {count} occurrences\n")
    f.write(f"\nDuplicate entities in File 2: {len(duplicates2)}\n")
    if duplicates2:
        f.write("  Entity (Count):\n")
        for entity, count in sorted(duplicates2.items()):
            f.write(f"    {entity}: {count} occurrences\n")
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
        f.write("\nPoint Geometry Analysis:\n")
        f.write(f"  Total point comparisons: {len(point_distances)}\n")
        f.write(f"  Max distance: {max(point_distances):.10f} degrees\n")
        f.write(f"  Min distance: {min(point_distances):.10f} degrees\n")
        f.write(
            f"  Avg distance: {sum(point_distances) / len(point_distances):.10f} degrees\n"
        )
        f.write(
            f"  Significant differences (>={SIGNIFICANT_DISTANCE_THRESHOLD}): {field_mismatch_counts.get('point', 0)}\n"
        )
        f.write(
            f"\n  Note: A significant difference is defined as a distance >= {SIGNIFICANT_DISTANCE_THRESHOLD} degrees\n"
        )
        f.write(
            "  (~11 meters). Differences below this threshold are considered negligible and\n"
        )
        f.write(
            "  likely due to floating - point precision variations during data processing.\n"
        )

    f.write("\n" + "=" * 80 + "\n")
    f.write("SUMMARY\n")
    f.write("=" * 80 + "\n")
    if (
        not only_in_file1
        and not only_in_file2
        and not differences
        and not duplicates1
        and not duplicates2
    ):
        f.write("✓ Files are identical\n")
    else:
        f.write("✗ Files have differences:\n")
        f.write(
            f"  - {len(duplicates1)} duplicate entities in File 1 ({sum(duplicates1.values())} total duplicate records)\n"
        )
        f.write(
            f"  - {len(duplicates2)} duplicate entities in File 2 ({sum(duplicates2.values())} total duplicate records)\n"
        )
        f.write(f"  - {len(only_in_file1)} records only in File 1\n")
        f.write(f"  - {len(only_in_file2)} records only in File 2\n")
        f.write(f"  - {len(differences)} records with field differences\n")

# Write significant point differences to CSV
if significant_point_diffs:
    sig_output = f"reports/{timestamp}_significant_point_differences.csv"
    with open(sig_output, "w", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "entity",
                "distance_degrees",
                "distance_meters",
                "point_file1",
                "point_file2",
            ],
        )
        writer.writeheader()
        writer.writerows(significant_point_diffs)
    print(f"Reconciliation complete. Report saved to: {output}")
    print(
        f"Significant point differences saved to: {sig_output} ({len(significant_point_diffs)} records)"
    )
else:
    print(f"Reconciliation complete. Report saved to: {output}")
