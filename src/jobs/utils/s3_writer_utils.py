"""S3 Writer utilities for data transformation and writing."""

import json
import logging
import re
from datetime import date, datetime
from typing import List, Optional

import boto3
from pyspark.sql.functions import lit

logger = logging.getLogger(__name__)

MULTIPART_TARGET_PARTITION_BYTES = 64 * 1024 * 1024

df_entity = None


def write_delta(
    df,
    output_path: str,
    dataset: str,
    partition_by: Optional[List[str]] = None,
):
    """Write DataFrame as a Delta Lake table, atomically replacing the dataset partition.

    If a Delta table already exists at output_path, the incoming DataFrame schema must
    match exactly. Schema migrations must be handled separately before writing.

    Args:
        df: PySpark DataFrame to write.
        output_path: Destination path (local or s3://).
        dataset: Dataset identifier used to scope the partition replacement.
        partition_by: Columns to partition by. If None, writes without partitioning.

    Raises:
        ValueError: If a Delta table exists at output_path with a different schema.
    """
    from cloudpathlib import AnyPath
    from delta.tables import DeltaTable

    spark = df.sparkSession

    logger.info(f"write_delta: Writing dataset '{dataset}' to {output_path}")

    path = AnyPath(output_path)
    is_delta = DeltaTable.isDeltaTable(spark, output_path)

    if path.exists() and any(path.iterdir()) and not is_delta:
        raise ValueError(
            f"write_delta: {output_path} contains existing files but is not a Delta table. "
            f"Remove the existing data before writing."
        )

    if is_delta:
        existing_schema = DeltaTable.forPath(spark, output_path).toDF().schema
        existing_fields = {f.name: f.dataType for f in existing_schema.fields}
        incoming_fields = {f.name: f.dataType for f in df.schema.fields}
        if existing_fields != incoming_fields:
            raise ValueError(
                f"write_delta: Schema mismatch for Delta table at {output_path}. "
                f"Run a schema migration before writing.\n"
                f"  Existing : {existing_schema.simpleString()}\n"
                f"  Incoming : {df.schema.simpleString()}"
            )

    row_count = df.count()
    optimal_partitions = max(1, min(200, row_count // 1000000))

    writer = (
        df.coalesce(optimal_partitions)
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"dataset = '{dataset}'")
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(output_path)

    logger.info(f"write_delta: Successfully wrote {row_count:,} rows for '{dataset}'")


def cleanup_temp_path(env, dataset_name):
    """Delete all objects in the temp S3 path for a dataset."""
    s3_client = boto3.client("s3")
    bucket_name = f"{env}-collection-data"
    prefix = f"dataset/temp/{dataset_name}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            logger.info(f"Deleted {len(objects)} objects from {prefix}")


def _sanitize_json_row(row) -> dict:
    """Convert a Row to a JSON-safe dict: dates/datetimes to ISO strings, None to ''."""
    row_dict = row.asDict()
    for key, value in row_dict.items():
        if isinstance(value, (date, datetime)):
            row_dict[key] = value.isoformat() if value else ""
        elif value is None:
            row_dict[key] = ""
    return row_dict


def _upload_json_entities_partition(
    bucket, key, upload_id, first_nonempty_idx, last_nonempty_idx, partition_index, rows
):
    """Reference implementation of the per-partition JSON-assembly logic --
    kept only so it's directly unit-testable, NOT called by
    write_json_entities_s3 in production.

    write_json_entities_s3 instead defines an inline, self-contained nested
    closure with the same logic, because this function's identity is
    resolvable via a plain module attribute lookup (getattr(jobs.utils.
    s3_writer_utils, "_upload_json_entities_partition")), which makes
    cloudpickle ship a *reference* to it -- requiring the `jobs` package to
    be importable in whatever Python process unpickles it. On the driver
    that's fine, but Spark runs mapPartitionsWithIndex closures in executor
    worker subprocesses, and this codebase's executors are not guaranteed to
    have the `jobs` wheel on their PYTHONPATH (a real EMR Serverless
    `--py-files` gap, driver-only in practice), so a shipped reference to
    this function fails with `ModuleNotFoundError: No module named 'jobs'`.
    A nested function defined inside write_json_entities_s3 isn't reachable
    via module attribute lookup, so cloudpickle ships it by value (embedded
    bytecode) instead, and it only touches builtins/stdlib/boto3, all
    guaranteed present without `jobs`.

    If you change the assembly logic here (header/footer/comma placement),
    make the identical change in write_json_entities_s3's nested closure --
    they must stay in sync, and this function is what the tests below
    actually exercise.

    The partition at first_nonempty_idx opens the `{"entities":[...]}` array
    and the one at last_nonempty_idx closes it, so the closing bracket lands
    in the true final part of the upload, which is the only part exempt from
    S3's 5MB-per-part minimum. first/last_nonempty_idx must be the *actual*
    non-empty partition boundaries, not assumed to be 0 and
    (partition count - 1): Spark's round-robin repartitioning starts from a
    random offset per *input* partition, so when the requested output
    partition count is large relative to the number of partitions already
    present, it can leave any partition -- including the first or last --
    empty.
    """
    pieces = [json.dumps(_sanitize_json_row(row)) for row in rows]
    if not pieces:
        return iter([])

    body = ",".join(pieces)
    if partition_index == first_nonempty_idx:
        body = '{"entities":[' + body
    else:
        # separates this partition's first item from the previous
        # partition's last item -- the join above only handles commas
        # *within* a partition's own rows.
        body = "," + body
    if partition_index == last_nonempty_idx:
        body += "]}"

    s3 = boto3.client("s3")
    part_number = partition_index + 1
    part = s3.upload_part(
        Bucket=bucket,
        Key=key,
        PartNumber=part_number,
        UploadId=upload_id,
        Body=body,
    )
    return iter([(part_number, part["ETag"])])


def _nonempty_partition_bounds(rdd):
    """Return (first_nonempty_idx, last_nonempty_idx) for an RDD's partitions.

    Spark's round-robin repartitioning starts from a random offset per
    *input* partition, so when the requested output partition count is large
    relative to the number of partitions already present, it can leave
    arbitrary partitions -- including the first or last -- empty. Callers
    that need to know which physical partition is truly first/last (e.g. to
    place an opening/closing bracket) must use this instead of assuming
    indices 0 and rdd.getNumPartitions() - 1.
    """
    partition_counts = rdd.mapPartitionsWithIndex(
        lambda idx, rows: [(idx, sum(1 for _ in rows))]
    ).collect()
    nonempty_indices = [idx for idx, count in partition_counts if count > 0]
    return min(nonempty_indices), max(nonempty_indices)


def write_json_entities_s3(
    df,
    s3_client,
    bucket: str,
    key: str,
    target_partition_bytes: int = MULTIPART_TARGET_PARTITION_BYTES,
):
    """Write `df` as `{"entities": [...]}` JSON to S3 via a distributed multipart upload.

    Each partition serializes and uploads its own rows directly to S3 from
    the executor that holds them, so the driver only orchestrates the
    multipart upload rather than collecting every row itself. Partitions are
    sized (via a byte-size estimate from a random sample) well above S3's
    5MB-per-part minimum so that, in the common case, every partition except
    the last produces a part large enough to be valid on its own. This is a
    heuristic, not a guarantee: a sample that misses extreme outliers (e.g.
    a few rows with unusually large geometries) could still under-size a
    partition and fail the upload.
    """
    row_count = df.count()
    if row_count == 0:
        s3_client.put_object(Bucket=bucket, Key=key, Body='{"entities":[]}')
        logger.info(f"write_json_entities_s3: wrote empty entities array to {key}")
        return

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        s3_client.delete_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.ClientError:
        pass

    sample = df.rdd.takeSample(False, min(200, row_count), seed=42)
    avg_row_bytes = max(
        1,
        sum(len(json.dumps(_sanitize_json_row(r)).encode("utf-8")) for r in sample)
        // len(sample),
    )
    estimated_total_bytes = avg_row_bytes * row_count
    num_partitions = max(
        1, min(row_count, estimated_total_bytes // target_partition_bytes)
    )

    # Cached because it drives two actions below (the row-count pass and the
    # upload pass) -- without caching, everything upstream of temp_df would
    # be recomputed a second time.
    partitioned = df.repartition(num_partitions).cache()

    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mpu["UploadId"]

    try:
        first_nonempty_idx, last_nonempty_idx = _nonempty_partition_bounds(
            partitioned.rdd
        )

        def _upload_partition(partition_index, rows):
            # Nested (not module-level) on purpose: this closure is shipped
            # by Spark to run inside executor Python worker subprocesses,
            # which don't reliably have the `jobs` package on PYTHONPATH.
            # A nested function can't be reached via module attribute lookup,
            # so cloudpickle ships it by value instead of by reference -- it
            # must therefore only touch builtins/stdlib/boto3 (all present
            # without `jobs`), never a module-level jobs.* name. Mirrors
            # _upload_json_entities_partition / _sanitize_json_row above,
            # which exist purely so this logic is directly unit-testable --
            # keep both in sync; see that function's docstring for why the
            # duplication is necessary.
            pieces = []
            for row in rows:
                row_dict = row.asDict()
                for field, value in row_dict.items():
                    if isinstance(value, (date, datetime)):
                        row_dict[field] = value.isoformat() if value else ""
                    elif value is None:
                        row_dict[field] = ""
                pieces.append(json.dumps(row_dict))
            if not pieces:
                return iter([])

            body = ",".join(pieces)
            if partition_index == first_nonempty_idx:
                body = '{"entities":[' + body
            else:
                body = "," + body
            if partition_index == last_nonempty_idx:
                body += "]}"

            s3 = boto3.client("s3")
            part_number = partition_index + 1
            part = s3.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=body,
            )
            return iter([(part_number, part["ETag"])])

        uploaded_parts = partitioned.rdd.mapPartitionsWithIndex(
            _upload_partition
        ).collect()

        parts = [
            {"PartNumber": num, "ETag": etag} for num, etag in sorted(uploaded_parts)
        ]
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        logger.info(f"write_json_entities_s3: wrote {row_count:,} entities to {key}")
    except Exception as e:
        logger.error(f"write_json_entities_s3: multipart upload failed: {e}")
        s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
    finally:
        partitioned.unpersist()


def _upload_geojson_entities_partition(
    bucket,
    key,
    upload_id,
    dataset_name,
    first_nonempty_idx,
    last_nonempty_idx,
    partition_index,
    rows,
):
    """Reference implementation of the per-partition GeoJSON-assembly logic --
    kept only so it's directly unit-testable, NOT called by
    write_geojson_entities_s3 in production. See
    _upload_json_entities_partition's docstring for why: this function is
    resolvable via a plain module attribute lookup, so cloudpickle would ship
    a reference to it that fails to unpickle on an executor without the
    `jobs` package. write_geojson_entities_s3 instead defines an inline,
    self-contained nested closure with the same logic (including its own
    copy of the WKT-to-GeoJSON parsing below, rather than calling
    resolve_geometry/wkt_to_geojson) -- keep both in sync.

    Same header/footer/comma placement rules as _upload_json_entities_partition,
    wrapping in a GeoJSON FeatureCollection instead of an entities array.
    """
    pieces = []
    for row in rows:
        row_dict = row.asDict()
        geometry_wkt = row_dict.pop("geometry", None)
        point_wkt = row_dict.pop("point", None)
        for field, value in row_dict.items():
            if isinstance(value, (date, datetime)):
                row_dict[field] = value.isoformat() if value else ""
            elif value is None:
                row_dict[field] = ""
        feature = {
            "type": "Feature",
            "properties": row_dict,
            "geometry": resolve_geometry(geometry_wkt, point_wkt),
        }
        pieces.append(json.dumps(feature))
    if not pieces:
        return iter([])

    body = ",".join(pieces)
    if partition_index == first_nonempty_idx:
        body = (
            '{"type":"FeatureCollection","name":"' + dataset_name + '","features":['
        ) + body
    else:
        body = "," + body
    if partition_index == last_nonempty_idx:
        body += "]}"

    s3 = boto3.client("s3")
    part_number = partition_index + 1
    part = s3.upload_part(
        Bucket=bucket,
        Key=key,
        PartNumber=part_number,
        UploadId=upload_id,
        Body=body,
    )
    return iter([(part_number, part["ETag"])])


def _sample_avg_geojson_feature_bytes(sample):
    """Driver-side only -- safe to call resolve_geometry/wkt_to_geojson here
    since this never runs on an executor."""
    sizes = []
    for row in sample:
        row_dict = row.asDict()
        geometry_wkt = row_dict.pop("geometry", None)
        point_wkt = row_dict.pop("point", None)
        for field, value in row_dict.items():
            if isinstance(value, (date, datetime)):
                row_dict[field] = value.isoformat() if value else ""
            elif value is None:
                row_dict[field] = ""
        feature = {
            "type": "Feature",
            "properties": row_dict,
            "geometry": resolve_geometry(geometry_wkt, point_wkt),
        }
        sizes.append(len(json.dumps(feature).encode("utf-8")))
    return max(1, sum(sizes) // len(sizes))


def write_geojson_entities_s3(
    df,
    s3_client,
    bucket: str,
    key: str,
    dataset_name: str,
    target_partition_bytes: int = MULTIPART_TARGET_PARTITION_BYTES,
):
    """Write `df` as a GeoJSON FeatureCollection to S3 via a distributed
    multipart upload. See write_json_entities_s3's docstring for the general
    approach (each partition uploads its own rows directly from its
    executor; partitions are sized well above S3's 5MB-per-part minimum via
    a sampled byte-size estimate, a heuristic rather than a guarantee).

    geometry/point WKT columns are popped from each row and converted to a
    GeoJSON geometry, same as the driver-side writers this replaces.
    """
    row_count = df.count()
    if row_count == 0:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=(
                '{"type":"FeatureCollection","name":"'
                + dataset_name
                + '","features":[]}'
            ),
        )
        logger.info(
            f"write_geojson_entities_s3: wrote empty feature collection to {key}"
        )
        return

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        s3_client.delete_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.ClientError:
        pass

    sample = df.rdd.takeSample(False, min(200, row_count), seed=42)
    avg_row_bytes = _sample_avg_geojson_feature_bytes(sample)
    estimated_total_bytes = avg_row_bytes * row_count
    num_partitions = max(
        1, min(row_count, estimated_total_bytes // target_partition_bytes)
    )

    # Cached because it drives two actions below (the row-count pass and the
    # upload pass) -- without caching, everything upstream of df would be
    # recomputed a second time.
    partitioned = df.repartition(num_partitions).cache()

    mpu = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = mpu["UploadId"]

    try:
        first_nonempty_idx, last_nonempty_idx = _nonempty_partition_bounds(
            partitioned.rdd
        )

        def _upload_partition(partition_index, rows):
            # Nested (not module-level) on purpose -- see
            # _upload_geojson_entities_partition's docstring. Only
            # builtins/stdlib/boto3 (via the module-level `re`, `json`,
            # `boto3`, `date`, `datetime` imports) are touched here, never a
            # module-level jobs.* name -- including resolve_geometry/
            # wkt_to_geojson, which is why the WKT parsing is duplicated
            # inline below instead of calling them.
            def wkt_to_geojson(wkt_string):
                if not wkt_string:
                    return None
                wkt_string = wkt_string.strip()

                if wkt_string.startswith("POINT"):
                    coords = re.findall(r"[-\d.]+", wkt_string)
                    return {
                        "type": "Point",
                        "coordinates": [float(coords[0]), float(coords[1])],
                    }

                elif wkt_string.startswith("POLYGON"):
                    rings = re.findall(r"\(([^()]+)\)", wkt_string)
                    coordinates = []
                    for ring in rings:
                        points = []
                        coords = re.findall(r"([-\d.]+)\s+([-\d.]+)", ring)
                        for lon, lat in coords:
                            points.append([float(lon), float(lat)])
                        coordinates.append(points)
                    return {"type": "Polygon", "coordinates": coordinates}

                elif wkt_string.startswith("MULTIPOLYGON"):
                    wkt_string = wkt_string.replace("MULTIPOLYGON ", "").strip()
                    polygons = []
                    depth = 0
                    current_polygon = ""

                    for char in wkt_string:
                        if char == "(":
                            depth += 1
                            if depth > 1:
                                current_polygon += char
                        elif char == ")":
                            depth -= 1
                            if depth > 0:
                                current_polygon += char
                            elif depth == 0 and current_polygon:
                                rings = re.findall(r"\(([^()]+)\)", current_polygon)
                                coordinates = []
                                for ring in rings:
                                    points = []
                                    coords = re.findall(r"([-\d.]+)\s+([-\d.]+)", ring)
                                    for lon, lat in coords:
                                        points.append([float(lon), float(lat)])
                                    coordinates.append(points)
                                polygons.append(coordinates)
                                current_polygon = ""
                        elif depth > 0:
                            current_polygon += char

                    if len(polygons) == 1:
                        return {"type": "Polygon", "coordinates": polygons[0]}
                    return {"type": "MultiPolygon", "coordinates": polygons}

                return None

            pieces = []
            for row in rows:
                row_dict = row.asDict()
                geometry_wkt = row_dict.pop("geometry", None)
                point_wkt = row_dict.pop("point", None)
                for field, value in row_dict.items():
                    if isinstance(value, (date, datetime)):
                        row_dict[field] = value.isoformat() if value else ""
                    elif value is None:
                        row_dict[field] = ""
                wkt = geometry_wkt or point_wkt
                feature = {
                    "type": "Feature",
                    "properties": row_dict,
                    "geometry": wkt_to_geojson(wkt) if wkt else None,
                }
                pieces.append(json.dumps(feature))
            if not pieces:
                return iter([])

            body = ",".join(pieces)
            if partition_index == first_nonempty_idx:
                body = (
                    '{"type":"FeatureCollection","name":"'
                    + dataset_name
                    + '","features":['
                ) + body
            else:
                body = "," + body
            if partition_index == last_nonempty_idx:
                body += "]}"

            s3 = boto3.client("s3")
            part_number = partition_index + 1
            part = s3.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=body,
            )
            return iter([(part_number, part["ETag"])])

        uploaded_parts = partitioned.rdd.mapPartitionsWithIndex(
            _upload_partition
        ).collect()

        parts = [
            {"PartNumber": num, "ETag": etag} for num, etag in sorted(uploaded_parts)
        ]
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        logger.info(f"write_geojson_entities_s3: wrote {row_count:,} features to {key}")
    except Exception as e:
        logger.error(f"write_geojson_entities_s3: multipart upload failed: {e}")
        s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
    finally:
        partitioned.unpersist()


def resolve_geometry(
    geometry_wkt: Optional[str], point_wkt: Optional[str]
) -> Optional[dict]:
    """Convert geometry WKT to GeoJSON, falling back to point WKT if geometry is absent."""
    wkt = geometry_wkt or point_wkt
    return wkt_to_geojson(wkt) if wkt else None


def wkt_to_geojson(wkt_string):
    """Convert WKT geometry string to GeoJSON geometry object."""
    if not wkt_string:
        return None

    wkt_string = wkt_string.strip()

    if wkt_string.startswith("POINT"):
        coords = re.findall(r"[-\d.]+", wkt_string)
        return {"type": "Point", "coordinates": [float(coords[0]), float(coords[1])]}

    elif wkt_string.startswith("POLYGON"):
        rings = re.findall(r"\(([^()]+)\)", wkt_string)
        coordinates = []
        for ring in rings:
            points = []
            coords = re.findall(r"([-\d.]+)\s+([-\d.]+)", ring)
            for lon, lat in coords:
                points.append([float(lon), float(lat)])
            coordinates.append(points)
        return {"type": "Polygon", "coordinates": coordinates}

    elif wkt_string.startswith("MULTIPOLYGON"):
        wkt_string = wkt_string.replace("MULTIPOLYGON ", "").strip()
        polygons = []
        depth = 0
        current_polygon = ""

        for char in wkt_string:
            if char == "(":
                depth += 1
                if depth > 1:
                    current_polygon += char
            elif char == ")":
                depth -= 1
                if depth > 0:
                    current_polygon += char
                elif depth == 0 and current_polygon:
                    rings = re.findall(r"\(([^()]+)\)", current_polygon)
                    coordinates = []
                    for ring in rings:
                        points = []
                        coords = re.findall(r"([-\d.]+)\s+([-\d.]+)", ring)
                        for lon, lat in coords:
                            points.append([float(lon), float(lat)])
                        coordinates.append(points)
                    polygons.append(coordinates)
                    current_polygon = ""
            elif depth > 0:
                current_polygon += char

        if len(polygons) == 1:
            return {"type": "Polygon", "coordinates": polygons[0]}
        return {"type": "MultiPolygon", "coordinates": polygons}

    return None


def s3_rename_and_move(dataset_name, file_type, bucket_name):
    """Rename and move files in S3."""
    s3_client = boto3.client("s3")
    unique_data_filename = f"{dataset_name}.{file_type}"
    target_key = f"dataset/{unique_data_filename}"

    try:
        s3_client.head_object(Bucket=bucket_name, Key=target_key)
        s3_client.delete_object(Bucket=bucket_name, Key=target_key)
        logger.info(f"Deleted existing file: {target_key}")
    except s3_client.exceptions.ClientError:
        logger.info(f"No existing file to delete: {target_key}")

    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=f"dataset/temp/{dataset_name}/"
    )
    data_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(f".{file_type}")
    ]

    for data_file in data_files:
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": data_file},
            Key=target_key,
        )
        s3_client.delete_object(Bucket=bucket_name, Key=data_file)
        logger.info(f"Renamed: {data_file} -> {target_key}")


def ensure_schema_fields(df, dataset_name):
    """Ensure DataFrame has all required fields from schema specification."""
    try:
        import requests

        url = f"https://raw.githubusercontent.com/digital-land/specification/main/content/dataset/{dataset_name}.md"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        content = response.text
        fields = []
        in_frontmatter = False
        in_fields_section = False

        for line in content.split("\n"):
            if line.strip() == "---":
                if not in_frontmatter:
                    in_frontmatter = True
                else:
                    break
                continue

            if in_frontmatter:
                if line.startswith("fields:"):
                    in_fields_section = True
                    continue
                if in_fields_section:
                    if line.startswith("- field:"):
                        field_name = line.split("- field:")[1].strip()
                        fields.append(field_name)
                    elif not line.startswith(" ") and not line.startswith("-"):
                        in_fields_section = False

        if not fields:
            return df

        current_columns = set(df.columns)
        missing_fields = [field for field in fields if field not in current_columns]

        if missing_fields:
            existing_cols = df.columns
            for field in missing_fields:
                df = df.withColumn(field, lit(""))
            final_columns = existing_cols + missing_fields
            df = df.select(final_columns)

        return df
    except Exception as e:
        logger.error(f"Error ensuring schema fields: {e}")
        return df
