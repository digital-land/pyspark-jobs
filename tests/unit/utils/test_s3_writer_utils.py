"""Unit tests for s3_writer_utils pure-Python helpers (no Spark session)."""

import json
from datetime import date
from unittest.mock import patch

import pytest

from jobs.utils import s3_writer_utils
from jobs.utils.s3_writer_utils import (
    _sanitize_json_row,
    _upload_geojson_entities_partition,
    _upload_json_entities_partition,
    resolve_geometry,
    s3_rename_and_move,
)


class FakeRow:
    """Stands in for a pyspark.sql.Row -- just needs .asDict()."""

    def __init__(self, d):
        self._d = d

    def asDict(self):
        return dict(self._d)


def test_resolve_geometry_uses_geometry_when_present():
    """Uses geometry WKT when geometry is available."""
    result = resolve_geometry("POINT(1.0 2.0)", None)
    assert result == {"type": "Point", "coordinates": [1.0, 2.0]}


def test_resolve_geometry_falls_back_to_point_when_geometry_absent():
    """Falls back to point WKT when geometry is None."""
    result = resolve_geometry(None, "POINT(3.0 4.0)")
    assert result == {"type": "Point", "coordinates": [3.0, 4.0]}


def test_resolve_geometry_falls_back_to_point_when_geometry_empty_string():
    """Falls back to point WKT when geometry is an empty string."""
    result = resolve_geometry("", "POINT(3.0 4.0)")
    assert result == {"type": "Point", "coordinates": [3.0, 4.0]}


def test_resolve_geometry_prefers_geometry_over_point_when_both_present():
    """Uses geometry over point when both are provided."""
    result = resolve_geometry("POINT(1.0 2.0)", "POINT(9.0 9.0)")
    assert result == {"type": "Point", "coordinates": [1.0, 2.0]}


def test_resolve_geometry_returns_none_when_both_absent():
    """Returns None when both geometry and point are absent."""
    assert resolve_geometry(None, None) is None


def test_resolve_geometry_returns_none_when_both_empty():
    """Returns None when both geometry and point are empty strings."""
    assert resolve_geometry("", "") is None


def test_sanitize_json_row_converts_date_to_isoformat():
    """Date values are converted to ISO 8601 strings."""
    row = FakeRow({"d": date(2024, 1, 2)})
    assert _sanitize_json_row(row) == {"d": "2024-01-02"}


def test_sanitize_json_row_converts_none_to_empty_string():
    """None values are converted to empty strings, not JSON null."""
    row = FakeRow({"name": None})
    assert _sanitize_json_row(row) == {"name": ""}


def test_sanitize_json_row_leaves_other_values_unchanged():
    """Non-date, non-None values pass through unchanged."""
    row = FakeRow({"id": 1, "name": "x", "active": True})
    assert _sanitize_json_row(row) == {"id": 1, "name": "x", "active": True}


def _upload_partition(partition_index, rows, first_idx, last_idx):
    """Call _upload_json_entities_partition with boto3 mocked out, returning
    (part_number, body) instead of (part_number, etag) so tests can inspect
    the actual serialized bytes."""
    captured = {}

    class FakeS3:
        def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
            captured["part_number"] = PartNumber
            captured["body"] = Body
            return {"ETag": "fake-etag"}

    with patch("jobs.utils.s3_writer_utils.boto3") as mock_boto3:
        mock_boto3.client.return_value = FakeS3()
        result = list(
            _upload_json_entities_partition(
                "bucket", "key", "upload-1", first_idx, last_idx, partition_index, rows
            )
        )
    return result, captured.get("body")


def test_upload_json_entities_partition_empty_partition_uploads_nothing():
    """An empty partition contributes no part."""
    result, body = _upload_partition(1, [], first_idx=0, last_idx=2)
    assert result == []
    assert body is None


def test_upload_json_entities_partition_single_partition_wraps_full_document():
    """A lone partition (first == last) gets both the opening and closing brackets."""
    rows = [FakeRow({"id": 1}), FakeRow({"id": 2})]
    result, body = _upload_partition(0, rows, first_idx=0, last_idx=0)
    assert result == [(1, "fake-etag")]
    assert body == '{"entities":[{"id": 1},{"id": 2}]}'
    assert json.loads(body) == {"entities": [{"id": 1}, {"id": 2}]}


def test_upload_json_entities_partition_first_partition_opens_without_leading_comma():
    """The first non-empty partition opens the array and has no leading comma."""
    rows = [FakeRow({"id": 1})]
    _, body = _upload_partition(0, rows, first_idx=0, last_idx=3)
    assert body == '{"entities":[{"id": 1}'


def test_upload_json_entities_partition_middle_partition_gets_leading_comma_only():
    """A middle partition is neither header nor footer -- just a comma-prefixed chunk."""
    rows = [FakeRow({"id": 5}), FakeRow({"id": 6})]
    _, body = _upload_partition(2, rows, first_idx=0, last_idx=3)
    assert body == ',{"id": 5},{"id": 6}'


def test_upload_json_entities_partition_last_partition_closes_document():
    """The last non-empty partition appends the closing bracket."""
    rows = [FakeRow({"id": 9})]
    _, body = _upload_partition(3, rows, first_idx=0, last_idx=3)
    assert body == ',{"id": 9}]}'


def _assemble(partitions, first_idx, last_idx):
    """Run every partition through _upload_json_entities_partition (boto3
    mocked) and stitch the resulting parts back together in part-number
    order, mirroring what S3's complete_multipart_upload does."""
    parts = []
    with patch("jobs.utils.s3_writer_utils.boto3") as mock_boto3:
        bodies = {}

        class FakeS3:
            def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
                bodies[PartNumber] = Body
                return {"ETag": f"etag-{PartNumber}"}

        mock_boto3.client.return_value = FakeS3()
        for idx, rows in enumerate(partitions):
            result = list(
                _upload_json_entities_partition(
                    "bucket", "key", "upload-1", first_idx, last_idx, idx, rows
                )
            )
            parts.extend(result)

    ordered = sorted(parts)
    return "".join(bodies[num] for num, _ in ordered)


def test_upload_json_entities_partition_assembles_valid_json_across_partitions():
    """Multiple non-empty partitions stitch together into one valid document."""
    partitions = [
        [FakeRow({"id": 0}), FakeRow({"id": 1})],
        [FakeRow({"id": 2})],
        [FakeRow({"id": 3}), FakeRow({"id": 4})],
    ]
    body = _assemble(partitions, first_idx=0, last_idx=2)
    parsed = json.loads(body)
    assert sorted(e["id"] for e in parsed["entities"]) == [0, 1, 2, 3, 4]


def test_upload_json_entities_partition_handles_empty_first_partition():
    """If the first physical partition is empty, the next non-empty one must
    open the document -- regresses a bug where the header was silently
    dropped because it was only ever attached to partition index 0."""
    partitions = [[], [FakeRow({"id": 0}), FakeRow({"id": 1})], [FakeRow({"id": 2})]]
    body = _assemble(partitions, first_idx=1, last_idx=2)
    parsed = json.loads(body)
    assert sorted(e["id"] for e in parsed["entities"]) == [0, 1, 2]


def test_upload_json_entities_partition_handles_empty_last_partition():
    """If the last physical partition is empty, the previous non-empty one
    must close the document -- regresses the same bug for the footer."""
    partitions = [[FakeRow({"id": 0})], [FakeRow({"id": 1}), FakeRow({"id": 2})], []]
    body = _assemble(partitions, first_idx=0, last_idx=1)
    parsed = json.loads(body)
    assert sorted(e["id"] for e in parsed["entities"]) == [0, 1, 2]


def test_upload_json_entities_partition_handles_empty_first_last_and_middle_gap():
    """Empty partitions at both ends plus a gap in the middle -- the layout
    actually observed from a real Spark repartition() call in practice."""
    partitions = [
        [],
        [FakeRow({"id": 0})],
        [],
        [FakeRow({"id": 1}), FakeRow({"id": 2})],
        [],
    ]
    body = _assemble(partitions, first_idx=1, last_idx=3)
    parsed = json.loads(body)
    assert sorted(e["id"] for e in parsed["entities"]) == [0, 1, 2]


def _upload_geojson_partition(partition_index, rows, first_idx, last_idx, dataset="ds"):
    """Call _upload_geojson_entities_partition with boto3 mocked out,
    returning (part_number, body) instead of (part_number, etag)."""
    captured = {}

    class FakeS3:
        def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
            captured["part_number"] = PartNumber
            captured["body"] = Body
            return {"ETag": "fake-etag"}

    with patch("jobs.utils.s3_writer_utils.boto3") as mock_boto3:
        mock_boto3.client.return_value = FakeS3()
        result = list(
            _upload_geojson_entities_partition(
                "bucket",
                "key",
                "upload-1",
                dataset,
                first_idx,
                last_idx,
                partition_index,
                rows,
            )
        )
    return result, captured.get("body")


def test_upload_geojson_entities_partition_empty_partition_uploads_nothing():
    """An empty partition contributes no part."""
    result, body = _upload_geojson_partition(1, [], first_idx=0, last_idx=2)
    assert result == []
    assert body is None


def test_upload_geojson_entities_partition_single_partition_wraps_full_document():
    """A lone partition gets both the FeatureCollection header and footer."""
    rows = [FakeRow({"id": 1, "geometry": "POINT(1.0 2.0)", "point": None})]
    result, body = _upload_geojson_partition(
        0, rows, first_idx=0, last_idx=0, dataset="my-ds"
    )
    assert result == [(1, "fake-etag")]
    parsed = json.loads(body)
    assert parsed == {
        "type": "FeatureCollection",
        "name": "my-ds",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": 1},
                "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
            }
        ],
    }


def test_upload_geojson_entities_partition_pops_geometry_and_point_from_properties():
    """geometry/point WKT columns are consumed for the geometry, not left in properties."""
    rows = [
        FakeRow({"id": 1, "name": "x", "geometry": "POINT(1.0 2.0)", "point": None})
    ]
    _, body = _upload_geojson_partition(0, rows, first_idx=0, last_idx=0)
    feature = json.loads(body)["features"][0]
    assert feature["properties"] == {"id": 1, "name": "x"}


def test_upload_geojson_entities_partition_falls_back_to_point_when_geometry_absent():
    """Matches resolve_geometry's fallback: point WKT used when geometry is missing."""
    rows = [FakeRow({"id": 1, "geometry": None, "point": "POINT(3.0 4.0)"})]
    _, body = _upload_geojson_partition(0, rows, first_idx=0, last_idx=0)
    feature = json.loads(body)["features"][0]
    assert feature["geometry"] == {"type": "Point", "coordinates": [3.0, 4.0]}


def test_upload_geojson_entities_partition_converts_polygon():
    rows = [
        FakeRow(
            {
                "id": 1,
                "geometry": "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "point": None,
            }
        )
    ]
    _, body = _upload_geojson_partition(0, rows, first_idx=0, last_idx=0)
    feature = json.loads(body)["features"][0]
    assert feature["geometry"] == {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
    }


def test_upload_geojson_entities_partition_null_geometry_when_both_absent():
    rows = [FakeRow({"id": 1, "geometry": None, "point": None})]
    _, body = _upload_geojson_partition(0, rows, first_idx=0, last_idx=0)
    feature = json.loads(body)["features"][0]
    assert feature["geometry"] is None


def test_upload_geojson_entities_partition_first_partition_opens_without_leading_comma():
    rows = [FakeRow({"id": 1, "geometry": None, "point": None})]
    _, body = _upload_geojson_partition(0, rows, first_idx=0, last_idx=3, dataset="ds")
    assert body.startswith('{"type":"FeatureCollection","name":"ds","features":[{')
    assert not body.startswith('{"type":"FeatureCollection","name":"ds","features":[,')


def test_upload_geojson_entities_partition_middle_partition_gets_leading_comma_only():
    rows = [FakeRow({"id": 5, "geometry": None, "point": None})]
    _, body = _upload_geojson_partition(2, rows, first_idx=0, last_idx=3)
    assert body.startswith(",")
    assert "FeatureCollection" not in body


def test_upload_geojson_entities_partition_last_partition_closes_document():
    rows = [FakeRow({"id": 9, "geometry": None, "point": None})]
    _, body = _upload_geojson_partition(3, rows, first_idx=0, last_idx=3)
    assert body.endswith("]}")


def _assemble_geojson(partitions, first_idx, last_idx, dataset="ds"):
    """Run every partition through _upload_geojson_entities_partition (boto3
    mocked) and stitch the resulting parts back together in part-number
    order, mirroring what S3's complete_multipart_upload does."""
    parts = []
    with patch("jobs.utils.s3_writer_utils.boto3") as mock_boto3:
        bodies = {}

        class FakeS3:
            def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
                bodies[PartNumber] = Body
                return {"ETag": f"etag-{PartNumber}"}

        mock_boto3.client.return_value = FakeS3()
        for idx, rows in enumerate(partitions):
            result = list(
                _upload_geojson_entities_partition(
                    "bucket",
                    "key",
                    "upload-1",
                    dataset,
                    first_idx,
                    last_idx,
                    idx,
                    rows,
                )
            )
            parts.extend(result)

    ordered = sorted(parts)
    return "".join(bodies[num] for num, _ in ordered)


def test_upload_geojson_entities_partition_handles_empty_first_last_and_middle_gap():
    """Same pathological layout regression as the JSON writer: empty
    partitions at both ends plus a gap in the middle."""
    partitions = [
        [],
        [FakeRow({"id": 0, "geometry": None, "point": None})],
        [],
        [
            FakeRow({"id": 1, "geometry": None, "point": None}),
            FakeRow({"id": 2, "geometry": None, "point": None}),
        ],
        [],
    ]
    body = _assemble_geojson(partitions, first_idx=1, last_idx=3)
    parsed = json.loads(body)
    assert parsed["type"] == "FeatureCollection"
    assert sorted(f["properties"]["id"] for f in parsed["features"]) == [0, 1, 2]


class FakeS3RenameClient:
    """Fake boto3 S3 client covering exactly the calls s3_rename_and_move
    and _copy_large_s3_object make."""

    class exceptions:
        ClientError = Exception

    def __init__(self, target_key, temp_files, target_exists=False):
        self.target_key = target_key
        self.temp_files = dict(temp_files)  # key -> size_bytes
        self.target_exists = target_exists
        self.copy_calls = []
        self.copy_part_calls = []
        self.deleted = []
        self.completed_parts = None
        self.aborted = False
        self.fail_upload_part_copy_after = None

    def head_object(self, Bucket, Key):
        if Key == self.target_key:
            if self.target_exists:
                return {}
            raise self.exceptions.ClientError("not found")
        if Key in self.temp_files:
            return {"ContentLength": self.temp_files[Key]}
        raise self.exceptions.ClientError("not found")

    def delete_object(self, Bucket, Key):
        self.deleted.append(Key)
        self.target_exists = False

    def list_objects_v2(self, Bucket, Prefix):
        return {
            "Contents": [
                {"Key": key} for key in self.temp_files if key.startswith(Prefix)
            ]
        }

    def copy_object(self, Bucket, CopySource, Key):
        self.copy_calls.append({"CopySource": CopySource, "Key": Key})

    def create_multipart_upload(self, Bucket, Key):
        return {"UploadId": "upload-1"}

    def upload_part_copy(
        self, Bucket, Key, PartNumber, UploadId, CopySource, CopySourceRange
    ):
        if self.fail_upload_part_copy_after is not None and (
            PartNumber > self.fail_upload_part_copy_after
        ):
            raise RuntimeError("simulated copy failure")
        self.copy_part_calls.append(
            {"PartNumber": PartNumber, "CopySourceRange": CopySourceRange}
        )
        return {"CopyPartResult": {"ETag": f"etag-{PartNumber}"}}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        self.completed_parts = MultipartUpload["Parts"]

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted = True


def test_s3_rename_and_move_uses_simple_copy_under_the_5gb_limit():
    """Files at or under CopyObject's 5GB limit use the simple, cheap copy."""
    fake = FakeS3RenameClient(
        target_key="dataset/ds.csv",
        temp_files={"dataset/temp/ds/part-0.csv": 100},
    )
    with patch.object(s3_writer_utils, "boto3") as mock_boto3:
        mock_boto3.client.return_value = fake
        s3_rename_and_move("ds", "csv", "bucket")

    assert fake.copy_calls == [
        {
            "CopySource": {"Bucket": "bucket", "Key": "dataset/temp/ds/part-0.csv"},
            "Key": "dataset/ds.csv",
        }
    ]
    assert fake.copy_part_calls == []
    assert fake.deleted == ["dataset/temp/ds/part-0.csv"]


def test_s3_rename_and_move_uses_multipart_copy_over_the_5gb_limit(monkeypatch):
    """Files over CopyObject's 5GB limit fall back to a multipart
    UploadPartCopy, split into correctly-bounded byte ranges."""
    monkeypatch.setattr(s3_writer_utils, "S3_COPY_OBJECT_MAX_BYTES", 20)
    monkeypatch.setattr(s3_writer_utils, "S3_MULTIPART_COPY_PART_BYTES", 10)

    fake = FakeS3RenameClient(
        target_key="dataset/ds.csv",
        temp_files={"dataset/temp/ds/part-0.csv": 25},
    )
    with patch.object(s3_writer_utils, "boto3") as mock_boto3:
        mock_boto3.client.return_value = fake
        s3_rename_and_move("ds", "csv", "bucket")

    assert fake.copy_calls == []
    assert fake.copy_part_calls == [
        {"PartNumber": 1, "CopySourceRange": "bytes=0-9"},
        {"PartNumber": 2, "CopySourceRange": "bytes=10-19"},
        {"PartNumber": 3, "CopySourceRange": "bytes=20-24"},
    ]
    assert fake.completed_parts == [
        {"PartNumber": 1, "ETag": "etag-1"},
        {"PartNumber": 2, "ETag": "etag-2"},
        {"PartNumber": 3, "ETag": "etag-3"},
    ]
    assert not fake.aborted
    assert fake.deleted == ["dataset/temp/ds/part-0.csv"]


def test_s3_rename_and_move_aborts_multipart_copy_on_failure(monkeypatch):
    """A failed part copy aborts the multipart upload and propagates."""
    monkeypatch.setattr(s3_writer_utils, "S3_COPY_OBJECT_MAX_BYTES", 20)
    monkeypatch.setattr(s3_writer_utils, "S3_MULTIPART_COPY_PART_BYTES", 10)

    fake = FakeS3RenameClient(
        target_key="dataset/ds.csv",
        temp_files={"dataset/temp/ds/part-0.csv": 25},
    )
    fake.fail_upload_part_copy_after = 1

    with patch.object(s3_writer_utils, "boto3") as mock_boto3:
        mock_boto3.client.return_value = fake
        with pytest.raises(RuntimeError, match="simulated copy failure"):
            s3_rename_and_move("ds", "csv", "bucket")

    assert fake.aborted
    assert fake.completed_parts is None
    # the failed copy means the source temp file must NOT be deleted
    assert fake.deleted == []
