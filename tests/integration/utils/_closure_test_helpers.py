"""Helpers for validating the actual cloudpickled closures write_json_entities_s3
and write_geojson_entities_s3 ship to Spark executors.

Named so pytest's test-file collection (test_*.py / *_test.py) skips it.

Why this exists: those closures are deliberately nested (not module-level)
specifically so cloudpickle ships them *by value* instead of by reference --
a module-level reference would require the executor to `import jobs...`,
which fails on real EMR Serverless executors that don't have the `jobs`
wheel on PYTHONPATH (see write_json_entities_s3's docstring). Each closure
therefore carries its own inline copy of row-sanitizing / geometry-parsing
logic rather than calling the tested, importable reference implementations
(_upload_json_entities_partition, _upload_geojson_entities_partition). That
duplication is a real drift risk: nothing else in the test suite exercises
the closures that actually run in production. These helpers do, by
capturing the real closure from a real Spark job, cloudpickling it exactly
as Spark would, and executing it in a subprocess that cannot import `jobs`.
"""

import json
import subprocess
import sys

import pyspark.cloudpickle as cloudpickle
from pyspark.rdd import RDD


def capture_upload_closure(monkeypatch, run_write_fn):
    """Run run_write_fn() (a zero-arg callable that triggers a real
    write_json_entities_s3 / write_geojson_entities_s3 call) with
    RDD.mapPartitionsWithIndex patched to intercept the nested closure
    named `_upload_partition`, and return it.

    run_write_fn should use a trivial single-partition DataFrame so the
    captured closure's first/last-nonempty-partition bounds are both 0 --
    that lets callers later invoke `fn(0, arbitrary_rows)` directly and get
    a complete, self-contained document (opening AND closing brackets)
    regardless of what rows they pass, decoupling "capture a genuine
    closure" from "exercise it with interesting data".

    Local-mode Spark workers share this machine's filesystem, so `jobs` IS
    importable there (unlike real EMR executors) and the closure's real
    boto3.client("s3").upload_part(...) call actually fires and fails (no
    credentials/network) -- that failure is expected and swallowed; only
    the captured closure matters.
    """
    captured = {}
    real_map = RDD.mapPartitionsWithIndex

    def spying(self, f, preservesPartitioning=False):
        if getattr(f, "__name__", "") == "_upload_partition":
            captured["fn"] = f
        return real_map(self, f, preservesPartitioning)

    monkeypatch.setattr(RDD, "mapPartitionsWithIndex", spying)
    try:
        run_write_fn()
    except Exception:
        pass

    assert "fn" in captured, (
        "never captured the upload closure -- run_write_fn didn't reach "
        "mapPartitionsWithIndex as expected"
    )
    return captured["fn"]


def run_closure_without_jobs_importable(fn, child_body):
    """Pickle `fn` (as Spark would to ship it to an executor), then in a
    fresh subprocess where `jobs` cannot be imported: fake boto3.client("s3")
    to print each uploaded part's body instead of making a real call, and
    run `child_body` -- a Python source fragment that can reference the
    unpickled closure as `fn`.

    Returns the list of parsed JSON bodies printed, in upload order.
    """
    pickled_hex = cloudpickle.dumps(fn).hex()

    script = f"""
import sys, builtins
_real_import = builtins.__import__
def _blocking_import(name, *a, **kw):
    if name == "jobs" or name.startswith("jobs."):
        raise ModuleNotFoundError(f"No module named '{{name}}'")
    return _real_import(name, *a, **kw)
builtins.__import__ = _blocking_import

class FakeBoto3Module:
    class client:
        def __init__(self, service):
            pass
        def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
            print("UPLOAD_PART_BODY:" + Body)
            return {{"ETag": "e1"}}

sys.modules["boto3"] = FakeBoto3Module()

import pyspark.cloudpickle as cloudpickle
fn = cloudpickle.loads(bytes.fromhex("{pickled_hex}"))

{child_body}
"""
    proc = subprocess.run(
        [sys.executable, "-c", script], cwd="/tmp", capture_output=True, text=True
    )
    assert proc.returncode == 0, (
        f"closure failed in a subprocess without `jobs` importable:\n"
        f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    return [
        json.loads(line[len("UPLOAD_PART_BODY:") :])
        for line in proc.stdout.splitlines()
        if line.startswith("UPLOAD_PART_BODY:")
    ]
