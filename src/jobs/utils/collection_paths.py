"""Paths within the collection-data bucket.

Encodes where collected files live, so callers don't repeat the layout:

    {collection}-collection/collection/{filename}            log.csv, source.csv, ...
    {collection}-collection/issue/{dataset}/{resource}.csv

Discovery is built from targeted listings rather than glob(). A glob() with a
'/' in the pattern lists the WHOLE bucket and filters client-side, so finding a
few dozen log.csv files means walking every resource ever collected.
"""

import logging

from cloudpathlib import AnyPath

logger = logging.getLogger(__name__)


def collection_names(base: AnyPath) -> list[str]:
    """Top-level {collection}-collection folder names.

    iterdir() is a single Delimiter='/' call, where glob() with a '/' in the
    pattern lists the whole bucket and filters client-side.

    Args:
        base: Root of the collection-data bucket

    Returns:
        Sorted folder names, e.g. ["article-4-direction-collection", ...]
    """
    return sorted(p.name for p in base.iterdir() if p.name.endswith("-collection"))


def collection_files(base: AnyPath, collections: list[str], filename: str) -> list[str]:
    """Paths to {collection}-collection/collection/{filename} that exist.

    Builds the known paths directly and keeps the ones that are there, avoiding
    a full-bucket glob per file.

    Args:
        base: Root of the collection-data bucket
        collections: Folder names, from collection_names
        filename: File to look for in each collection, e.g. "log.csv"

    Returns:
        Paths as strings, ready to hand to spark.read
    """
    paths = [base / c / "collection" / filename for c in collections]
    return [str(p) for p in paths if p.exists()]


def issue_files_for_resources(
    base: AnyPath, collections: list[str], resources: set[str]
) -> list[str]:
    """Issue CSV paths for the given resources only.

    Issue files live at {collection}-collection/issue/{dataset}/{resource}.csv.
    Listing each dataset directory and intersecting on resource keeps this to a
    few hundred prefix listings, and returns only the files that will survive
    the join against active resources — roughly 3,000 rather than the ~26,450
    in the bucket.

    Restricting to active resources also keeps most legacy-format issue CSVs
    out of the read, but it is not a guarantee: an endpoint whose last
    successful collection was years ago still has a current resource, and its
    issue file may predate the layout changes. read_issue_csvs handles that.

    Args:
        base: Root of the collection-data bucket
        collections: Folder names, from collection_names
        resources: Resource hashes to keep

    Returns:
        Paths as strings, ready to hand to spark.read
    """
    files = []
    for collection in collections:
        issue_dir = base / collection / "issue"
        if not issue_dir.exists():
            continue
        for dataset_dir in issue_dir.iterdir():
            if not dataset_dir.is_dir():
                continue
            files.extend(str(p) for p in dataset_dir.iterdir() if p.stem in resources)
    return files


def expectation_files(base: AnyPath) -> list[str]:
    """Paths to the per-dataset expectation parquet files.

    These sit at the bucket root under a bare `log/`, not under a
    {collection}-collection prefix, because OUTPUT_LOG_DIR is `log/` and
    makerules syncs it straight to the root.

    The layout is hive-style (`dataset=<name>/`), but the files are listed
    rather than handed to Spark as a partitioned directory: the parquet
    already carries its own `dataset` column, so partition discovery would
    produce a second column of the same name and collide. Reading the files
    directly and using the in-file column avoids the question.

    Args:
        base: Root of the collection-data bucket

    Returns:
        Paths as strings, ready to hand to spark.read
    """
    expectation_dir = base / "log" / "expectation"
    if not expectation_dir.exists():
        logger.warning(f"expectation_files: {expectation_dir} does not exist")
        return []

    files = []
    for dataset_dir in expectation_dir.iterdir():
        if not dataset_dir.is_dir():
            continue
        files.extend(str(p) for p in dataset_dir.iterdir() if p.suffix == ".parquet")
    return files
