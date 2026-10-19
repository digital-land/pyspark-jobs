"""
Unit tests for collection_paths utilities.
"""

from cloudpathlib import AnyPath

from jobs.utils.collection_paths import dataset_issue_files

# -- dataset_issue_files ------------------------------------------------------


def touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("")


def test_dataset_issue_files_keeps_only_transformed_resources(tmp_path):
    collection_dir = AnyPath(tmp_path) / "test-collection"
    for resource in ("res-b", "res-a", "res-retired"):
        touch(collection_dir / "issue" / "test-dataset" / f"{resource}.csv")
    for resource in ("res-a", "res-b"):
        touch(collection_dir / "transformed" / "test-dataset" / f"{resource}.parquet")

    result = dataset_issue_files(collection_dir, "test-dataset")

    assert result == [
        str(collection_dir / "issue" / "test-dataset" / "res-a.csv"),
        str(collection_dir / "issue" / "test-dataset" / "res-b.csv"),
    ]


def test_dataset_issue_files_ignores_other_transformed_formats(tmp_path):
    """The transformed folder also holds a CSV copy of each resource; only the
    parquet decides which resources the dataset is assembled from."""
    collection_dir = AnyPath(tmp_path) / "test-collection"
    touch(collection_dir / "issue" / "test-dataset" / "res-a.csv")
    touch(collection_dir / "transformed" / "test-dataset" / "res-a.csv")

    assert dataset_issue_files(collection_dir, "test-dataset") == []


def test_dataset_issue_files_returns_empty_when_a_folder_is_missing(tmp_path):
    collection_dir = AnyPath(tmp_path) / "test-collection"
    touch(collection_dir / "issue" / "test-dataset" / "res-a.csv")

    assert dataset_issue_files(collection_dir, "test-dataset") == []
