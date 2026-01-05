import csv
import urllib.request

DATASET_SPEC_URL = "https://raw.githubusercontent.com/digital - land/specification/main/specification/dataset.csv"


def load_datasets():
    """Download and parse dataset.csv into a list of dictionaries."""
    with urllib.request.urlopen(DATASET_SPEC_URL) as response:
        content_lines = [line.decode("utf-8") for line in response.readlines()]
        reader = csv.DictReader(content_lines)
        return [row for row in reader]


def get_dataset_typology(dataset_name):
    """Return the typology for the given dataset name, or None if not found."""
    datasets = load_datasets()
    for row in datasets:
        if row.get("dataset") == dataset_name:
            return row.get("typology")
    return None
