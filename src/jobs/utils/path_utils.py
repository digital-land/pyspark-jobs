import os
import json


def resolve_desktop_path(relative_path):
    """
    Resolves a path relative to the user's Desktop directory.
    """
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    return os.path.join(desktop_path, relative_path)


def resolve_repo_path(relative_path):
    """
    Resolves a path relative to the 'pyspark-jobs' repo, assuming it's in ~/github_repo/.
    """
    repo_base = os.path.join(os.path.expanduser("~"), "github_repo", "pyspark-jobs")
    return os.path.join(repo_base, relative_path)


def load_json_from_repo(relative_path):
    """
    Loads a JSON file from a path relative to the 'pyspark-jobs' repo.

    Args:
        relative_path (str): The path relative to the user's repo directory.

    Returns:
        dict: Parsed JSON content.
    """
    full_path = resolve_repo_path(relative_path)
    with open(full_path, "r") as file:
        return json.load(file)
