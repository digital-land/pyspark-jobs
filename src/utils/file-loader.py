import os
import yaml

def load_yaml_from_home(relative_path):
    """
    Loads a YAML file from a path relative to the user's home directory for local testing purposes.

    Args:
        relative_path (str): The path relative to the home directory (e.g., 'MHCLG-Repo/.../file.yaml').

    Returns:
        dict: Parsed YAML content.
    """
    full_path = os.path.expanduser(os.path.join("~", relative_path))
    with open(full_path, "r") as file:
        return yaml.safe_load(file)

# Example usage:
# from utils.file_loader import load_yaml_from_home

# def main():
#     yaml_data = load_yaml_from_home("MHCLG-Repo/digital-land-python/pyspark/config/transformed-source.yaml")
#     # Continue with ETL logic...

# if __name__ == "__main__":
#     main()

