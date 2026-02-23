import json
import logging
import os
import pkgutil

import boto3

from jobs.utils.logger_config import log_execution_time

logger = logging.getLogger(__name__)


@log_execution_time
def load_metadata(uri: str) -> dict:
    """
    Load a JSON configuration file from either an S3 URI or a local file path.

    Args:
        uri (str): S3 URI (e.g., s3://bucket/key) or local file path

    Returns:
        dict: Parsed JSON content.

    Raises:
        FileNotFoundError: If the file is not found.
        ValueError: If the file content is invalid.
    """
    logger.info(f"Loading metadata from {uri}")
    try:
        if uri.lower().startswith("s3://"):
            # Handle S3 path
            s3 = boto3.client("s3")
            bucket, key = uri.replace("s3://", "", 1).split("/", 1)
            response = s3.get_object(Bucket=bucket, Key=key)
            return json.load(response["Body"])
        else:
            # Handle local file path or file within .whl package
            try:
                # Try to load using pkgutil if running from .whl
                package_name = __package__ or "jobs"  # will be 'jobs'
                logger.info(
                    f"Attempting to load from package using pkgutil with package_name: {package_name} and uri: {uri}"
                )
                data = pkgutil.get_data(package_name, uri)
                if data:
                    logger.info("Successfully loaded from package using pkgutil")
                    return json.loads(data.decode("utf-8"))
                else:
                    raise FileNotFoundError(f"pkgutil.get_data could not find {uri}")
            except (
                FileNotFoundError,
                json.JSONDecodeError,
                UnicodeDecodeError,
                AttributeError,
            ) as e:
                # If pkgutil fails, try to load from the file system
                logger.warning(
                    f"pkgutil.get_data failed: {e}, attempting to read from file system"
                )
                try:
                    # Check if the path is absolute
                    if os.path.isabs(uri):
                        filepath = uri
                    else:
                        # Construct the absolute path relative to the script's location
                        script_dir = os.path.dirname(os.path.abspath(__file__))
                        filepath = os.path.normpath(os.path.join(script_dir, uri))

                        # Validate path doesn't escape base directory
                        if not filepath.startswith(script_dir):
                            raise ValueError(
                                f"Invalid file path: path traversal detected in {uri}"
                            )

                    logger.info(
                        f"Attempting to load from file system with filepath: {filepath}"
                    )
                    with open(filepath, "r") as f:
                        logger.info("Successfully loaded from file system")
                        return json.load(f)
                except FileNotFoundError as e:
                    logger.error(f"Configuration file not found in file system: {e}")
                    raise
                except (json.JSONDecodeError, IOError) as e:
                    logger.error(f"Error reading or parsing file: {e}")
                    raise
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading metadata from {uri}: {e}")
        raise
