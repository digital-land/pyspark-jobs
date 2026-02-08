import importlib.util
import os
import sys

import pytest
from click.testing import CliRunner
from pyspark.sql import SparkSession


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def run_main_cmd():
    """Load the click command from the run_main entry point script."""
    script_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "entry_points", "run_main.py"
        )
    )
    spec = importlib.util.spec_from_file_location("run_main", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.run


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for e2e acceptance tests."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]").appName("AcceptanceTest").getOrCreate()
    )
    yield session
    session.stop()
