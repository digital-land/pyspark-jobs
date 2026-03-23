import importlib.util
import os

import pytest
from click.testing import CliRunner


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
