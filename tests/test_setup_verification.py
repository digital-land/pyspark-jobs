"""
Simple test to verify the testing setup is working correctly.

This test should run without any external dependencies to validate
the basic testing infrastructure.
"""

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def test_python_environment():
    """Test that Python environment is working."""
    assert sys.version_info.major >= 3
    assert sys.version_info.minor >= 8


def test_import_paths():
    """Test that import paths are set up correctly."""
    # Should be able to import from jobs package
    try:
        from jobs.utils.logger_config import setup_logging

        assert callable(setup_logging)
    except ImportError as e:
        pytest.fail(f"Cannot import from jobs package: {e}")


def test_basic_pytest_functionality():
    """Test that pytest is working correctly."""
    assert True is True
    assert False is not True
    assert 1 + 1 == 2


def test_directory_structure():
    """Test that required directories exist."""
    test_dir = os.path.dirname(__file__)
    project_root = os.path.dirname(test_dir)

    # Check for key directories
    assert os.path.exists(os.path.join(project_root, "src"))
    assert os.path.exists(os.path.join(project_root, "src", "jobs"))
    assert os.path.exists(os.path.join(test_dir, "unit"))
    assert os.path.exists(os.path.join(test_dir, "integration"))


@pytest.mark.unit
def test_unit_marker():
    """Test that unit test marker works."""
    assert True


class TestBasicFixtures:
    """Test basic fixtures and configuration."""

    def test_sample_database_config(self, sample_database_config):
        """Test that sample_database_config fixture works."""
        assert "host" in sample_database_config
        assert "port" in sample_database_config
        assert sample_database_config["host"] == "localhost"

    def test_sample_s3_config(self, sample_s3_config):
        """Test that sample_s3_config fixture works."""
        assert "bucket" in sample_s3_config
        assert "region" in sample_s3_config
        assert sample_s3_config["bucket"] == "test - bucket"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
