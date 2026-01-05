"""Unit tests for path_utils module."""

import json
import os
import sys
import tempfile
from unittest.mock import Mock, mock_open, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

import pytest

from jobs.utils.path_utils import (
    load_json_from_repo,
    resolve_desktop_path,
    resolve_repo_path,
)


class TestPathUtils:
    """Test suite for path_utils module."""

    @patch("os.path.expanduser")
    def test_resolve_desktop_path_success(self, mock_expanduser):
        """Test successful desktop path resolution."""
        mock_expanduser.return_value = "/home/testuser"

        result = resolve_desktop_path("test_file.txt")

        expected = "/home/testuser/Desktop/test_file.txt"
        assert result == expected
        mock_expanduser.assert_called_once_with("~")

    @patch("os.path.expanduser")
    def test_resolve_desktop_path_with_subdirectory(self, mock_expanduser):
        """Test desktop path resolution with subdirectory."""
        mock_expanduser.return_value = "/Users/testuser"

        result = resolve_desktop_path("projects/data/file.csv")

        expected = "/Users/testuser/Desktop/projects/data/file.csv"
        assert result == expected

    @patch("os.path.expanduser")
    def test_resolve_desktop_path_empty_relative_path(self, mock_expanduser):
        """Test desktop path resolution with empty relative path."""
        mock_expanduser.return_value = "/home/testuser"

        result = resolve_desktop_path("")

        expected = "/home/testuser/Desktop/"
        assert result == expected

    @patch("os.path.expanduser")
    def test_resolve_desktop_path_root_file(self, mock_expanduser):
        """Test desktop path resolution with root - level file."""
        mock_expanduser.return_value = "/home/testuser"

        result = resolve_desktop_path("document.pd")

        expected = "/home/testuser/Desktop/document.pd"
        assert result == expected

    @patch("os.path.expanduser")
    def test_resolve_repo_path_success(self, mock_expanduser):
        """Test successful repository path resolution."""
        mock_expanduser.return_value = "/home/testuser"

        result = resolve_repo_path("src/main.py")

        expected = "/home/testuser/github_repo/pyspark - jobs/src/main.py"
        assert result == expected
        mock_expanduser.assert_called_once_with("~")

    @patch("os.path.expanduser")
    def test_resolve_repo_path_with_nested_structure(self, mock_expanduser):
        """Test repository path resolution with nested directory structure."""
        mock_expanduser.return_value = "/Users/developer"

        result = resolve_repo_path("tests/unit/test_module.py")

        expected = (
            "/Users/developer/github_repo/pyspark - jobs/tests/unit/test_module.py"
        )
        assert result == expected

    @patch("os.path.expanduser")
    def test_resolve_repo_path_config_file(self, mock_expanduser):
        """Test repository path resolution for configuration files."""
        mock_expanduser.return_value = "/home/testuser"

        result = resolve_repo_path("config/settings.json")

        expected = "/home/testuser/github_repo/pyspark - jobs/config/settings.json"
        assert result == expected

    @patch("os.path.expanduser")
    def test_resolve_repo_path_empty_relative_path(self, mock_expanduser):
        """Test repository path resolution with empty relative path."""
        mock_expanduser.return_value = "/home/testuser"

        result = resolve_repo_path("")

        expected = "/home/testuser/github_repo/pyspark - jobs/"
        assert result == expected

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_success(self, mock_file, mock_resolve_path):
        """Test successful JSON loading from repository."""
        test_json = {"key": "value", "number": 42}
        mock_resolve_path.return_value = "/path/to/repo/config.json"
        mock_file.return_value.read.return_value = json.dumps(test_json)

        result = load_json_from_repo("config.json")

        assert result == test_json
        mock_resolve_path.assert_called_once_with("config.json")
        mock_file.assert_called_once_with("/path/to/repo/config.json", "r")

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_complex_structure(self, mock_file, mock_resolve_path):
        """Test JSON loading with complex nested structure."""
        test_json = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "credentials": {"username": "user", "password": "pass"},
            },
            "features": ["feature1", "feature2"],
            "enabled": True,
        }
        mock_resolve_path.return_value = "/path/to/repo/complex.json"
        mock_file.return_value.read.return_value = json.dumps(test_json)

        result = load_json_from_repo("config/complex.json")

        assert result == test_json
        assert result["database"]["host"] == "localhost"
        assert result["features"] == ["feature1", "feature2"]
        assert result["enabled"] is True

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_empty_file(self, mock_file, mock_resolve_path):
        """Test JSON loading from empty file."""
        mock_resolve_path.return_value = "/path/to/repo/empty.json"
        mock_file.return_value.read.return_value = "{}"

        result = load_json_from_repo("empty.json")

        assert result == {}

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_array_content(self, mock_file, mock_resolve_path):
        """Test JSON loading with array content."""
        test_json = [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"},
            {"id": 3, "name": "item3"},
        ]
        mock_resolve_path.return_value = "/path/to/repo/array.json"
        mock_file.return_value.read.return_value = json.dumps(test_json)

        result = load_json_from_repo("data/array.json")

        assert result == test_json
        assert len(result) == 3
        assert result[0]["name"] == "item1"

    @patch("jobs.utils.path_utils.resolve_repo_path")
    def test_load_json_from_repo_file_not_found(self, mock_resolve_path):
        """Test JSON loading when file doesn't exist."""
        mock_resolve_path.return_value = "/path/to/repo/nonexistent.json"

        with pytest.raises(FileNotFoundError):
            load_json_from_repo("nonexistent.json")

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_invalid_json(self, mock_file, mock_resolve_path):
        """Test JSON loading with invalid JSON content."""
        mock_resolve_path.return_value = "/path/to/repo/invalid.json"
        mock_file.return_value.read.return_value = "{ invalid json content"

        with pytest.raises(json.JSONDecodeError):
            load_json_from_repo("invalid.json")

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_permission_error(self, mock_file, mock_resolve_path):
        """Test JSON loading with permission error."""
        mock_resolve_path.return_value = "/path/to/repo/restricted.json"
        mock_file.side_effect = PermissionError("Permission denied")

        with pytest.raises(PermissionError):
            load_json_from_repo("restricted.json")

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_io_error(self, mock_file, mock_resolve_path):
        """Test JSON loading with IO error."""
        mock_resolve_path.return_value = "/path/to/repo/corrupted.json"
        mock_file.side_effect = IOError("Disk error")

        with pytest.raises(IOError):
            load_json_from_repo("corrupted.json")

    @patch("jobs.utils.path_utils.resolve_repo_path")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_json_from_repo_unicode_content(self, mock_file, mock_resolve_path):
        """Test JSON loading with Unicode content."""
        test_json = {
            "message": "Hello, 世界! 🌍",
            "symbols": "αβγδε",
            "emoji": "😀😃😄😁",
        }
        mock_resolve_path.return_value = "/path/to/repo/unicode.json"
        mock_file.return_value.read.return_value = json.dumps(
            test_json, ensure_ascii=False
        )

        result = load_json_from_repo("unicode.json")

        assert result == test_json
        assert "世界" in result["message"]
        assert "🌍" in result["message"]

    def test_path_resolution_integration(self):
        """Test integration between path resolution functions."""
        with patch("os.path.expanduser", return_value="/home/testuser"):
            # Test desktop path
            desktop_result = resolve_desktop_path("test.txt")
            assert desktop_result == "/home/testuser/Desktop/test.txt"

            # Test repo path
            repo_result = resolve_repo_path("src/test.py")
            assert (
                repo_result == "/home/testuser/github_repo/pyspark - jobs/src/test.py"
            )

            # Verify they use the same home directory
            assert desktop_result.startswith("/home/testuser/Desktop")
            assert repo_result.startswith("/home/testuser/github_repo")

    def test_path_normalization(self):
        """Test that paths are properly normalized."""
        with patch("os.path.expanduser", return_value="/home/testuser"):
            # Test with various path formats
            test_cases = [
                ("file.txt", "/home/testuser/Desktop/file.txt"),
                ("./file.txt", "/home/testuser/Desktop/./file.txt"),
                ("dir/file.txt", "/home/testuser/Desktop/dir/file.txt"),
                ("dir/../file.txt", "/home/testuser/Desktop/dir/../file.txt"),
            ]

            for input_path, expected in test_cases:
                result = resolve_desktop_path(input_path)
                assert result == expected


@pytest.mark.unit
class TestPathUtilsIntegration:
    """Integration - style tests for path_utils module."""

    def test_complete_json_loading_workflow(self):
        """Test complete JSON loading workflow with real file operations."""
        test_data = {
            "application": {"name": "pyspark - jobs", "version": "1.0.0"},
            "database": {"host": "localhost", "port": 5432},
            "features": ["logging", "monitoring", "testing"],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a temporary JSON file
            json_file = os.path.join(temp_dir, "test_config.json")
            with open(json_file, "w") as f:
                json.dump(test_data, f)

            # Mock resolve_repo_path to return our temp file
            with patch(
                "jobs.utils.path_utils.resolve_repo_path", return_value=json_file
            ):
                result = load_json_from_repo("config/test_config.json")

                assert result == test_data
                assert result["application"]["name"] == "pyspark - jobs"
                assert len(result["features"]) == 3

    def test_error_handling_workflow(self):
        """Test complete error handling workflow."""
        # Test file not found
        with patch(
            "jobs.utils.path_utils.resolve_repo_path",
            return_value="/nonexistent/path.json",
        ):
            with pytest.raises(FileNotFoundError):
                load_json_from_repo("nonexistent.json")

        # Test invalid JSON with real file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as temp_file:
            temp_file.write("{ invalid json")
            temp_file.flush()

            try:
                with patch(
                    "jobs.utils.path_utils.resolve_repo_path",
                    return_value=temp_file.name,
                ):
                    with pytest.raises(json.JSONDecodeError):
                        load_json_from_repo("invalid.json")
            finally:
                os.unlink(temp_file.name)

    def test_path_resolution_consistency(self):
        """Test that path resolution is consistent across different calls."""
        with patch("os.path.expanduser", return_value="/consistent/home"):
            # Multiple calls should return consistent results
            desktop1 = resolve_desktop_path("file1.txt")
            desktop2 = resolve_desktop_path("file2.txt")
            repo1 = resolve_repo_path("src/file1.py")
            repo2 = resolve_repo_path("src/file2.py")

            # All should use the same home directory
            assert desktop1.startswith("/consistent/home/Desktop")
            assert desktop2.startswith("/consistent/home/Desktop")
            assert repo1.startswith("/consistent/home/github_repo/pyspark - jobs")
            assert repo2.startswith("/consistent/home/github_repo/pyspark - jobs")

            # Different files should have different paths
            assert desktop1 != desktop2
            assert repo1 != repo2
