"""Unit tests for s3_dataset_typology module."""

import pytest
import os
import sys
import csv
from unittest.mock import Mock, patch, mock_open
from urllib.error import URLError, HTTPError

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))

from jobs.utils.s3_dataset_typology import (
    DATASET_SPEC_URL,
    load_datasets,
    get_dataset_typology,
)


class TestS3DatasetTypology:
    """Test suite for s3_dataset_typology module."""

    def test_dataset_spec_url_constant(self):
        """Test that DATASET_SPEC_URL is properly defined."""
        expected_url = "https://raw.githubusercontent.com/digital-land/specification/main/specification/dataset.csv"
        assert DATASET_SPEC_URL == expected_url

    @patch("urllib.request.urlopen")
    def test_load_datasets_success(self, mock_urlopen):
        """Test successful dataset loading from URL."""
        # Mock CSV content
        csv_content = """dataset,typology,name
transport-access-node,geography,Transport access node
title-boundary,geography,Title boundary
conservation-area,geography,Conservation area
listed-building,heritage,Listed building"""

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = load_datasets()

        assert len(result) == 4
        assert result[0]["dataset"] == "transport-access-node"
        assert result[0]["typology"] == "geography"
        assert result[0]["name"] == "Transport access node"

        assert result[3]["dataset"] == "listed-building"
        assert result[3]["typology"] == "heritage"

    @patch("urllib.request.urlopen")
    def test_load_datasets_empty_response(self, mock_urlopen):
        """Test dataset loading with empty response."""
        # Mock empty CSV (only header)
        csv_content = "dataset,typology,name"

        mock_response = Mock()
        mock_response.readlines.return_value = [csv_content.encode("utf-8")]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = load_datasets()

        assert len(result) == 0

    @patch("urllib.request.urlopen")
    def test_load_datasets_single_row(self, mock_urlopen):
        """Test dataset loading with single data row."""
        csv_content = """dataset,typology,name
single-dataset,test-typology,Single Dataset"""

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = load_datasets()

        assert len(result) == 1
        assert result[0]["dataset"] == "single-dataset"
        assert result[0]["typology"] == "test-typology"
        assert result[0]["name"] == "Single Dataset"

    @patch("urllib.request.urlopen")
    def test_load_datasets_with_special_characters(self, mock_urlopen):
        """Test dataset loading with special characters and Unicode."""
        csv_content = """dataset,typology,name
special-chars,geography,"Dataset with, comma"
unicode-test,heritage,Café & Restaurant
quotes-test,geography,"Dataset ""with"" quotes\""""

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = load_datasets()

        assert len(result) == 3
        assert result[0]["name"] == "Dataset with, comma"
        assert result[1]["name"] == "Café & Restaurant"
        assert result[2]["name"] == 'Dataset "with" quotes'

    @patch("urllib.request.urlopen")
    def test_load_datasets_network_error(self, mock_urlopen):
        """Test dataset loading with network error."""
        mock_urlopen.side_effect = URLError("Network error")

        with pytest.raises(URLError):
            load_datasets()

    @patch("urllib.request.urlopen")
    def test_load_datasets_http_error(self, mock_urlopen):
        """Test dataset loading with HTTP error."""
        mock_urlopen.side_effect = HTTPError(
            url=DATASET_SPEC_URL, code=404, msg="Not Found", hdrs=None, fp=None
        )

        with pytest.raises(HTTPError):
            load_datasets()

    @patch("urllib.request.urlopen")
    def test_load_datasets_malformed_csv(self, mock_urlopen):
        """Test dataset loading with malformed CSV."""
        # CSV with inconsistent columns
        csv_content = """dataset,typology,name
valid-row,geography,Valid Dataset
invalid-row,missing-name
another-valid,heritage,Another Dataset"""

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = load_datasets()

        # Should handle malformed rows gracefully
        assert len(result) >= 2  # At least the valid rows
        valid_datasets = [row for row in result if "name" in row and row["name"]]
        assert len(valid_datasets) >= 2

    @patch("urllib.request.urlopen")
    def test_load_datasets_encoding_issues(self, mock_urlopen):
        """Test dataset loading with encoding issues."""
        csv_content = """dataset,typology,name
encoding-test,geography,Test with émojis 🏠"""

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = load_datasets()

        assert len(result) == 1
        assert result[0]["name"] == "Test with émojis 🏠"

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_found(self, mock_load_datasets):
        """Test successful typology retrieval for existing dataset."""
        mock_datasets = [
            {
                "dataset": "transport-access-node",
                "typology": "geography",
                "name": "Transport access node",
            },
            {
                "dataset": "listed-building",
                "typology": "heritage",
                "name": "Listed building",
            },
            {
                "dataset": "conservation-area",
                "typology": "geography",
                "name": "Conservation area",
            },
        ]
        mock_load_datasets.return_value = mock_datasets

        result = get_dataset_typology("transport-access-node")

        assert result == "geography"
        mock_load_datasets.assert_called_once()

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_not_found(self, mock_load_datasets):
        """Test typology retrieval for non-existent dataset."""
        mock_datasets = [
            {
                "dataset": "transport-access-node",
                "typology": "geography",
                "name": "Transport access node",
            },
            {
                "dataset": "listed-building",
                "typology": "heritage",
                "name": "Listed building",
            },
        ]
        mock_load_datasets.return_value = mock_datasets

        result = get_dataset_typology("non-existent-dataset")

        assert result is None
        mock_load_datasets.assert_called_once()

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_empty_datasets(self, mock_load_datasets):
        """Test typology retrieval with empty dataset list."""
        mock_load_datasets.return_value = []

        result = get_dataset_typology("any-dataset")

        assert result is None
        mock_load_datasets.assert_called_once()

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_missing_typology_field(self, mock_load_datasets):
        """Test typology retrieval when typology field is missing."""
        mock_datasets = [
            {
                "dataset": "transport-access-node",
                "name": "Transport access node",
            },  # Missing typology
            {
                "dataset": "listed-building",
                "typology": "heritage",
                "name": "Listed building",
            },
        ]
        mock_load_datasets.return_value = mock_datasets

        result = get_dataset_typology("transport-access-node")

        assert result is None  # Should return None when typology field is missing

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_empty_typology_value(self, mock_load_datasets):
        """Test typology retrieval when typology value is empty."""
        mock_datasets = [
            {
                "dataset": "transport-access-node",
                "typology": "",
                "name": "Transport access node",
            },
            {
                "dataset": "listed-building",
                "typology": "heritage",
                "name": "Listed building",
            },
        ]
        mock_load_datasets.return_value = mock_datasets

        result = get_dataset_typology("transport-access-node")

        assert result == ""  # Should return empty string if that's the value

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_case_sensitivity(self, mock_load_datasets):
        """Test that dataset name matching is case-sensitive."""
        mock_datasets = [
            {
                "dataset": "Transport-Access-Node",
                "typology": "geography",
                "name": "Transport access node",
            },
            {
                "dataset": "transport-access-node",
                "typology": "geography",
                "name": "Transport access node",
            },
        ]
        mock_load_datasets.return_value = mock_datasets

        # Test exact match
        result1 = get_dataset_typology("transport-access-node")
        assert result1 == "geography"

        # Test case mismatch
        result2 = get_dataset_typology("Transport-Access-Node")
        assert result2 == "geography"

        # Test no match
        result3 = get_dataset_typology("TRANSPORT-ACCESS-NODE")
        assert result3 is None

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_multiple_matches(self, mock_load_datasets):
        """Test typology retrieval when there are multiple matches (should return first)."""
        mock_datasets = [
            {
                "dataset": "duplicate-dataset",
                "typology": "first-typology",
                "name": "First Dataset",
            },
            {
                "dataset": "duplicate-dataset",
                "typology": "second-typology",
                "name": "Second Dataset",
            },
            {
                "dataset": "other-dataset",
                "typology": "other-typology",
                "name": "Other Dataset",
            },
        ]
        mock_load_datasets.return_value = mock_datasets

        result = get_dataset_typology("duplicate-dataset")

        assert result == "first-typology"  # Should return the first match

    @patch("jobs.utils.s3_dataset_typology.load_datasets")
    def test_get_dataset_typology_load_datasets_exception(self, mock_load_datasets):
        """Test typology retrieval when load_datasets raises an exception."""
        mock_load_datasets.side_effect = URLError("Network error")

        with pytest.raises(URLError):
            get_dataset_typology("any-dataset")

    def test_get_dataset_typology_none_input(self):
        """Test typology retrieval with None input."""
        with patch(
            "jobs.utils.s3_dataset_typology.load_datasets"
        ) as mock_load_datasets:
            mock_datasets = [
                {
                    "dataset": "test-dataset",
                    "typology": "test-typology",
                    "name": "Test Dataset",
                }
            ]
            mock_load_datasets.return_value = mock_datasets

            result = get_dataset_typology(None)

            assert result is None

    def test_get_dataset_typology_empty_string_input(self):
        """Test typology retrieval with empty string input."""
        with patch(
            "jobs.utils.s3_dataset_typology.load_datasets"
        ) as mock_load_datasets:
            mock_datasets = [
                {
                    "dataset": "test-dataset",
                    "typology": "test-typology",
                    "name": "Test Dataset",
                }
            ]
            mock_load_datasets.return_value = mock_datasets

            result = get_dataset_typology("")

            assert result is None


@pytest.mark.unit
class TestS3DatasetTypologyIntegration:
    """Integration-style tests for s3_dataset_typology module."""

    @patch("urllib.request.urlopen")
    def test_complete_workflow_success(self, mock_urlopen):
        """Test complete workflow from URL fetch to typology retrieval."""
        # Mock realistic dataset CSV content
        csv_content = """dataset,typology,name,description,phase,collection,consideration
transport-access-node,geography,Transport access node,Points of access to the transport network,beta,transport,
title-boundary,geography,Title boundary,Land registry title boundaries,alpha,title-boundary,
conservation-area,geography,Conservation area,Areas designated for conservation,live,conservation-area,
listed-building,heritage,Listed building,Buildings of special architectural or historic interest,live,listed-building,
tree-preservation-order,geography,Tree preservation order,Orders to protect trees,beta,tree,"""

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Test loading datasets
        datasets = load_datasets()
        assert len(datasets) == 5

        # Test getting specific typologies
        assert get_dataset_typology("transport-access-node") == "geography"
        assert get_dataset_typology("listed-building") == "heritage"
        assert get_dataset_typology("conservation-area") == "geography"
        assert get_dataset_typology("non-existent") is None

        # Verify URL was called correctly
        mock_urlopen.assert_called_with(DATASET_SPEC_URL)

    @patch("urllib.request.urlopen")
    def test_error_handling_workflow(self, mock_urlopen):
        """Test complete error handling workflow."""
        # Test network error propagation
        mock_urlopen.side_effect = URLError("Connection failed")

        with pytest.raises(URLError):
            load_datasets()

        with pytest.raises(URLError):
            get_dataset_typology("any-dataset")

    @patch("urllib.request.urlopen")
    def test_real_world_csv_format(self, mock_urlopen):
        """Test with realistic CSV format including edge cases."""
        csv_content = """dataset,typology,name,description,phase,collection,consideration
"dataset-with-commas,in-name",geography,"Dataset, with commas",Description with commas,live,collection,
dataset-with-quotes,heritage,"Dataset ""with"" quotes","Description ""with"" quotes",beta,collection,
dataset-with-unicode,geography,Café & Résumé,Unicode description: 🏠🌳,alpha,unicode-collection,
empty-typology,,Empty Typology Dataset,Dataset with empty typology,live,empty,
missing-fields,geography,Missing Fields
extra-field,geography,Extra Field Dataset,Normal dataset,live,collection,extra-consideration,extra-field"""

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        datasets = load_datasets()

        # Test various edge cases
        assert get_dataset_typology("dataset-with-commas,in-name") == "geography"
        assert get_dataset_typology("dataset-with-quotes") == "heritage"
        assert get_dataset_typology("dataset-with-unicode") == "geography"
        assert get_dataset_typology("empty-typology") == ""  # Empty string, not None
        assert get_dataset_typology("missing-fields") == "geography"
        assert get_dataset_typology("extra-field") == "geography"

    def test_performance_with_large_dataset_simulation(self):
        """Test performance with simulated large dataset."""
        # Create a large mock dataset
        large_datasets = []
        for i in range(1000):
            large_datasets.append(
                {
                    "dataset": f"dataset-{i:04d}",
                    "typology": f"typology-{i % 10}",  # 10 different typologies
                    "name": f"Dataset {i:04d}",
                }
            )

        with patch(
            "jobs.utils.s3_dataset_typology.load_datasets", return_value=large_datasets
        ):
            # Test finding dataset at the beginning
            result1 = get_dataset_typology("dataset-0000")
            assert result1 == "typology-0"

            # Test finding dataset in the middle
            result2 = get_dataset_typology("dataset-0500")
            assert result2 == "typology-0"

            # Test finding dataset at the end
            result3 = get_dataset_typology("dataset-0999")
            assert result3 == "typology-9"

            # Test non-existent dataset
            result4 = get_dataset_typology("dataset-9999")
            assert result4 is None

    @patch("urllib.request.urlopen")
    def test_csv_parsing_edge_cases(self, mock_urlopen):
        """Test CSV parsing with various edge cases."""
        csv_content = '''dataset,typology,name
normal-dataset,geography,Normal Dataset
"quoted-dataset",heritage,"Quoted Dataset"
dataset-with-newline,geography,"Dataset with
newline in name"
dataset-with-tabs,geography,"Dataset	with	tabs"
dataset-with-semicolon,geography,"Dataset; with semicolon"'''

        mock_response = Mock()
        mock_response.readlines.return_value = [
            line.encode("utf-8") for line in csv_content.split("\n")
        ]
        mock_urlopen.return_value.__enter__.return_value = mock_response

        datasets = load_datasets()

        # Verify all datasets are parsed correctly
        dataset_names = [d["dataset"] for d in datasets]
        assert "normal-dataset" in dataset_names
        assert "quoted-dataset" in dataset_names
        assert "dataset-with-newline" in dataset_names
        assert "dataset-with-tabs" in dataset_names
        assert "dataset-with-semicolon" in dataset_names
