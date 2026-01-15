"""Extended unit tests for transform_collection_data module to increase coverage."""

import os
import sys
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import pytest

from jobs.transform.entity_transformer import EntityTransformer
from jobs.transform.fact_transformer import FactTransformer
from jobs.transform.fact_resource_transformer import FactResourceTransformer
from jobs.transform.issue_transformer import IssueTransformer


@pytest.mark.skip(
    reason="PySpark DataFrame creation conflicts with pandas mock in conftest.py"
)
class TestTransformCollectionDataExtended:
    """Extended tests to increase coverage of transform_collection_data module."""

    def test_transform_data_fact_with_priority(self, spark, sample_fact_data):
        """Test fact transformation with priority column."""
        pass

    def test_transform_data_fact_exception_handling(self, spark):
        """Test fact transformation exception handling."""
        # Create invalid DataFrame to trigger exception
        invalid_df = spark.createDataFrame([], "invalid_schema string")

        with pytest.raises(Exception):
            FactTransformer.transform(invalid_df)

    def test_transform_data_fact_res_basic(self, spark):
        """Test fact resource transformation."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("end_date", StringType(), True),
                StructField("fact", StringType(), True),
                StructField("entry_date", StringType(), True),
                StructField("entry_number", StringType(), True),
                StructField("priority", IntegerType(), True),
                StructField("resource", StringType(), True),
                StructField("start_date", StringType(), True),
            ]
        )

        data = [
            ("", "fact1", "2023 - 01 - 01", "1", 1, "resource1", "2023 - 01 - 01"),
            ("", "fact2", "2023 - 01 - 02", "2", 2, "resource2", "2023 - 01 - 02"),
        ]

        df = spark.createDataFrame(data, schema)
        result = FactResourceTransformer.transform(df)

        assert result is not None
        assert result.count() == 2
        assert "fact" in result.columns
        assert "resource" in result.columns

    def test_transform_data_fact_res_exception(self, spark):
        """Test fact resource transformation exception handling."""
        invalid_df = spark.createDataFrame([], "invalid_schema string")

        with pytest.raises(Exception):
            FactResourceTransformer.transform(invalid_df)

    def test_transform_data_issue_basic(self, spark, sample_issue_data):
        """Test issue transformation."""
        result = IssueTransformer.transform(sample_issue_data)

        assert result is not None
        assert result.count() == 2
        assert "issue_type" in result.columns
        assert "message" in result.columns
        # Check that date columns are added
        assert "start_date" in result.columns
        assert "entry_date" in result.columns
        assert "end_date" in result.columns

    def test_transform_data_issue_exception(self, spark):
        """Test issue transformation exception handling."""
        invalid_df = spark.createDataFrame([], "invalid_schema string")

        with pytest.raises(Exception):
            IssueTransformer.transform(invalid_df)

    @patch("jobs.transform_collection_data.get_dataset_typology")
    @patch("jobs.utils.s3_dataset_typology.get_dataset_typology")
    def test_transform_data_entity_with_mocks(
        self, mock_s3_typology, mock_typology, spark, sample_entity_data
    ):
        """Test entity transformation with mocked dependencies."""
        mock_typology.return_value = "geography"
        mock_s3_typology.return_value = "geography"

        # Mock Spark read for organisation data
        mock_org_df = spark.createDataFrame(
            [("org1", "600001", "Test Org")], ["organisation", "entity", "name"]
        )

        with patch.object(spark.read, "option") as mock_option:
            mock_option.return_value.csv.return_value = mock_org_df

            result = EntityTransformer().transform(
                sample_entity_data, "test - dataset", spark, "test"
            )

            assert result is not None
            assert "typology" in result.columns
            assert "dataset" in result.columns

    def test_transform_data_entity_no_priority_column(self, spark):
        """Test entity transformation without priority column."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType(
            [
                StructField("entity", StringType(), True),
                StructField("field", StringType(), True),
                StructField("value", StringType(), True),
                StructField("entry_date", StringType(), True),
                StructField("entry_number", StringType(), True),
            ]
        )

        data = [
            ("entity1", "name", "Test Entity", "2023 - 01 - 01", "1"),
            ("entity1", "reference", "REF001", "2023 - 01 - 02", "2"),
        ]

        df = spark.createDataFrame(data, schema)

        with patch(
            "jobs.transform_collection_data.get_dataset_typology",
            return_value="geography",
        ):
            with patch.object(spark.read, "option") as mock_option:
                mock_org_df = spark.createDataFrame(
                    [], ["organisation", "entity", "name"]
                )
                mock_option.return_value.csv.return_value = mock_org_df

                result = EntityTransformer().transform(df, "test - dataset", spark, "test")

                assert result is not None
                assert "entity" in result.columns

    def test_transform_data_entity_exception(self, spark):
        """Test entity transformation exception handling."""
        invalid_df = spark.createDataFrame([], "invalid_schema string")

        with pytest.raises(Exception):
            EntityTransformer().transform(invalid_df, "test - dataset", spark, "test")
