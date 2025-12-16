"""Unit tests for transform_collection_data module."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from jobs.transform_collection_data import (
    transform_data_fact, transform_data_fact_res,
    transform_data_issue, transform_data_entity
)


class TestTransformCollectionData:
    """Test suite for transform_collection_data module."""

    def test_transform_data_fact_success(self, spark, sample_fact_data):
        """Test successful fact data transformation."""
        result = transform_data_fact(sample_fact_data)
        
        assert result is not None
        assert result.count() > 0
        
        # Check that duplicate facts are removed (keeping highest priority)
        fact1_rows = result.filter(result.fact == "fact1").collect()
        assert len(fact1_rows) == 1
        assert fact1_rows[0].priority == 2  # Higher priority should be kept
        
        # Verify column order
        expected_columns = ["end_date", "entity", "fact", "field", "entry_date", 
                          "priority", "reference_entity", "start_date", "value"]
        assert result.columns == expected_columns

    def test_transform_data_fact_empty_dataframe(self, spark):
        """Test fact transformation with empty DataFrame."""
        schema = StructType([
            StructField("fact", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("entry_date", StringType(), True),
            StructField("entry_number", StringType(), True)
        ])
        empty_df = spark.createDataFrame([], schema)
        
        result = transform_data_fact(empty_df)
        
        assert result is not None
        assert result.count() == 0

    def test_transform_data_fact_missing_columns(self, spark):
        """Test fact transformation with missing required columns."""
        schema = StructType([
            StructField("fact", StringType(), True),
            StructField("entity", StringType(), True)
        ])
        df = spark.createDataFrame([("fact1", "entity1")], schema)
        
        with pytest.raises(Exception):
            transform_data_fact(df)

    def test_transform_data_fact_res_success(self, spark):
        """Test successful fact resource transformation."""
        schema = StructType([
            StructField("end_date", StringType(), True),
            StructField("fact", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("resource", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("extra_column", StringType(), True)
        ])
        
        data = [
            ("2023-12-31", "fact1", "2023-01-01", "1", 1, "resource1", "2023-01-01", "extra"),
            ("2023-12-31", "fact2", "2023-01-02", "2", 2, "resource2", "2023-01-01", "extra2")
        ]
        df = spark.createDataFrame(data, schema)
        
        result = transform_data_fact_res(df)
        
        assert result is not None
        assert result.count() == 2
        
        # Verify only required columns are selected
        expected_columns = ["end_date", "fact", "entry_date", "entry_number", 
                          "priority", "resource", "start_date"]
        assert result.columns == expected_columns

    def test_transform_data_fact_res_empty(self, spark):
        """Test fact resource transformation with empty DataFrame."""
        schema = StructType([
            StructField("end_date", StringType(), True),
            StructField("fact", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("resource", StringType(), True),
            StructField("start_date", StringType(), True)
        ])
        empty_df = spark.createDataFrame([], schema)
        
        result = transform_data_fact_res(empty_df)
        
        assert result is not None
        assert result.count() == 0

    def test_transform_data_issue_success(self, spark, sample_issue_data):
        """Test successful issue data transformation."""
        result = transform_data_issue(sample_issue_data)
        
        assert result is not None
        assert result.count() == 2
        
        # Verify that date columns are added with empty strings
        collected = result.collect()
        for row in collected:
            assert row.start_date == ""
            assert row.entry_date == ""
            assert row.end_date == ""
        
        # Verify column order
        expected_columns = ["end_date", "entity", "entry_date", "entry_number", 
                          "field", "issue_type", "line_number", "dataset", 
                          "resource", "start_date", "value", "message"]
        assert result.columns == expected_columns

    def test_transform_data_issue_empty(self, spark):
        """Test issue transformation with empty DataFrame."""
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("field", StringType(), True),
            StructField("issue_type", StringType(), True),
            StructField("line_number", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("resource", StringType(), True),
            StructField("value", StringType(), True),
            StructField("message", StringType(), True)
        ])
        empty_df = spark.createDataFrame([], schema)
        
        result = transform_data_issue(empty_df)
        
        assert result is not None
        assert result.count() == 0

    @patch('jobs.transform_collection_data.get_dataset_typology')
    def test_transform_data_entity_success(self, mock_typology, spark, sample_entity_data):
        """Test successful entity data transformation."""
        mock_typology.return_value = "test-typology"
        
        # Mock organisation data
        org_schema = StructType([
            StructField("organisation", StringType(), True),
            StructField("entity", StringType(), True)
        ])
        org_data = [("org1", "org_entity1")]
        org_df = spark.createDataFrame(org_data, org_schema)
        
        with patch.object(spark.read, 'csv') as mock_csv:
            mock_csv.return_value = org_df
            
            result = transform_data_entity(sample_entity_data, "test-dataset", spark, "development")
        
        assert result is not None
        assert result.count() > 0
        
        # Verify typology column is added
        collected = result.collect()
        for row in collected:
            assert row.typology == "test-typology"
        
        # Verify dataset column is added
        for row in collected:
            assert row.dataset == "test-dataset"

    @patch('jobs.transform_collection_data.get_dataset_typology')
    def test_transform_data_entity_with_priority(self, mock_typology, spark):
        """Test entity transformation with priority-based ranking."""
        mock_typology.return_value = "test-typology"
        
        # Create data with multiple entries for same entity/field with different priorities
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("priority", IntegerType(), True)
        ])
        
        data = [
            ("entity1", "name", "Old Name", "1", "2023-01-01", "2023-01-01", "", 1),
            ("entity1", "name", "New Name", "2", "2023-01-02", "2023-01-01", "", 2),  # Higher priority
            ("entity1", "reference", "REF001", "3", "2023-01-01", "2023-01-01", "", 1)
        ]
        df = spark.createDataFrame(data, schema)
        
        # Mock organisation data
        org_schema = StructType([
            StructField("organisation", StringType(), True),
            StructField("entity", StringType(), True)
        ])
        org_data = []
        org_df = spark.createDataFrame(org_data, org_schema)
        
        with patch.object(spark.read, 'csv') as mock_csv:
            mock_csv.return_value = org_df
            
            result = transform_data_entity(df, "test-dataset", spark, "development")
        
        # Should keep the higher priority value
        collected = result.collect()
        entity1_row = [row for row in collected if row.entity == "entity1"][0]
        assert entity1_row.name == "New Name"  # Higher priority value should be kept

    @patch('jobs.transform_collection_data.get_dataset_typology')
    def test_transform_data_entity_without_priority(self, mock_typology, spark):
        """Test entity transformation without priority column."""
        mock_typology.return_value = "test-typology"
        
        # Create data without priority column
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])
        
        data = [
            ("entity1", "name", "Test Name", "1", "2023-01-01", "2023-01-01", ""),
            ("entity1", "reference", "REF001", "2", "2023-01-02", "2023-01-01", "")  # Later date
        ]
        df = spark.createDataFrame(data, schema)
        
        # Mock organisation data
        org_schema = StructType([
            StructField("organisation", StringType(), True),
            StructField("entity", StringType(), True)
        ])
        org_data = []
        org_df = spark.createDataFrame(org_data, org_schema)
        
        with patch.object(spark.read, 'csv') as mock_csv:
            mock_csv.return_value = org_df
            
            result = transform_data_entity(df, "test-dataset", spark, "development")
        
        assert result is not None
        assert result.count() == 1

    @patch('jobs.transform_collection_data.get_dataset_typology')
    def test_transform_data_entity_column_normalization(self, mock_typology, spark):
        """Test entity transformation with kebab-case to snake_case conversion."""
        mock_typology.return_value = "test-typology"
        
        # Create data with kebab-case column names
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("priority", IntegerType(), True)
        ])
        
        data = [
            ("entity1", "test-field", "test-value", "1", "2023-01-01", "2023-01-01", "", 1)
        ]
        df = spark.createDataFrame(data, schema)
        
        # Mock organisation data
        org_schema = StructType([
            StructField("organisation", StringType(), True),
            StructField("entity", StringType(), True)
        ])
        org_data = []
        org_df = spark.createDataFrame(org_data, org_schema)
        
        with patch.object(spark.read, 'csv') as mock_csv:
            mock_csv.return_value = org_df
            
            result = transform_data_entity(df, "test-dataset", spark, "development")
        
        # Check that kebab-case fields are converted to snake_case in pivoted columns
        collected = result.collect()
        entity1_row = collected[0]
        
        # The field "test-field" should become column "test_field" after pivoting
        assert hasattr(entity1_row, 'test_field') or 'test_field' in result.columns

    @patch('jobs.transform_collection_data.get_dataset_typology')
    def test_transform_data_entity_json_creation(self, mock_typology, spark):
        """Test entity transformation with JSON creation for non-standard columns."""
        mock_typology.return_value = "test-typology"
        
        # Create data with extra fields that should go into JSON
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("priority", IntegerType(), True)
        ])
        
        data = [
            ("entity1", "custom_field", "custom_value", "1", "2023-01-01", "2023-01-01", "", 1),
            ("entity1", "another_field", "another_value", "2", "2023-01-01", "2023-01-01", "", 1)
        ]
        df = spark.createDataFrame(data, schema)
        
        # Mock organisation data
        org_schema = StructType([
            StructField("organisation", StringType(), True),
            StructField("entity", StringType(), True)
        ])
        org_data = []
        org_df = spark.createDataFrame(org_data, org_schema)
        
        with patch.object(spark.read, 'csv') as mock_csv:
            mock_csv.return_value = org_df
            
            result = transform_data_entity(df, "test-dataset", spark, "development")
        
        # Verify JSON column is created
        assert "json" in result.columns
        collected = result.collect()
        entity1_row = collected[0]
        
        # JSON should contain the non-standard fields
        import json
        json_data = json.loads(entity1_row.json)
        assert "custom_field" in json_data or "another_field" in json_data

    def test_transform_data_fact_exception_handling(self, spark):
        """Test exception handling in transform_data_fact."""
        # Create invalid DataFrame that will cause an error
        invalid_df = Mock()
        invalid_df.withColumn.side_effect = Exception("Test error")
        
        with pytest.raises(Exception):
            transform_data_fact(invalid_df)

    def test_transform_data_fact_res_exception_handling(self, spark):
        """Test exception handling in transform_data_fact_res."""
        invalid_df = Mock()
        invalid_df.select.side_effect = Exception("Test error")
        
        with pytest.raises(Exception):
            transform_data_fact_res(invalid_df)

    def test_transform_data_issue_exception_handling(self, spark):
        """Test exception handling in transform_data_issue."""
        invalid_df = Mock()
        invalid_df.withColumn.side_effect = Exception("Test error")
        
        with pytest.raises(Exception):
            transform_data_issue(invalid_df)

    @patch('jobs.transform_collection_data.get_dataset_typology')
    def test_transform_data_entity_exception_handling(self, mock_typology, spark):
        """Test exception handling in transform_data_entity."""
        mock_typology.return_value = "test-typology"
        
        invalid_df = Mock()
        invalid_df.columns = ["entity", "field"]
        invalid_df.withColumn.side_effect = Exception("Test error")
        
        with pytest.raises(Exception):
            transform_data_entity(invalid_df, "test-dataset", spark, "development")


@pytest.mark.unit
class TestTransformCollectionDataIntegration:
    """Integration-style tests for transform_collection_data module."""

    @patch('jobs.transform_collection_data.get_dataset_typology')
    def test_full_entity_transformation_workflow(self, mock_typology, spark):
        """Test complete entity transformation workflow."""
        mock_typology.return_value = "test-typology"
        
        # Create comprehensive test data
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("priority", IntegerType(), True),
            StructField("organisation", StringType(), True)
        ])
        
        data = [
            ("entity1", "name", "Entity One", "1", "2023-01-01", "2023-01-01", "", 1, "org1"),
            ("entity1", "reference", "REF001", "2", "2023-01-01", "2023-01-01", "", 1, "org1"),
            ("entity2", "name", "Entity Two", "3", "2023-01-01", "2023-01-01", "", 1, "org2")
        ]
        df = spark.createDataFrame(data, schema)
        
        # Mock organisation data
        org_schema = StructType([
            StructField("organisation", StringType(), True),
            StructField("entity", StringType(), True)
        ])
        org_data = [
            ("org1", "org_entity1"),
            ("org2", "org_entity2")
        ]
        org_df = spark.createDataFrame(org_data, org_schema)
        
        with patch.object(spark.read, 'csv') as mock_csv:
            mock_csv.return_value = org_df
            
            result = transform_data_entity(df, "test-dataset", spark, "development")
        
        # Verify complete transformation
        assert result is not None
        assert result.count() == 2
        
        # Check all expected columns are present
        expected_columns = [
            "dataset", "end_date", "entity", "entry_date", "geometry", "json",
            "name", "organisation_entity", "point", "prefix", "reference",
            "start_date", "typology"
        ]
        for col in expected_columns:
            assert col in result.columns
        
        # Verify data integrity
        collected = result.collect()
        entity1_row = [row for row in collected if row.entity == "entity1"][0]
        assert entity1_row.name == "Entity One"
        assert entity1_row.reference == "REF001"
        assert entity1_row.dataset == "test-dataset"
        assert entity1_row.typology == "test-typology"