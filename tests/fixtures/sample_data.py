"""
Sample data generators for testing PySpark collection data jobs.

This module provides realistic test data that matches the expected schemas
for collection data processing.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta
import pandas as pd


class SampleDataGenerator:
    """Generate sample data for testing collection data jobs."""
    
    @staticmethod
    def get_fact_schema():
        """Get the schema for fact table data."""
        return StructType([
            StructField("fact", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("priority", StringType(), True),
            StructField("reference_entity", StringType(), True),
            StructField("resource", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("value", StringType(), True)
        ])
    
    @staticmethod
    def get_fact_res_schema():
        """Get the schema for fact resource table data."""
        return StructType([
            StructField("end_date", StringType(), True),
            StructField("fact", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("priority", StringType(), True),
            StructField("resource", StringType(), True),
            StructField("start_date", StringType(), True)
        ])
    
    @staticmethod
    def get_entity_schema():
        """Get the schema for entity table data."""
        return StructType([
            StructField("entity", StringType(), True),
            StructField("field", StringType(), True),
            StructField("value", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("entry_date", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True)
        ])
    
    @staticmethod
    def get_issue_schema():
        """Get the schema for issue table data."""
        return StructType([
            StructField("issue", StringType(), True),
            StructField("resource", StringType(), True),
            StructField("lineNumber", StringType(), True),
            StructField("entry_number", StringType(), True),
            StructField("field", StringType(), True),
            StructField("issue_type", StringType(), True),
            StructField("value", StringType(), True),
            StructField("message", StringType(), True)
        ])
    
    @classmethod
    def generate_fact_data(cls, num_records=10):
        """Generate sample fact data."""
        base_date = datetime(2024, 1, 1)
        data = []
        
        for i in range(num_records):
            entry_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            start_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            end_date = (base_date + timedelta(days=i+30)).strftime("%Y-%m-%d")
            
            data.append({
                "fact": f"fact_{i:03d}",
                "end_date": end_date if i % 3 != 0 else "",
                "entity": f"entity_{i:03d}",
                "field": "test_field",
                "entry_date": entry_date,
                "entry_number": str(i + 1000),
                "priority": str(i % 5 + 1),
                "reference_entity": f"ref_entity_{i:03d}" if i % 2 == 0 else "",
                "resource": f"resource_{i:03d}",
                "start_date": start_date,
                "value": f"test_value_{i}"
            })
        
        return data
    
    @classmethod
    def generate_fact_res_data(cls, num_records=10):
        """Generate sample fact resource data."""
        base_date = datetime(2024, 1, 1)
        data = []
        
        for i in range(num_records):
            entry_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            start_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            end_date = (base_date + timedelta(days=i+30)).strftime("%Y-%m-%d")
            
            data.append({
                "end_date": end_date if i % 3 != 0 else "",
                "fact": f"fact_{i:03d}",
                "entry_date": entry_date,
                "entry_number": str(i + 1000),
                "priority": str(i % 5 + 1),
                "resource": f"resource_{i:03d}",
                "start_date": start_date
            })
        
        return data
    
    @classmethod
    def generate_entity_data(cls, num_records=10):
        """Generate sample entity data."""
        base_date = datetime(2024, 1, 1)
        data = []
        
        for i in range(num_records):
            entry_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            start_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            end_date = (base_date + timedelta(days=i+30)).strftime("%Y-%m-%d")
            
            data.append({
                "entity": f"entity_{i:03d}",
                "field": "test_field",
                "value": f"test_value_{i}",
                "entry_number": str(i + 1000),
                "entry_date": entry_date,
                "start_date": start_date,
                "end_date": end_date if i % 3 != 0 else ""
            })
        
        return data
    
    @classmethod
    def generate_issue_data(cls, num_records=5):
        """Generate sample issue data."""
        data = []
        
        for i in range(num_records):
            data.append({
                "issue": f"issue_{i:03d}",
                "resource": f"resource_{i:03d}",
                "lineNumber": str(i + 1),
                "entry_number": str(i + 1000),
                "field": "test_field",
                "issue_type": "validation_error" if i % 2 == 0 else "format_error",
                "value": f"invalid_value_{i}",
                "message": f"Test issue message {i}"
            })
        
        return data
    
    @classmethod
    def generate_transport_access_node_data(cls, num_records=10):
        """Generate sample transport access node data (main use case)."""
        base_date = datetime(2024, 1, 1)
        data = []
        
        transport_fields = [
            "geometry", "name", "point", "reference", "organisation",
            "entry-date", "start-date", "end-date"
        ]
        
        for i in range(num_records):
            entry_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            start_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            end_date = (base_date + timedelta(days=i+365)).strftime("%Y-%m-%d")
            
            # Generate multiple rows per entity (one for each field)
            for j, field in enumerate(transport_fields):
                if field == "geometry":
                    value = f"POINT({-0.1 + i*0.01} {51.5 + i*0.01})"
                elif field == "name":
                    value = f"Transport Node {i:03d}"
                elif field == "point":
                    value = f"POINT({-0.1 + i*0.01} {51.5 + i*0.01})"
                elif field == "reference":
                    value = f"TAN{i:06d}"
                elif field == "organisation":
                    value = "development:test-organisation"
                elif field == "entry-date":
                    value = entry_date
                elif field == "start-date":
                    value = start_date
                elif field == "end-date":
                    value = end_date if i % 4 != 0 else ""
                else:
                    value = f"value_{i}_{j}"
                
                data.append({
                    "entry_number": str(i + 1000),
                    "entity": f"transport-access-node:{i:06d}",
                    "field": field,
                    "value": value,
                    "entry_date": entry_date,
                    "start_date": start_date,
                    "end_date": end_date if field != "end-date" and i % 4 != 0 else "",
                    "organisation": "development:test-organisation"
                })
        
        return data


@pytest.fixture(scope="session")
def sample_data_generator():
    """Provide the sample data generator."""
    return SampleDataGenerator


@pytest.fixture
def sample_fact_data(sample_data_generator):
    """Generate sample fact data."""
    return sample_data_generator.generate_fact_data()


@pytest.fixture
def sample_fact_res_data(sample_data_generator):
    """Generate sample fact resource data."""
    return sample_data_generator.generate_fact_res_data()


@pytest.fixture
def sample_entity_data(sample_data_generator):
    """Generate sample entity data."""
    return sample_data_generator.generate_entity_data()


@pytest.fixture
def sample_issue_data(sample_data_generator):
    """Generate sample issue data."""
    return sample_data_generator.generate_issue_data()


@pytest.fixture
def sample_transport_data(sample_data_generator):
    """Generate sample transport access node data."""
    return sample_data_generator.generate_transport_access_node_data()


@pytest.fixture
def sample_fact_dataframe(spark, sample_fact_data, sample_data_generator):
    """Create a sample fact DataFrame."""
    schema = sample_data_generator.get_fact_schema()
    return spark.createDataFrame(sample_fact_data, schema)


@pytest.fixture
def sample_fact_res_dataframe(spark, sample_fact_res_data, sample_data_generator):
    """Create a sample fact resource DataFrame."""
    schema = sample_data_generator.get_fact_res_schema()
    return spark.createDataFrame(sample_fact_res_data, schema)


@pytest.fixture
def sample_entity_dataframe(spark, sample_entity_data, sample_data_generator):
    """Create a sample entity DataFrame."""
    schema = sample_data_generator.get_entity_schema()
    return spark.createDataFrame(sample_entity_data, schema)


@pytest.fixture
def sample_issue_dataframe(spark, sample_issue_data, sample_data_generator):
    """Create a sample issue DataFrame."""
    schema = sample_data_generator.get_issue_schema()
    return spark.createDataFrame(sample_issue_data, schema)


@pytest.fixture
def sample_transport_dataframe(spark, sample_transport_data, sample_data_generator):
    """Create a sample transport access node DataFrame."""
    schema = sample_data_generator.get_entity_schema()
    return spark.createDataFrame(sample_transport_data, schema)
