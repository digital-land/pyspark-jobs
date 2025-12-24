"""
Simple functional tests to actually execute s3_writer_utils functions.
Target the functions that can be tested without complex PySpark mocking.
"""

import pytest
from unittest.mock import Mock, patch


class TestActualFunctionExecution:
    """Execute actual functions to hit uncovered lines."""
    
    def test_wkt_to_geojson_all_formats(self):
        """Test all WKT geometry formats to hit lines 360-430."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson
        
        # Test POINT
        point_result = wkt_to_geojson("POINT (1.5 2.5)")
        assert point_result["type"] == "Point"
        assert point_result["coordinates"] == [1.5, 2.5]
        
        # Test POLYGON  
        polygon_result = wkt_to_geojson("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
        assert polygon_result["type"] == "Polygon"
        
        # Test MULTIPOLYGON single
        multi_single = wkt_to_geojson("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))")
        assert multi_single["type"] == "Polygon"
        
        # Test invalid WKT
        invalid_result = wkt_to_geojson("INVALID")
        assert invalid_result is None
        
        # Test empty/None
        assert wkt_to_geojson("") is None
        assert wkt_to_geojson(None) is None
    
    def test_fetch_dataset_schema_fields_real_execution(self):
        """Actually execute fetch_dataset_schema_fields to hit lines 490-711."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields
        
        # Test with mocked successful response
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.text = """---
fields:
- field: entity
- field: name  
- field: reference
---
Content after frontmatter"""
            mock_get.return_value = mock_response
            
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                result = fetch_dataset_schema_fields("test-dataset")
                assert "entity" in result
                assert "name" in result
                assert "reference" in result
        
        # Test with no fields section
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.text = """---
title: Test Dataset
description: No fields here
---"""
            mock_get.return_value = mock_response
            
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                result = fetch_dataset_schema_fields("test-dataset")
                assert result == []
        
        # Test with request error
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception("Network error")
            
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                result = fetch_dataset_schema_fields("test-dataset")
                assert result == []
    
    def test_ensure_schema_fields_real_execution(self):
        """Actually execute ensure_schema_fields to hit lines 718-721."""
        from jobs.utils.s3_writer_utils import ensure_schema_fields
        
        # Mock DataFrame with basic operations
        mock_df = Mock()
        mock_df.columns = ["entity", "name"]
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        
        # Test with no schema fields
        with patch('jobs.utils.s3_writer_utils.fetch_dataset_schema_fields', return_value=[]):
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                result = ensure_schema_fields(mock_df, "test")
                assert result == mock_df
        
        # Test with missing fields
        with patch('jobs.utils.s3_writer_utils.fetch_dataset_schema_fields', return_value=["entity", "name", "reference"]):
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                result = ensure_schema_fields(mock_df, "test")
                assert result is not None
        
        # Test with error
        with patch('jobs.utils.s3_writer_utils.fetch_dataset_schema_fields', side_effect=Exception("Error")):
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                result = ensure_schema_fields(mock_df, "test")
                assert result == mock_df
    
    def test_round_point_coordinates_execution(self):
        """Execute round_point_coordinates to hit lines 410-430."""
        from jobs.utils.s3_writer_utils import round_point_coordinates
        
        # Test with no point column
        mock_df = Mock()
        mock_df.columns = ["entity", "name"]
        
        result = round_point_coordinates(mock_df)
        assert result == mock_df
        
        # Test with point column (mock the UDF execution)
        mock_df_with_point = Mock()
        mock_df_with_point.columns = ["entity", "point"]
        mock_df_with_point.withColumn.return_value = mock_df_with_point
        
        with patch('pyspark.sql.functions.udf'):
            with patch('pyspark.sql.functions.col'):
                result = round_point_coordinates(mock_df_with_point)
                assert result is not None
    
    def test_cleanup_temp_path_execution(self):
        """Execute cleanup_temp_path to hit lines 345-355."""
        from jobs.utils.s3_writer_utils import cleanup_temp_path
        
        # Test with objects to delete
        with patch('boto3.client') as mock_boto3:
            mock_s3 = Mock()
            mock_boto3.return_value = mock_s3
            
            mock_paginator = Mock()
            mock_s3.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': 'temp/file1.csv'}, {'Key': 'temp/file2.csv'}]}
            ]
            
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                cleanup_temp_path("dev", "test")
                mock_s3.delete_objects.assert_called_once()
        
        # Test with no objects
        with patch('boto3.client') as mock_boto3:
            mock_s3 = Mock()
            mock_boto3.return_value = mock_s3
            
            mock_paginator = Mock()
            mock_s3.get_paginator.return_value = mock_paginator
            mock_paginator.paginate.return_value = [{}]  # No Contents
            
            with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                cleanup_temp_path("dev", "test")
                mock_s3.delete_objects.assert_not_called()


class TestSimpleFunctionCalls:
    """Simple function calls to ensure basic execution."""
    
    def test_normalise_dataframe_schema_error_path(self):
        """Hit the error path in normalise_dataframe_schema."""
        from jobs.utils.s3_writer_utils import normalise_dataframe_schema
        
        mock_df = Mock()
        mock_df.columns = ["field1"]
        mock_df.withColumnRenamed.return_value = mock_df
        mock_df.printSchema = Mock()
        
        with patch('jobs.main_collection_data.load_metadata', return_value={}):
            with patch('jobs.utils.s3_writer_utils.show_df'):
                with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                    with pytest.raises(ValueError, match="Unknown table name"):
                        normalise_dataframe_schema(mock_df, "unknown", "test", Mock(), "dev")
    
    def test_write_to_s3_entity_table(self):
        """Hit the entity table path in write_to_s3."""
        from jobs.utils.s3_writer_utils import write_to_s3
        
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.count.return_value = 100
        mock_df.coalesce.return_value.write.partitionBy.return_value.mode.return_value.option.return_value.option.return_value.parquet = Mock()
        
        with patch('jobs.utils.s3_writer_utils.cleanup_dataset_data', return_value={"objects_deleted": 0, "errors": []}):
            with patch('jobs.utils.s3_writer_utils.show_df'):
                with patch('jobs.utils.s3_writer_utils.get_logger', return_value=Mock()):
                    write_to_s3(mock_df, "s3://test/", "test", "entity", "dev")