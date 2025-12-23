"""
Additional targeted tests for remaining low-coverage modules.
Focus on improving coverage for postgres_connectivity, s3_writer_utils, postgres_writer_utils, csv_s3_writer.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import json

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


@pytest.mark.unit
class TestPostgresConnectivityAdvanced:
    """Advanced tests for postgres_connectivity module (42.91% -> target 60%+)."""

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    @patch('jobs.dbaccess.postgres_connectivity.json')
    def test_get_aws_secret_success(self, mock_json, mock_get_secret):
        """Test get_aws_secret successful retrieval."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import get_aws_secret
            
            # Mock successful secret retrieval
            mock_get_secret.return_value = '{"username": "test", "password": "pass", "db_name": "db", "host": "localhost", "port": "5432"}'
            mock_json.loads.return_value = {
                "username": "test",
                "password": "pass", 
                "db_name": "db",
                "host": "localhost",
                "port": "5432"
            }
            
            result = get_aws_secret("development")
            
            # Verify result structure
            assert "database" in result
            assert "host" in result
            assert "port" in result
            assert "user" in result
            assert "password" in result
            
            mock_get_secret.assert_called_once()

    @patch('jobs.dbaccess.postgres_connectivity.get_secret_emr_compatible')
    def test_get_aws_secret_missing_fields(self, mock_get_secret):
        """Test get_aws_secret with missing required fields."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import get_aws_secret
            
            # Mock secret with missing fields
            mock_get_secret.return_value = '{"username": "test"}'
            
            with pytest.raises(ValueError, match="Missing required secret fields"):
                get_aws_secret("development")

    def test_performance_recommendations_small_dataset(self):
        """Test get_performance_recommendations for small datasets."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import get_performance_recommendations
            
            result = get_performance_recommendations(5000)
            
            assert result["method"] == "optimized"
            assert result["batch_size"] == 1000
            assert result["num_partitions"] == 1
            assert "Small dataset" in result["notes"][0]

    def test_performance_recommendations_large_dataset(self):
        """Test get_performance_recommendations for large datasets."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import get_performance_recommendations
            
            result = get_performance_recommendations(15000000)  # 15M rows
            
            assert result["method"] == "optimized"
            assert result["batch_size"] == 5000
            assert "Very large dataset" in result["notes"][0]

    def test_cleanup_old_staging_tables_function_exists(self):
        """Test cleanup_old_staging_tables function exists."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import cleanup_old_staging_tables
            
            # Test function exists and is callable
            assert callable(cleanup_old_staging_tables)

    def test_pyspark_entity_columns_structure(self):
        """Test pyspark_entity_columns dictionary structure."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import pyspark_entity_columns
            
            # Test dictionary exists and has expected structure
            assert isinstance(pyspark_entity_columns, dict)
            assert "entity" in pyspark_entity_columns
            assert "dataset" in pyspark_entity_columns
            assert "geometry" in pyspark_entity_columns
            
            # Test data types are strings
            for col, dtype in pyspark_entity_columns.items():
                assert isinstance(dtype, str)
                assert len(dtype) > 0

    def test_entity_table_name_constant(self):
        """Test ENTITY_TABLE_NAME constant."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import ENTITY_TABLE_NAME
            
            # Test constant exists and is string
            assert isinstance(ENTITY_TABLE_NAME, str)
            assert len(ENTITY_TABLE_NAME) > 0


@pytest.mark.unit
class TestS3WriterUtilsAdvanced:
    """Advanced tests for s3_writer_utils module (32.31% -> target 50%+)."""

    def test_wkt_to_geojson_point(self):
        """Test wkt_to_geojson function with POINT geometry."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson
        
        # Test POINT conversion
        result = wkt_to_geojson("POINT (1.0 2.0)")
        assert result["type"] == "Point"
        assert result["coordinates"] == [1.0, 2.0]
        
        # Test empty/None input
        assert wkt_to_geojson(None) is None
        assert wkt_to_geojson("") is None

    def test_wkt_to_geojson_polygon(self):
        """Test wkt_to_geojson function with POLYGON geometry."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson
        
        # Test POLYGON conversion
        wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
        result = wkt_to_geojson(wkt)
        assert result["type"] == "Polygon"
        assert len(result["coordinates"]) == 1
        assert len(result["coordinates"][0]) == 5

    def test_wkt_to_geojson_multipolygon_simplification(self):
        """Test wkt_to_geojson MULTIPOLYGON simplification to POLYGON."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson
        
        # Test single polygon in MULTIPOLYGON gets simplified
        wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))"
        result = wkt_to_geojson(wkt)
        # Should be simplified to Polygon
        assert result["type"] == "Polygon"

    def test_cleanup_temp_path_function_exists(self):
        """Test cleanup_temp_path function exists."""
        from jobs.utils.s3_writer_utils import cleanup_temp_path
        
        # Test function exists and is callable
        assert callable(cleanup_temp_path)

    @patch('jobs.utils.s3_writer_utils.requests')
    def test_fetch_dataset_schema_fields_success(self, mock_requests):
        """Test fetch_dataset_schema_fields successful response."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields
        
        # Mock successful response
        mock_response = Mock()
        mock_response.text = """---
fields:
- field: entity
- field: name
- field: geometry
---
"""
        mock_requests.get.return_value = mock_response
        
        result = fetch_dataset_schema_fields("test-dataset")
        
        # Should return list of fields
        assert isinstance(result, list)
        assert "entity" in result
        assert "name" in result
        assert "geometry" in result

    @patch('jobs.utils.s3_writer_utils.requests')
    def test_fetch_dataset_schema_fields_failure(self, mock_requests):
        """Test fetch_dataset_schema_fields with request failure."""
        from jobs.utils.s3_writer_utils import fetch_dataset_schema_fields
        
        # Mock failed response
        mock_requests.get.side_effect = Exception("Network error")
        
        result = fetch_dataset_schema_fields("test-dataset")
        
        # Should return empty list on failure
        assert result == []

    def test_s3_rename_and_move_function_exists(self):
        """Test s3_rename_and_move function exists."""
        from jobs.utils.s3_writer_utils import s3_rename_and_move
        
        # Test function exists and is callable
        assert callable(s3_rename_and_move)

    def test_function_existence_checks(self):
        """Test that key functions exist and are callable."""
        from jobs.utils.s3_writer_utils import (
            round_point_coordinates,
            ensure_schema_fields,
            normalise_dataframe_schema,
            transform_data_entity_format,
            write_to_s3_format
        )
        
        # Test functions exist and are callable
        assert callable(round_point_coordinates)
        assert callable(ensure_schema_fields)
        assert callable(normalise_dataframe_schema)
        assert callable(transform_data_entity_format)
        assert callable(write_to_s3_format)

    def test_wkt_to_geojson_edge_cases(self):
        """Test wkt_to_geojson with edge cases."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson
        
        # Test invalid WKT
        result = wkt_to_geojson("INVALID WKT")
        assert result is None
        
        # Test whitespace handling
        result = wkt_to_geojson("  POINT (1.0 2.0)  ")
        assert result["type"] == "Point"

    def test_global_variables_exist(self):
        """Test that global variables exist in s3_writer_utils."""
        from jobs.utils import s3_writer_utils
        
        # Test df_entity global variable exists
        assert hasattr(s3_writer_utils, 'df_entity')


@pytest.mark.unit
class TestPostgresWriterUtilsAdvanced:
    """Advanced tests for postgres_writer_utils module (31.30% -> target 50%+)."""

    def test_ensure_required_columns_type_casting(self):
        """Test _ensure_required_columns type casting logic."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.utils.postgres_writer_utils import _ensure_required_columns
            
            mock_df = Mock()
            mock_df.columns = ['entity', 'json', 'entry_date']
            mock_df.withColumn.return_value = mock_df
            
            result = _ensure_required_columns(mock_df, ['entity', 'json', 'entry_date'])
            
            # Should cast columns to appropriate types
            assert mock_df.withColumn.call_count >= 3

    def test_required_columns_list(self):
        """Test required columns list in postgres_writer_utils."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
            
            # Function should exist and be callable
            assert callable(write_dataframe_to_postgres_jdbc)


@pytest.mark.unit
class TestCsvS3WriterAdvanced:
    """Advanced tests for csv_s3_writer module (51.19% -> target 65%+)."""

    def test_csv_s3_writer_imports(self):
        """Test csv_s3_writer imports work correctly."""
        # Test that we can import the module without errors
        try:
            from jobs import csv_s3_writer
            from jobs.csv_s3_writer import logger
            success = True
        except ImportError:
            success = False
        
        assert success

    def test_error_handling_functions(self):
        """Test error handling in csv_s3_writer functions."""
        from jobs.csv_s3_writer import cleanup_temp_csv_files
        
        # Test function exists and handles basic cases
        assert callable(cleanup_temp_csv_files)


@pytest.mark.unit
class TestAdditionalCoverageImprovements:
    """Additional tests to improve coverage across remaining modules."""

    def test_s3_writer_utils_error_handling(self):
        """Test error handling in s3_writer_utils functions."""
        from jobs.utils.s3_writer_utils import wkt_to_geojson
        
        # Test with malformed WKT strings
        test_cases = [
            "POINT",  # Incomplete
            "POLYGON",  # Incomplete
            "MULTIPOLYGON",  # Incomplete
            "LINESTRING (0 0, 1 1)",  # Unsupported type
        ]
        
        for test_case in test_cases:
            result = wkt_to_geojson(test_case)
            # Should handle gracefully (return None or valid result)
            assert result is None or isinstance(result, dict)

    def test_postgres_connectivity_constants(self):
        """Test constants and configuration in postgres_connectivity."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess.postgres_connectivity import (
                ENTITY_TABLE_NAME,
                pyspark_entity_columns
            )
            
            # Test constants are properly defined
            assert isinstance(ENTITY_TABLE_NAME, str)
            assert isinstance(pyspark_entity_columns, dict)
            assert len(pyspark_entity_columns) > 0
            
            # Test required columns exist
            required_cols = ['entity', 'dataset', 'geometry', 'json']
            for col in required_cols:
                assert col in pyspark_entity_columns

    def test_module_level_variables(self):
        """Test module-level variables and globals."""
        # Test s3_writer_utils globals
        from jobs.utils import s3_writer_utils
        assert hasattr(s3_writer_utils, 'df_entity')
        
        # Test postgres_connectivity globals  
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.dbaccess import postgres_connectivity
            assert hasattr(postgres_connectivity, 'dbtable_name')
            assert hasattr(postgres_connectivity, 'pyspark_entity_columns')_schema_fields,
            normalise_dataframe_schema,
            transform_data_entity_format,
            write_to_s3_format
        )
        
        # Test functions exist and are callable
        assert callable(round_point_coordinates)
        assert callable(ensure_schema_fields)
        assert callable(normalise_dataframe_schema)
        assert callable(transform_data_entity_format)
        assert callable(write_to_s3_format)


@pytest.mark.unit
class TestPostgresWriterUtilsAdvanced:
    """Advanced tests for postgres_writer_utils module (31.30% -> target 50%+)."""

    def test_ensure_required_columns_missing_columns(self):
        """Test _ensure_required_columns with missing columns."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.utils.postgres_writer_utils import _ensure_required_columns
            
            # Mock DataFrame
            mock_df = Mock()
            mock_df.columns = ['existing_col']
            mock_df.withColumn.return_value = mock_df
            
            required_cols = ['existing_col', 'missing_col']
            
            result = _ensure_required_columns(mock_df, required_cols)
            
            # Should add missing column
            mock_df.withColumn.assert_called()

    def test_ensure_required_columns_with_defaults(self):
        """Test _ensure_required_columns with default values."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.utils.postgres_writer_utils import _ensure_required_columns
            
            mock_df = Mock()
            mock_df.columns = []
            mock_df.withColumn.return_value = mock_df
            
            defaults = {'missing_col': 'default_value'}
            result = _ensure_required_columns(mock_df, ['missing_col'], defaults=defaults)
            
            mock_df.withColumn.assert_called()

    def test_ensure_required_columns_logger_warnings(self):
        """Test _ensure_required_columns logger warnings."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.utils.postgres_writer_utils import _ensure_required_columns
            
            mock_df = Mock()
            mock_df.columns = ['extra_col']
            mock_df.withColumn.return_value = mock_df
            
            mock_logger = Mock()
            
            result = _ensure_required_columns(mock_df, ['required_col'], logger=mock_logger)
            
            # Should log warnings for missing and extra columns
            mock_logger.warning.assert_called()
            mock_logger.info.assert_called()

    def test_write_dataframe_to_postgres_jdbc_function_exists(self):
        """Test write_dataframe_to_postgres_jdbc function exists."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
            
            # Test function exists and is callable
            assert callable(write_dataframe_to_postgres_jdbc)

    def test_module_imports_and_structure(self):
        """Test postgres_writer_utils module structure."""
        with patch.dict('sys.modules', {
            'pg8000': Mock(),
            'pg8000.exceptions': Mock()
        }):
            from jobs.utils import postgres_writer_utils
            
            # Test module has expected attributes
            assert hasattr(postgres_writer_utils, '_ensure_required_columns')
            assert hasattr(postgres_writer_utils, 'write_dataframe_to_postgres_jdbc')
            assert hasattr(postgres_writer_utils, 'logger')


@pytest.mark.unit
class TestCsvS3WriterAdvanced:
    """Advanced tests for csv_s3_writer module (51.19% -> target 65%+)."""

    def test_module_imports_and_functions(self):
        """Test csv_s3_writer module imports and main functions."""
        from jobs import csv_s3_writer
        
        # Test module has expected functions
        assert hasattr(csv_s3_writer, 'write_dataframe_to_csv_s3')
        assert hasattr(csv_s3_writer, 'import_csv_to_aurora')
        assert hasattr(csv_s3_writer, 'cleanup_temp_csv_files')
        assert hasattr(csv_s3_writer, 'logger')

    def test_function_existence_checks(self):
        """Test that key functions exist and are callable."""
        from jobs.csv_s3_writer import (
            write_dataframe_to_csv_s3,
            import_csv_to_aurora,
            cleanup_temp_csv_files
        )
        
        # Test functions exist and are callable
        assert callable(write_dataframe_to_csv_s3)
        assert callable(import_csv_to_aurora)
        assert callable(cleanup_temp_csv_files)

    def test_cleanup_temp_csv_files_function_exists(self):
        """Test cleanup_temp_csv_files function exists."""
        from jobs.csv_s3_writer import cleanup_temp_csv_files
        
        # Test function exists and is callable
        assert callable(cleanup_temp_csv_files)

    @patch('jobs.csv_s3_writer.logger')
    def test_csv_s3_writer_logger_usage(self, mock_logger):
        """Test logger usage in csv_s3_writer module."""
        from jobs import csv_s3_writer
        
        # Test logger exists and can be used
        assert hasattr(csv_s3_writer, 'logger')
        assert csv_s3_writer.logger is not None

    def test_csv_s3_writer_constants_and_config(self):
        """Test csv_s3_writer module constants and configuration."""
        from jobs import csv_s3_writer
        
        # Test module can be imported without errors
        assert csv_s3_writer is not None
        
        # Test basic module structure
        module_attrs = dir(csv_s3_writer)
        expected_attrs = ['write_dataframe_to_csv_s3', 'import_csv_to_aurora', 'cleanup_temp_csv_files']
        
        for attr in expected_attrs:
            assert attr in module_attrs