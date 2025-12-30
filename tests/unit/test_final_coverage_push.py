"""Final targeted tests to push coverage from 76.27% to 80%+ by covering remaining high-impact lines."""
import pytest
import os
import sys
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Mock all external dependencies
with patch.dict('sys.modules', {
    'pyspark': MagicMock(),
    'pyspark.sql': MagicMock(),
    'pyspark.sql.functions': MagicMock(),
    'pyspark.sql.types': MagicMock(),
    'pyspark.sql.window': MagicMock(),
    'pg8000': MagicMock(),
    'boto3': MagicMock(),
    'requests': MagicMock(),
}):
    from jobs.utils import postgres_writer_utils
    from jobs.utils import s3_format_utils
    from jobs.utils import s3_writer_utils


def create_mock_df(columns=None, count_return=100):
    """Create a mock DataFrame."""
    mock_df = Mock()
    mock_df.columns = columns or ['entity', 'name']
    mock_df.count.return_value = count_return
    mock_df.withColumn.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.write = Mock()
    mock_df.write.jdbc = Mock()
    mock_df.schema = []
    mock_df.dtypes = [('entity', 'bigint'), ('name', 'string')]
    return mock_df


@pytest.mark.unit
class TestPostgresWriterUtilsTargeted:
    """Target specific lines in postgres_writer_utils.py."""

    @patch('jobs.utils.postgres_writer_utils.get_aws_secret')
    @patch('pg8000.connect')
    def test_jdbc_retry_mechanism(self, mock_connect, mock_get_secret):
        """Test JDBC retry mechanism."""
        mock_df = create_mock_df(['entity', 'name'], 1000)
        mock_get_secret.return_value = {
            'host': 'localhost', 'port': 5432, 'database': 'test', 
            'user': 'user', 'password': 'pass'
        }
        
        mock_conn = Mock()
        mock_cur = Mock()
        mock_cur.rowcount = 100
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn
        
        # Mock JDBC write to fail then succeed
        mock_df.write.jdbc.side_effect = [Exception("Timeout"), None]
        
        with patch('jobs.utils.postgres_writer_utils._ensure_required_columns') as mock_ensure, \
             patch('hashlib.md5') as mock_md5, \
             patch('time.sleep'):
            
            mock_ensure.return_value = mock_df
            mock_md5.return_value.hexdigest.return_value = 'abcd1234'
            
            postgres_writer_utils.write_dataframe_to_postgres_jdbc(
                mock_df, "entity", "test-dataset", "dev"
            )
            
            assert mock_df.write.jdbc.call_count == 2

    def test_ensure_required_columns_all_types(self):
        """Test all column type handling."""
        mock_df = create_mock_df(['existing_col'])
        required_cols = ['entity', 'json', 'entry_date', 'custom_field']
        
        with patch('jobs.utils.postgres_writer_utils.lit') as mock_lit, \
             patch('jobs.utils.postgres_writer_utils.col') as mock_col, \
             patch('jobs.utils.postgres_writer_utils.to_json') as mock_to_json:
            
            mock_lit.return_value.cast.return_value = 'mocked_column'
            mock_col.return_value.cast.return_value = 'mocked_column'
            mock_to_json.return_value = 'json_string'
            
            result = postgres_writer_utils._ensure_required_columns(
                mock_df, required_cols, {}, Mock()
            )
            
            assert mock_df.withColumn.call_count >= 3
            assert result == mock_df


@pytest.mark.unit
class TestS3FormatUtilsTargeted:
    """Target specific lines in s3_format_utils.py."""

    def test_s3_csv_format_json_detection(self):
        """Test JSON column detection."""
        mock_df = create_mock_df(['json_col', 'normal_col'])
        
        mock_field = Mock()
        mock_field.name = 'json_col'
        mock_field.dataType = Mock()
        mock_field.dataType.__class__.__name__ = 'StringType'
        mock_df.schema = [mock_field]
        
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value='{"key": "value"}')
        mock_df.select.return_value.dropna.return_value.limit.return_value.collect.return_value = [mock_row]
        
        with patch('jobs.utils.s3_format_utils.parse_possible_json') as mock_parse:
            mock_parse.return_value = {"key": "value"}
            
            result = s3_format_utils.s3_csv_format(mock_df)
            
            mock_parse.assert_called()
            assert result == mock_df

    def test_flatten_s3_json_structs(self):
        """Test struct flattening."""
        mock_df = create_mock_df()
        # Mock dtypes as a list of tuples
        mock_df.dtypes = [('flat_col', 'string'), ('struct_col', 'struct<field1:string>')]
        mock_df.select.return_value.columns = ['field1']
        
        # Mock the iteration behavior
        def mock_dtypes_iter():
            return iter([('flat_col', 'string'), ('struct_col', 'struct<field1:string>')])
        mock_df.dtypes = mock_dtypes_iter()
        
        with patch('jobs.utils.s3_format_utils.col') as mock_col:
            mock_col.return_value.alias.return_value = 'aliased_col'
            
            result = s3_format_utils.flatten_s3_json(mock_df)
            
            mock_df.select.assert_called()
            assert result == mock_df


@pytest.mark.unit
class TestS3WriterUtilsTargeted:
    """Target specific lines in s3_writer_utils.py."""

    def test_wkt_to_geojson_point(self):
        """Test POINT conversion."""
        wkt = "POINT (1.234567 2.345678)"
        result = s3_writer_utils.wkt_to_geojson(wkt)
        
        expected = {"type": "Point", "coordinates": [1.234567, 2.345678]}
        assert result == expected

    def test_wkt_to_geojson_polygon(self):
        """Test POLYGON conversion."""
        wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
        result = s3_writer_utils.wkt_to_geojson(wkt)
        
        assert result["type"] == "Polygon"
        assert len(result["coordinates"]) == 1

    @patch('requests.get')
    def test_fetch_dataset_schema_fields_success(self, mock_get):
        """Test schema field fetching."""
        mock_response = Mock()
        mock_response.text = "---\nfields:\n- field: entity\n- field: name\n---"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result = s3_writer_utils.fetch_dataset_schema_fields('test-dataset')
        
        assert result == ['entity', 'name']
        mock_get.assert_called_once()

    @patch('requests.get')
    def test_fetch_dataset_schema_fields_failure(self, mock_get):
        """Test schema field fetching failure."""
        mock_get.side_effect = Exception("Network error")
        
        result = s3_writer_utils.fetch_dataset_schema_fields('test-dataset')
        
        assert result == []

    def test_ensure_schema_fields_missing(self):
        """Test ensuring schema fields when missing."""
        mock_df = create_mock_df(['existing_field'])
        
        with patch('jobs.utils.s3_writer_utils.fetch_dataset_schema_fields') as mock_fetch, \
             patch('jobs.utils.s3_writer_utils.lit') as mock_lit:
            
            mock_fetch.return_value = ['existing_field', 'missing_field']
            mock_lit.return_value = 'empty_string'
            
            result = s3_writer_utils.ensure_schema_fields(mock_df, 'test-dataset')
            
            mock_fetch.assert_called_once()
            mock_df.withColumn.assert_called()
            assert result == mock_df