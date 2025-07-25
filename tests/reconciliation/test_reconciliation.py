import pytest
from pyspark.sql import SparkSession
from tests.reconciliation.utils import get_record_count, load_csv_table, load_parquet_table


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("ReconciliationTest").getOrCreate()

def test_record_count_match(spark):
    """
    Ensure record counts match between source and target tables.
    """
    source_path = "s3://source-bucket/data/source_table/"
    target_path = "s3://target-bucket/data/target_table/"

    source_df = load_csv_table(spark, source_path)
    target_df = load_parquet_table(spark, target_path)

    source_count = get_record_count(source_df)
    target_count = get_record_count(target_df)

    assert abs(source_count - target_count) <= 0, \
        f"Mismatch: source={source_count}, target={target_count}"
