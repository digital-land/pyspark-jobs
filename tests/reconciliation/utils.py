from pyspark.sql import DataFrame

def get_record_count(df: DataFrame) -> int:
    return df.count()

def load_table(spark, path: str) -> DataFrame:
    """
    Load a table or dataset from the given path.
    """
    return spark.read.parquet(path)
