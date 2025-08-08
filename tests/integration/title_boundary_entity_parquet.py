
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()
df = spark.read.option("header", True).parquet("/home/lakshmi/entity_testing/aws-tb-fact-part-00000-d720b8da-0b6e-41da-901c-cacb5a6e3fc9.c000.snappy.parquet")

# Read Parquet file from local path
#df = spark.read.parquet("file:///path/to/your/file.parquet")

# Show first 5 rows with truncate=False

df.select("value").show(5, truncate=False)
