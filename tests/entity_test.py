from pyspark.sql import SparkSession
from pyspark.sql.functions import first, lit,to_json, struct


# Create a Spark session
spark = SparkSession.builder.appName("PivotDataset").getOrCreate()

#path = r"\\wsl$\Ubuntu\home\lakshmiproject\data.txt"

# Load the CSV file into a DataFrame
df = spark.read.option("header", True).csv("/home/lakshmi/entity_testing/entity_sample_data.csv")

# Pivot the dataset based on 'field' and 'value', grouped by 'entry-number'
pivot_df = df.groupBy("entry_number","entity").pivot("field").agg(first("value"))
pivot_df = pivot_df.drop("entry_number", "organisation")
pivot_df=pivot_df.withColumn("topology", lit("geography")).withColumn("organisation_entity", lit("600010"))
# Create JSON column
pivot_df = pivot_df.withColumn(
    "json",
    to_json(struct("naptan_code","bus_stop_type" ,"transport_access_node_type"))
)
pivot_df = pivot_df.drop("naptan_code","bus_stop_type","transport_access_node_type")

pivot_df.write.mode("overwrite").option("header", True) \
.csv("/home/lakshmi/entity_testing/pivoted_entity_data.csv")

# Show the resulting DataFrame
#pivot_df.show()
print("Executed successfully")