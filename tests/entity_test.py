from pyspark.sql import SparkSession
from pyspark.sql.functions import first, lit,to_json, struct,col, regexp_replace
import json

# Create a Spark session
spark = SparkSession.builder.appName("PivotDataset").getOrCreate()

#path = r"\\wsl$\Ubuntu\home\lakshmiproject\data.txt"

# Load the CSV file into a DataFrame
df = spark.read.option("header", True).csv("/home/lakshmi/entity_testing/entity_sample_data.csv")
dataset_name_arg='transport-access-node'
# Pivot the transport-access-node based on 'field' and 'value', grouped by 'entry-number'
pivot_df = df.groupBy("entry-number","entity").pivot("field").agg(first("value"))
pivot_df = pivot_df.drop("entry-number")
pivot_df=pivot_df.withColumn("dataset", lit(dataset_name_arg))

# Create JSON column
#pivot_df = pivot_df.withColumn("json",to_json(struct("naptan-code","bus-stop-type" ,"transport-access-node-type")))
#pivot_df.show()
if 'geometry' not in pivot_df.columns:
    pivot_df = pivot_df.withColumn('geometry', lit("").cast("string"))

pivot_df = pivot_df.drop("geojson")


organisation_df = spark.read.option("header", "true").csv("/home/lakshmi/entity_testing/organisation.csv")
#organisation_df.show()

pivot_df = pivot_df.join(organisation_df, pivot_df.organisation == organisation_df.organisation, "left") \
    .select(pivot_df["*"], organisation_df["entity"].alias("organisation_entity"))
#pivot_df["organisation"].show()
pivot_df = pivot_df.drop("organisation")



# Step 1: Define standard columns
standard_column = ["dataset", "end-date", "entity", "entry-date", "geometry", "json", "name", "organisation_entity", "point", "prefix", "reference", "start-date", "typology"]
pivot_df_columns = pivot_df.columns

# Step 2: Find difference columns
difference_columns = list(set(pivot_df_columns) - set(standard_column))

# Step 3: Dynamically create a struct of difference columns and convert to JSON
pivot_df_with_json = pivot_df.withColumn("json",to_json(struct(*[col(c) for c in difference_columns]))    )

# Remove escape characters (optional, for readability in CSV)
#pivot_df_with_json = pivot_df_with_json.withColumn(
#    "json",
#    regexp_replace("json", '\\"', '"')  # replaces \" with "
#)
#

#pivot_df_with_json = pivot_df.withColumn(    "json",    struct(*[col(c) for c in difference_columns]))

#pivot_df_with_json.select("json").show(truncate=False)

dataset_df = spark.read.option("header", "true").csv("/home/lakshmi/entity_testing/dataset.csv")

pivot_df_with_json = pivot_df_with_json.join(dataset_df, pivot_df_with_json.dataset == dataset_df.dataset, "left") \
    .select(pivot_df_with_json["*"], dataset_df["typology"])
pivot_df_with_json.select("typology").show(truncate=False)

#pivot_df = pivot_df.withColumn("json",to_json(struct("naptan-code","bus-stop-type" ,"transport-access-node-type")))
#{"bus-stop-type":"CUS","naptan-code":"22000047","transport-access-node-type":"BCT"}
pivot_df_with_json.write.mode("overwrite").option("header", True) \
.csv("/home/lakshmi/entity_testing/pivoted_entity_data.csv")

# Show the resulting DataFrame
#pivot_df.show()
print("Executed successfully")