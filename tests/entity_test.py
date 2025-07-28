from pyspark.sql import SparkSession
from pyspark.sql.functions import first, lit,to_json, struct


# Create a Spark session
spark = SparkSession.builder.appName("PivotDataset").getOrCreate()

#path = r"\\wsl$\Ubuntu\home\lakshmiproject\data.txt"

# Load the CSV file into a DataFrame
df = spark.read.option("header", True).csv("/home/lakshmi/entity_testing/entity_sample_data.csv")
dataset_name_arg='transport-access-node'
# Pivot the transport-access-node based on 'field' and 'value', grouped by 'entry-number'
pivot_df = df.groupBy("entry-number","entity").pivot("field").agg(first("value"))
pivot_df = pivot_df.drop("entry-number")
pivot_df=pivot_df.withColumn("dataset", lit(dataset_name_arg)).withColumn("typology", lit("geography"))

# Create JSON column
#pivot_df = pivot_df.withColumn("json",to_json(struct("naptan-code","bus-stop-type" ,"transport-access-node-type")))
#pivot_df.show()
if 'geometry' not in pivot_df.columns:
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    pivot_df = pivot_df.withColumn('geometry', lit("").cast("string"))

pivot_df = pivot_df.drop("geojson")


organisation_df = spark.read.option("header", "true").csv("/home/lakshmi/entity_testing/organisation.csv")
#organisation_df.show()

pivot_df = pivot_df.join(organisation_df, pivot_df.organisation == organisation_df.organisation, "left") \
    .select(pivot_df["*"], organisation_df["entity"].alias("organisation_entity"))
#pivot_df["organisation"].show()
pivot_df = pivot_df.drop("organisation")

standard_column= ["dataset","end-date","entity","entry-date","geometry","json","name","organisation_entity","point","prefix","reference","start-date","typology"]
pivot_df_columns=pivot_df.columns

# Find difference
difference_columns = list(set(pivot_df_columns) - set(standard_column))

# Convert to JSON-like dictionary
difference_json = {"difference_columns": difference_columns}



# Collect the data for those columns
difference_data = pivot_df.select(difference_columns).collect()

# Convert to JSON-like dictionary
difference_json = {
    col: [row[col] for row in difference_data]
    for col in difference_columns
}
print(difference_json)

#pivot_df = pivot_df.withColumn("json",to_json(struct("naptan-code","bus-stop-type" ,"transport-access-node-type")))

pivot_df.write.mode("overwrite").option("header", True) \
.csv("/home/lakshmi/entity_testing/pivoted_entity_data.csv")

# Show the resulting DataFrame
#pivot_df.show()
print("Executed successfully")