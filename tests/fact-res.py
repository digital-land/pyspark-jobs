from pyspark.sql import SparkSession
from pyspark.sql.functions import first, lit,to_json, struct,col, regexp_replace
import json

# Create a Spark session
spark = SparkSession.builder.appName("PivotDataset").getOrCreate()

#path = r"\\wsl$\Ubuntu\home\lakshmiproject\data.txt"

# Load the CSV file into a DataFrame
df = spark.read.option("header", True).csv("/home/lakshmi/entity_testing/title_boundary_testing_data.csv")
df.show()
fact_res_columns = ["end-date","fact","entry-date","entry-number", "priority","resource","start-date"]
for column in fact_res_columns:
    if column not in df.columns:
        df = df.withColumn(column, lit("").cast("string"))
df.show()
transf_df = df.select("end-date","fact","entry-date","entry-number", "priority","resource","start-date")

transf_df = transf_df.select("end-date", "fact", "entry-date", "entry-number", "priority", "resource", "start-date")

 
print(transf_df.columns)
transf_df.show()

#pivot_df = pivot_df.withColumn("json",to_json(struct("naptan-code","bus-stop-type" ,"transport-access-node-type")))
#{"bus-stop-type":"CUS","naptan-code":"22000047","transport-access-node-type":"BCT"}
#transf_df.write.mode("overwrite").option("header", True) .csv("/home/lakshmi/entity_testing/pivoted_entity_data.csv")
