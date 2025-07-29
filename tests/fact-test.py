from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import first, lit,to_json, struct,col, regexp_replace,row_number
import json

# Create a Spark session
spark = SparkSession.builder.appName("PivotDataset").getOrCreate()

#path = r"\\wsl$\Ubuntu\home\lakshmiproject\data.txt"

# Load the CSV file into a DataFrame
df = spark.read.option("header", True).csv("/home/lakshmi/entity_testing/title_boundary_testing_data.csv")
df = df.withColumn('priority', lit("").cast("string"))
for col in df.columns:
            if "-" in col:
                new_col = col.replace("-", "_")
                df = df.withColumnRenamed(col, new_col)
df.show()
window_spec = Window.partitionBy("fact").orderBy("priority", "entry_date", "entry_number").rowsBetween(Window.unboundedPreceding, Window.currentRow)
print(f"transform_data_fact:Window specification defined for partitioning by 'fact' and ordering {window_spec})")
# Add row number
df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))
print(f"transform_data_fact:Row number added to DataFrame {df_with_rownum.columns}")

# Filter to keep only the top row per partition
transf_df = df_with_rownum.filter(df_with_rownum["row_num"] == 1).drop("row_num")    
transf_df = transf_df.select("fact","end_date","entity","field","entry_date","priority","reference_entity","start_date", "value")
print(f"transform_data_fact:Final DataFrame after filtering: {transf_df.columns}")
transf_df = transf_df.select("end_date", "entity", "fact", "field", "entry_date", "priority", "reference_entity", "start_date", "value")
transf_df.show()

#pivot_df = pivot_df.withColumn("json",to_json(struct("naptan-code","bus-stop-type" ,"transport-access-node-type")))
#{"bus-stop-type":"CUS","naptan-code":"22000047","transport-access-node-type":"BCT"}
#transf_df.write.mode("overwrite").option("header", True) .csv("/home/lakshmi/entity_testing/pivoted_entity_data.csv")
