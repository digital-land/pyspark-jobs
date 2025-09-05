from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)
from pyspark.sql.functions import row_number, lit, first, to_json, struct, col, when, to_date
from pyspark.sql.window import Window

# -------------------- Transformation Processing --------------------
def transform_data_fact(df):
    try:      
        logger.info("transform_data_fact:Transforming data for Fact table")
        # Define the window specification
        window_spec = Window.partitionBy("fact").orderBy("priority", "entry_date", "entry_number").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        logger.info(f"transform_data_fact:Window specification defined for partitioning by 'fact' and ordering {window_spec})")
        # Add row number
        df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))
        logger.info(f"transform_data_fact:Row number added to DataFrame {df_with_rownum.columns}")
        
        # Filter to keep only the top row per partition
        transf_df = df_with_rownum.filter(df_with_rownum["row_num"] == 1).drop("row_num")    
        transf_df = transf_df.select("fact","end_date","entity","field","entry_date","priority","reference_entity","start_date", "value")
        logger.info(f"transform_data_fact:Final DataFrame after filtering: {transf_df.columns}")
        transf_df = transf_df.select("end_date", "entity", "fact", "field", "entry_date", "priority", "reference_entity", "start_date", "value")
        return transf_df
    except Exception as e:
        logger.error(f"transform_data_fact:Error occurred - {e}")
        raise
    

def transform_data_fact_res(df): 
    try:     
        logger.info("transform_data_fact_res:Transforming data for Fact Resource table")
        transf_df = df.select("end_date","fact","entry_date","entry_number", "priority","resource","start_date")
        logger.info(f"transform_data_fact_res:Final DataFrame after filtering: {transf_df.columns}")

        transf_df = transf_df.select("end_date", "fact", "entry_date", "entry_number", "priority", "resource", "start_date")

        return transf_df
    except Exception as e:
        logger.error(f"transform_data_fact_res:Error occurred - {e}")
        raise


def transform_data_issue(df): 
    try:
        logger.info("transform_data_issue:Transforming data for Issue table") 
        transf_df = df.withColumn("start_date", lit("").cast("string")) \
        .withColumn("entry_date", lit("").cast("string")) \
        .withColumn("end_date", lit("").cast("string"))   \

        logger.info(f"transform_data_fact_res:Final DataFrame after filtering: {transf_df.columns}")  
        transf_df = transf_df.select("end_date", "entity", "entry_date", "entry_number", "field", "issue_type", "line_number", "dataset", "resource", "start_date", "value", "message")    
        return transf_df
    except Exception as e:
        logger.error(f"transform_data_issue:Error occurred - {e}")
        raise


def transform_data_entity(df,data_set,spark):
    try: 
        logger.info("transform_data_entity:Transforming data for Entity table") 
        # Pivot the dataset based on 'field' and 'value', grouped by 'entry-number'
        #pivot_df = df.groupBy("entry_number","entity").pivot("field").agg(first("value"))
        #pivot_df = pivot_df.drop("entry_number", "organisation")
        #pivot_df=pivot_df.withColumn("topology", lit("geography")).withColumn("organisation_entity", lit("600010"))
        # Create JSON column
        #pivot_df = pivot_df.withColumn("json",to_json(struct("naptan_code","bus_stop_type" ,"transport_access_node_type")))
        #pivot_df = pivot_df.drop("naptan_code","bus_stop_type","transport_access_node_type")

        # Pivot the transport-access-node based on 'field' and 'value', grouped by 'entry-number'
        pivot_df = df.groupBy("entry_number","entity").pivot("field").agg(first("value"))
        for column in pivot_df.columns:  # <-- changed from 'col' to 'column'
            if "-" in column:
                new_col = column.replace("-", "_")
                pivot_df = pivot_df.withColumnRenamed(column, new_col)
        logger.info(f"transform_data_entity:Pivoted DataFrame columns: {pivot_df.columns}")
        pivot_df = pivot_df.drop("entry_number")
        logger.info(f"transform_data_entity:DataFrame after dropping 'entry_number': {pivot_df.columns}")
        pivot_df=pivot_df.withColumn("dataset", lit(data_set))
        logger.info(f"transform_data_entity:Final DataFrame after filtering: {pivot_df.columns}")
        #Create JSON column
        if 'geometry' not in pivot_df.columns:
            pivot_df = pivot_df.withColumn('geometry', lit(None).cast("string"))

        pivot_df = pivot_df.drop("geojson")
        
        organisation_df = spark.read.option("header", "true").csv("s3://development-collection-data/organisation/dataset/organisation.csv")
        pivot_df = pivot_df.join(organisation_df, pivot_df.organisation == organisation_df.organisation, "left") \
            .select(pivot_df["*"], organisation_df["entity"].alias("organisation_entity"))
        pivot_df = pivot_df.drop("organisation")
        # Define standard columns
        standard_column = ["dataset", "end_date", "entity", "entry_date", "geometry", "json", "name", "organisation_entity", "point", "prefix", "reference", "start_date", "typology"]
        pivot_df_columns = pivot_df.columns

        #Find difference columns
        difference_columns = list(set(pivot_df_columns) - set(standard_column))

        # Dynamically create a struct of difference columns and convert to JSON
        pivot_df_with_json = pivot_df.withColumn("json", to_json(struct(*[col(c) for c in difference_columns])))

        dataset_df = spark.read.option("header", "true").csv("s3://development-collection-data/emr-data-processing/specification/dataset/dataset.csv")

        pivot_df_with_json = pivot_df_with_json.join(dataset_df, pivot_df_with_json.dataset == dataset_df.dataset, "left") \
            .select(pivot_df_with_json["*"], dataset_df["typology"])
        
        pivot_df_with_json.select("typology").show(truncate=False)

        logger.info(f"transform_data_entity:Final DataFrame after filtering: {pivot_df_with_json.show(truncate=False)}")

        # Add missing columns with default values
        if 'end_date' not in pivot_df_with_json.columns:
            pivot_df_with_json = pivot_df_with_json.withColumn('end_date', lit(None).cast("date"))
        if 'start_date' not in pivot_df_with_json.columns:
            pivot_df_with_json = pivot_df_with_json.withColumn('start_date', lit(None).cast("date"))
        if 'name' not in pivot_df_with_json.columns:
            pivot_df_with_json = pivot_df_with_json.withColumn('name', lit("").cast("string"))
        if 'point' not in pivot_df_with_json.columns:
            pivot_df_with_json = pivot_df_with_json.withColumn('point', lit(None).cast("string"))

        # Fix date and geometry columns: convert empty strings to NULL for PostgreSQL compatibility
        logger.info("transform_data_entity: Fixing date and geometry columns for PostgreSQL compatibility")
        
        # Handle date columns
        date_columns = ["end_date", "entry_date", "start_date"]
        for date_col in date_columns:
            if date_col in pivot_df_with_json.columns:
                logger.info(f"transform_data_entity: Processing date column: {date_col}")
                pivot_df_with_json = pivot_df_with_json.withColumn(
                    date_col, 
                    when(col(date_col) == "", None)
                    .when(col(date_col).isNull(), None)
                    .otherwise(to_date(col(date_col), "yyyy-MM-dd"))
                )
        
        # Handle geometry columns: convert empty strings to NULL and format WKT for PostgreSQL
        geometry_columns = ["geometry", "point"]
        for geom_col in geometry_columns:
            if geom_col in pivot_df_with_json.columns:
                logger.info(f"transform_data_entity: Processing geometry column: {geom_col}")
                pivot_df_with_json = pivot_df_with_json.withColumn(
                    geom_col,
                    when(col(geom_col) == "", None)
                    .when(col(geom_col).isNull(), None)
                    .when(col(geom_col).startswith("POINT"), col(geom_col))  # Keep valid WKT as-is
                    .when(col(geom_col).startswith("POLYGON"), col(geom_col))  # Keep valid WKT as-is  
                    .when(col(geom_col).startswith("MULTIPOLYGON"), col(geom_col))  # Keep valid WKT as-is
                    .otherwise(None)  # Invalid geometry → NULL
                )
        
        pivot_df_with_json = pivot_df_with_json.select("dataset", "end_date", "entity", "entry_date", "geometry", "json", "name", "organisation_entity", "point", "prefix", "reference", "start_date", "typology")

        #pivot_df_with_json.write.mode("overwrite").option("header", True) \
        #.csv("/home/lakshmi/entity_testing/pivoted_entity_data.csv")
        return pivot_df_with_json
    except Exception as e:
        logger.error(f"transform_data_entity:Error occurred - {e}")
        raise