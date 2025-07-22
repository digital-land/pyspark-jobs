from venv import logger
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# -------------------- Transformation Processing --------------------
def transform_data_fact(df):      
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
    return transf_df

def transform_data_fact_res(df):      
    logger.info("transform_data_fact_res:Transforming data for Fact Resource table")
    transf_df = df.select("end_date","fact","entry_date","entry_number", "priority","resource","start_date")
    logger.info(f"transform_data_fact_res:Final DataFrame after filtering: {transf_df.columns}")
    return transf_df


def transform_data_issues(df):     
    
    return df

def transform_data_entity(df):       
        
    return df