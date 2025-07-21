from venv import logger
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

def transform_data_fact(df):      
    logger.info("Transforming data for fact table")
    # Define the window specification
    window_spec = Window.partitionBy("fact").orderBy("priority", "entry_date", "entry_number").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    logger.info(f"Window specification defined for partitioning by 'fact' and ordering {window_spec})")
    # Add row number
    df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))
    logger.info(f"Row number added to DataFrame {df_with_rownum.columns}")
    
    # Filter to keep only the top row per partition
    final_df = df_with_rownum.filter(df_with_rownum["row_num"] == 1).drop("row_num")
    logger.info(f"Final DataFrame after filtering: {final_df.columns}")

    return final_df


def transform_data_issues(df):      
    
    return df

def transform_data_entity(df):     
    
        
    return df
