"""Fact transformer for deduplicating and transforming fact records."""
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


class FactTransformer:
    """
    Transform fact records with priority-based deduplication.
    
    Keeps the highest priority record per fact, using entry_date and
    entry_number as tiebreakers.
    """

    @staticmethod
    def transform(df):
        """
        Transform fact data with deduplication.
        
        Deduplicates by fact, keeping the record with highest priority,
        most recent entry_date, and highest entry_number.
        """
        try:
            logger.info("FactTransformer: Transforming data for Fact table")
            
            # Define window specification for deduplication
            window_spec = (
                Window.partitionBy("fact")
                .orderBy("priority", "entry_date", "entry_number")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
            
            # Add row number and keep only top record per fact
            df_with_rownum = df.withColumn("row_num", row_number().over(window_spec))
            transf_df = df_with_rownum.filter(df_with_rownum["row_num"] == 1).drop("row_num")
            
            # Select and order columns
            transf_df = transf_df.select(
                "end_date",
                "entity",
                "fact",
                "field",
                "entry_date",
                "priority",
                "reference_entity",
                "start_date",
                "value",
            )
            
            logger.info(f"FactTransformer: Transformation complete, columns: {transf_df.columns}")
            return transf_df
            
        except Exception as e:
            logger.error(f"FactTransformer: Error occurred - {e}")
            raise
