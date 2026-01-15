"""Issue transformer for adding date columns and selecting issue fields."""
from pyspark.sql.functions import lit

from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


class IssueTransformer:
    """Transform issue records by adding date columns and selecting fields."""

    @staticmethod
    def transform(df):
        """
        Transform issue data.
        
        Adds empty date columns (start_date, entry_date, end_date) and
        selects the standard issue columns.
        """
        try:
            logger.info("IssueTransformer: Transforming data for Issue table")
            
            # Add empty date columns
            transf_df = (
                df.withColumn("start_date", lit("").cast("string"))
                .withColumn("entry_date", lit("").cast("string"))
                .withColumn("end_date", lit("").cast("string"))
            )
            
            # Select required columns in correct order
            transf_df = transf_df.select(
                "end_date",
                "entity",
                "entry_date",
                "entry_number",
                "field",
                "issue_type",
                "line_number",
                "dataset",
                "resource",
                "start_date",
                "value",
                "message",
            )
            
            logger.info(f"IssueTransformer: Transformation complete, columns: {transf_df.columns}")
            return transf_df
            
        except Exception as e:
            logger.error(f"IssueTransformer: Error occurred - {e}")
            raise
