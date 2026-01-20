"""Fact resource transformer for selecting and ordering fact resource columns."""

from jobs.utils.logger_config import get_logger

logger = get_logger(__name__)


class FactResourceTransformer:
    """Transform fact resource records by selecting required columns."""

    @staticmethod
    def transform(df):
        """
        Transform fact resource data.

        Selects and orders the standard fact resource columns.
        """
        try:
            logger.info(
                "FactResourceTransformer: Transforming data for Fact Resource table"
            )

            # Select required columns in correct order
            transf_df = df.select(
                "end_date",
                "fact",
                "entry_date",
                "entry_number",
                "priority",
                "resource",
                "start_date",
            )

            logger.info(
                f"FactResourceTransformer: Transformation complete, columns: {transf_df.columns}"
            )
            return transf_df

        except Exception as e:
            logger.error(f"FactResourceTransformer: Error occurred - {e}")
            raise
