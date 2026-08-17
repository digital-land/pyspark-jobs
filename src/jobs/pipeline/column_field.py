"""ColumnFieldPipeline: column-field log data."""

import logging

from cloudpathlib import AnyPath

from jobs.pipeline.base import BasePipeline
from jobs.transform.column_field_transformer import transform_column_field
from jobs.utils.df_utils import normalise_column_names, show_df
from jobs.utils.s3_writer_utils import write_delta

logger = logging.getLogger(__name__)


class ColumnFieldPipeline(BasePipeline):
    """
    Pipeline for column field log data.

    Reads column-field CSVs from the var directory and writes to a Delta table.
    """

    def execute(self, collection):
        spark = self.config.spark
        dataset = self.config.dataset
        env = self.config.env
        collection_data_path = self.config.collection_data_path

        base = AnyPath(collection_data_path)
        column_field_path = (
            str(base / f"{collection}-collection" / "var" / "column-field" / dataset)
            + "/*.csv"
        )

        logger.info(f"ColumnFieldPipeline: Reading data from {column_field_path}")
        df = spark.read.option("header", "true").csv(column_field_path)
        df.cache()
        show_df(df, 5, env)

        df = normalise_column_names(df)
        df = transform_column_field(df, dataset)
        logger.info("ColumnFieldPipeline: Transform complete")

        parquet_base = AnyPath(self.config.parquet_datasets_path)
        output_path = str(parquet_base / "column_field")
        write_delta(df, output_path, dataset, partition_by=["dataset"])
        logger.info("ColumnFieldPipeline: Wrote column_field Delta table")
