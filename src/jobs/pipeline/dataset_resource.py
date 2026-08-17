"""DatasetResourcePipeline: dataset-resource log data."""

import logging

from cloudpathlib import AnyPath

from jobs.pipeline.base import BasePipeline
from jobs.transform.dataset_resource_transformer import transform_dataset_resource
from jobs.utils.df_utils import normalise_column_names, show_df
from jobs.utils.s3_writer_utils import write_delta

logger = logging.getLogger(__name__)


class DatasetResourcePipeline(BasePipeline):
    """
    Pipeline for dataset resource data.

    Reads dataset-resource CSVs from the var directory and writes to a Delta table.
    """

    def execute(self, collection):
        spark = self.config.spark
        dataset = self.config.dataset
        env = self.config.env
        collection_data_path = self.config.collection_data_path

        base = AnyPath(collection_data_path)
        dataset_resource_path = (
            str(
                base / f"{collection}-collection" / "var" / "dataset-resource" / dataset
            )
            + "/*.csv"
        )

        logger.info(
            f"DatasetResourcePipeline: Reading data from {dataset_resource_path}"
        )
        df = spark.read.option("header", "true").csv(dataset_resource_path)
        df.cache()
        show_df(df, 5, env)

        df = normalise_column_names(df)
        df = transform_dataset_resource(df, dataset)
        logger.info("DatasetResourcePipeline: Transform complete")

        parquet_base = AnyPath(self.config.parquet_datasets_path)
        output_path = str(parquet_base / "dataset_resource")
        write_delta(df, output_path, dataset, partition_by=["dataset"])
        logger.info("DatasetResourcePipeline: Wrote dataset_resource Delta table")
