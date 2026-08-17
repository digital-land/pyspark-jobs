"""IssuePipeline: issue data from collection CSVs."""

import logging

from cloudpathlib import AnyPath

from jobs.pipeline.base import BasePipeline
from jobs.read import read_old_resources
from jobs.transform.filter import filter_old_resources
from jobs.transform.issue_transformer import transform_issue
from jobs.utils.df_utils import normalise_column_names, show_df
from jobs.utils.s3_writer_utils import write_delta

logger = logging.getLogger(__name__)


class IssuePipeline(BasePipeline):
    """
    Pipeline for issue data.

    Reads issue CSV, runs IssueTransformer, writes parquet output.
    """

    def execute(self, collection):
        spark = self.config.spark
        dataset = self.config.dataset
        env = self.config.env
        collection_data_path = self.config.collection_data_path

        # -- Extract ----------------------------------------------------------
        base = AnyPath(collection_data_path)
        issue_path = (
            str(base / f"{collection}-collection" / "issue" / dataset) + "/*.csv"
        )

        logger.info(f"IssuePipeline: Reading issue data from {issue_path}")
        issue_df = spark.read.option("header", "true").csv(issue_path)
        issue_df.cache()
        issue_df.printSchema()
        show_df(issue_df, 5, env)

        issue_df = normalise_column_names(issue_df)
        logger.info(f"IssuePipeline: Columns after renaming: {issue_df.columns}")

        # -- Filter old resources ---------------------------------------------
        old_resource_path = (
            base / "config" / "collection" / f"{collection}" / "old-resource.csv"
        )
        try:
            if old_resource_path.exists():
                old_resources_df = read_old_resources(spark, str(old_resource_path))
                issue_df = filter_old_resources(issue_df, old_resources_df)
            else:
                logger.info(
                    f"IssuePipeline: No old-resource.csv found at {old_resource_path}, skipping filter"
                )
        except Exception as e:
            logger.warning(
                f"IssuePipeline: Could not read old-resource.csv, skipping filter: {e}"
            )

        # -- Transform --------------------------------------------------------
        issue_df = transform_issue(issue_df, dataset)
        logger.info("IssuePipeline: issue transform completed")

        # -- Load -------------------------------------------------------------
        parquet_base = AnyPath(self.config.parquet_datasets_path)
        issue_output_path = str(parquet_base / "issue")
        write_delta(issue_df, issue_output_path, dataset, partition_by=["dataset"])
        logger.info("IssuePipeline: Wrote issue Delta table")
