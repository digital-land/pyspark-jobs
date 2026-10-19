"""OldEntityPipeline: the dataset's retired and redirected entity numbers."""

import logging

from cloudpathlib import AnyPath
from pyspark.sql.functions import col

from jobs.pipeline.base import BasePipeline
from jobs.transform.old_entity_transformer import filter_old_entity_for_dataset
from jobs.utils.df_utils import normalise_column_names, show_df
from jobs.utils.postgres_writer_utils import write_dataset_old_entity_to_postgres

logger = logging.getLogger(__name__)


class OldEntityPipeline(BasePipeline):
    """
    Pipeline for the dataset's rows in the old_entity table.

    Reads the collection's old-entity.csv from the config and replaces the dataset's rows
    in the old_entity Postgres table, which the website checks before the entity table to
    answer 410 Gone or redirect (301) an entity number that is no longer used.
    """

    def execute(self, collection):
        spark = self.config.spark
        dataset = self.config.dataset
        env = self.config.env

        base = AnyPath(self.config.collection_data_path)
        old_entity_path = base / "config" / "pipeline" / collection / "old-entity.csv"
        if not old_entity_path.exists():
            logger.info(
                f"OldEntityPipeline: No old-entity.csv for {collection}, nothing to write"
            )
            return

        dataset_path = str(base / "specification" / "dataset.csv")
        spec_df = normalise_column_names(
            spark.read.option("header", "true").csv(dataset_path)
        )
        spec = spec_df.filter(col("dataset") == dataset).first()
        if spec is None or not spec["entity_minimum"] or not spec["entity_maximum"]:
            raise ValueError(
                f"OldEntityPipeline: No entity range in the specification for {dataset}"
            )

        logger.info(f"OldEntityPipeline: Reading old entities from {old_entity_path}")
        old_entity_df = spark.read.option("header", "true").csv(str(old_entity_path))
        old_entity_df = filter_old_entity_for_dataset(
            old_entity_df,
            dataset,
            int(spec["entity_minimum"]),
            int(spec["entity_maximum"]),
        )
        show_df(old_entity_df, 5, env)

        write_dataset_old_entity_to_postgres(
            old_entity_df, dataset, self.config.database_url
        )
        logger.info("OldEntityPipeline: Wrote old_entity to Postgres")
