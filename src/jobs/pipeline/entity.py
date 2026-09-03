"""EntityPipeline: entity, fact, and fact_resource data."""

import json
import logging
from datetime import date, datetime

import boto3
from cloudpathlib import AnyPath, S3Path
from pyspark.sql.functions import col

from jobs.config.metadata import load_metadata
from jobs.pipeline.base import BasePipeline
from jobs.read import read_old_resources, read_resources
from jobs.transform.entity_transformer import transform_entity
from jobs.transform.fact_resource_transformer import transform_fact_resource
from jobs.transform.fact_transformer import transform_fact
from jobs.transform.filter import filter_old_resources
from jobs.utils.df_utils import count_df, normalise_column_names, show_df
from jobs.utils.flatten_csv import flatten_json_column
from jobs.utils.postgres_writer_utils import (
    SUBDIVIDED_DATASETS,
    write_dataframe_to_postgres_jdbc,
    write_entity_subdivided_to_postgres,
)
from jobs.utils.s3_writer_utils import (
    cleanup_temp_path,
    ensure_schema_fields,
    resolve_geometry,
    s3_rename_and_move,
    write_delta,
    write_geojson_entities_s3,
    write_json_entities_s3,
)

logger = logging.getLogger(__name__)


class EntityPipeline(BasePipeline):
    """
    Pipeline for entity, fact, and fact_resource data.

    Takes transformed data and produces fact, fact_resource, and entity data.

    Inputs:
    - Transformed data from bronze layer
    - Organisation dataset (read from gold layer)

    Outputs:
    - fact_resource data to parquet datasets
    - fact data to parquet datasets
    - entity data to parquet datasets
    - entity data to Postgres
    - individual dataset data to S3 (CSV, JSON, GeoJSON consumer formats)
    """

    def execute(self, collection):
        spark = self.config.spark
        dataset = self.config.dataset
        env = self.config.env
        collection_data_path = self.config.collection_data_path
        parquet_path = self.config.parquet_datasets_path

        # -- Extract ----------------------------------------------------------
        base = AnyPath(collection_data_path)
        organisation_path = str(
            base / "organisation-collection" / "dataset" / "organisation.csv"
        )
        transformed_path = (
            str(base / f"{collection}-collection" / "transformed" / dataset)
            + "/*.parquet"
        )
        resource_path = str(
            base / f"{collection}-collection" / "collection" / "resource.csv"
        )

        logger.info(
            f"EntityPipeline: Reading organisation data from {organisation_path}"
        )
        organisation_df = spark.read.option("header", "true").csv(organisation_path)
        organisation_df.cache()

        logger.info(f"EntityPipeline: Reading transformed data from {transformed_path}")
        transformed_df = spark.read.parquet(transformed_path)
        transformed_df.cache()

        logger.info(f"EntityPipeline: Reading resource data from {resource_path}")
        resource_df = read_resources(spark, resource_path)
        resource_df.cache()

        transformed_df.printSchema()
        show_df(transformed_df, 5, env)

        if transformed_df.rdd.isEmpty():
            raise ValueError("EntityPipeline: Transformed DataFrame is empty")

        # -- Filter old resources ---------------------------------------------
        old_resource_path = (
            base / "config" / "collection" / f"{collection}" / "old-resource.csv"
        )
        try:
            if old_resource_path.exists():
                old_resources_df = read_old_resources(spark, str(old_resource_path))
                transformed_df = filter_old_resources(transformed_df, old_resources_df)
            else:
                logger.info(
                    f"EntityPipeline: No old-resource.csv found at {old_resource_path}, skipping filter"
                )
        except Exception as e:
            logger.warning(
                f"EntityPipeline: Could not read old-resource.csv, skipping filter: {e}"
            )

        # Validate schema against schemas.json
        json_data = load_metadata("schemas.json")
        fields = json_data.get("transformed", [])
        logger.info(f"EntityPipeline: Transformed fields from schema: {fields}")

        transformed_df = normalise_column_names(transformed_df)
        logger.info(f"EntityPipeline: Columns after renaming: {transformed_df.columns}")

        if set(fields) == set(transformed_df.columns):
            logger.info("EntityPipeline: All expected fields present")
        else:
            logger.warning("EntityPipeline: Some fields missing from transformed data")

        # -- Filter rows with no resource --------------------------------------
        has_resource = col("resource").isNotNull() & (col("resource") != "")
        dropped_count = transformed_df.filter(~has_resource).count()
        if dropped_count:
            logger.warning(
                f"EntityPipeline: Dropping {dropped_count} row(s) with no resource"
            )
        transformed_df = transformed_df.filter(has_resource)

        # -- Transform --------------------------------------------------------
        fact_resource_df = transform_fact_resource(transformed_df, dataset)
        logger.info("EntityPipeline: fact_resource transform completed")
        show_df(fact_resource_df, 5, env)
        count = count_df(fact_resource_df, env)
        if count is not None:
            logger.info(f"EntityPipeline: fact_resource contains {count} records")

        fact_df = transform_fact(transformed_df, dataset)
        logger.info("EntityPipeline: fact transform completed")
        show_df(fact_df, 5, env)
        fact_count = count_df(fact_df, env)
        if fact_count is not None:
            logger.info(f"EntityPipeline: fact contains {fact_count} records")

        entity_df = transform_entity(
            transformed_df, dataset, organisation_df, resource_df, env
        )
        logger.info("EntityPipeline: entity transform completed")

        # -- Load: parquet ----------------------------------------------------
        parquet_base = AnyPath(parquet_path)
        for table_name, df in [
            ("fact_resource", fact_resource_df),
            ("fact", fact_df),
            ("entity", entity_df),
        ]:
            output_path = str(parquet_base / table_name)
            write_delta(df, output_path, dataset, partition_by=["dataset"])
            logger.info(f"EntityPipeline: Wrote {table_name} Delta table")

        # -- Load: consumer formats (CSV/JSON/GeoJSON) ------------------------
        self._write_consumer_formats(entity_df)

        # -- Load: Postgres ---------------------------------------------------
        self._write_postgres(entity_df)

    def _write_consumer_formats(self, entity_df):
        """Write CSV, parquet, JSON, GeoJSON consumer formats for entity data."""
        dataset = self.config.dataset
        env = self.config.env
        collection_data_path = self.config.collection_data_path

        base = AnyPath(collection_data_path)
        _is_s3 = isinstance(base, S3Path)
        temp_output_path = str(base / "dataset" / "temp" / dataset)

        temp_df = flatten_json_column(entity_df)

        # For CSVs and JSONs in the consumer layer '-' should be used
        for column in temp_df.columns:
            if "_" in column:
                temp_df = temp_df.withColumnRenamed(column, column.replace("_", "-"))

        # Align fields with spec
        temp_df = ensure_schema_fields(temp_df, dataset)

        if _is_s3:
            cleanup_temp_path(env, dataset)

        temp_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            temp_output_path
        )

        self._write_single_parquet(temp_df, base / "dataset", dataset)

        if _is_s3:
            s3_rename_and_move(dataset, "csv", f"{env}-collection-data")
            s3_client = boto3.client("s3")
            self._write_json_s3(s3_client, temp_df, dataset, env)
            self._write_geojson_s3(s3_client, temp_df, dataset, env)
        else:
            self._write_json_local(temp_df, dataset, base)
            self._write_geojson_local(temp_df, dataset, base)

    def _write_single_parquet(self, df, output_path, name):
        """Write df as a single parquet file at output_path/name.parquet.

        Spark writes a directory of part-files, so we coalesce(1) into a temp
        dir, then move the one part-file to the target name. Uses AnyPath
        throughout so it works uniformly for local and S3 output_paths.
        """
        tmp_dir = AnyPath(output_path) / f"_tmp_{name}_parquet"
        df.coalesce(1).write.mode("overwrite").parquet(str(tmp_dir))
        part = next(p for p in tmp_dir.glob("part-*.parquet"))
        target = AnyPath(output_path) / f"{name}.parquet"
        target.write_bytes(part.read_bytes())
        for p in tmp_dir.glob("*"):
            p.unlink()
        tmp_dir.rmdir()
        logger.info(f"EntityPipeline: Wrote {target}")

    def _write_json_s3(self, s3_client, temp_df, dataset, env):
        """Write entity JSON to S3.

        Delegates to write_json_entities_s3, which uploads each partition's
        rows directly from the executor that holds them rather than routing
        every row through the driver -- see that function's docstring for
        why this avoids both the EntityTooLarge failure (a single put_object
        holding the whole document exceeds S3's 5GB limit once the dataset
        is big enough) and the driver bottleneck of a sequential row loop.
        """
        target_key = f"dataset/{dataset}.json"
        write_json_entities_s3(temp_df, s3_client, f"{env}-collection-data", target_key)

    def _write_geojson_s3(self, s3_client, temp_df, dataset, env):
        """Write entity GeoJSON to S3.

        Delegates to write_geojson_entities_s3, which uploads each
        partition's rows directly from the executor that holds them --
        including the WKT-to-GeoJSON geometry conversion, previously the
        most expensive per-row work in this pipeline -- instead of a
        sequential toLocalIterator() loop on the driver. See that
        function's docstring and write_json_entities_s3's for why.
        """
        target_key = f"dataset/{dataset}.geojson"
        write_geojson_entities_s3(
            temp_df, s3_client, f"{env}-collection-data", target_key, dataset
        )

    def _write_json_local(self, temp_df, dataset, base):
        """Write entity JSON to local filesystem."""
        json_buffer = '{"entities":['
        first = True
        for row in temp_df.toLocalIterator():
            if not first:
                json_buffer += ","
            first = False
            row_dict = row.asDict()
            for key, value in row_dict.items():
                if isinstance(value, (date, datetime)):
                    row_dict[key] = value.isoformat() if value else ""
                elif value is None:
                    row_dict[key] = ""
            json_buffer += json.dumps(row_dict)
        json_buffer += "]}"

        output_file = base / "dataset" / f"{dataset}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(str(output_file), "w") as f:
            f.write(json_buffer)
        logger.info(f"EntityPipeline: JSON file written to {output_file}")

    def _write_geojson_local(self, temp_df, dataset, base):
        """Write entity GeoJSON to local filesystem."""
        header = '{"type":"FeatureCollection","name":"' + dataset + '","features":['
        buffer = header
        first_row = True
        for row in temp_df.toLocalIterator():
            row_dict = row.asDict()
            geometry_wkt = row_dict.pop("geometry", None)
            point_wkt = row_dict.pop("point", None)

            for key, value in row_dict.items():
                if isinstance(value, (date, datetime)):
                    row_dict[key] = value.isoformat() if value else ""
                elif value is None:
                    row_dict[key] = ""

            geojson_geom = resolve_geometry(geometry_wkt, point_wkt)
            feature = {
                "type": "Feature",
                "properties": row_dict,
                "geometry": geojson_geom,
            }

            if not first_row:
                buffer += ","
            first_row = False
            buffer += json.dumps(feature)

        buffer += "]}"

        output_file = base / "dataset" / f"{dataset}.geojson"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(str(output_file), "w") as f:
            f.write(buffer)
        logger.info(f"EntityPipeline: GeoJSON file written to {output_file}")

    def _write_postgres(self, entity_df):
        """Write entity data to Postgres via JDBC."""
        dataset = self.config.dataset
        env = self.config.env

        if entity_df is not None and not entity_df.rdd.isEmpty():
            show_df(entity_df, 5, env)
            entity_pg_df = entity_df.drop("processed_timestamp")
            logger.info("EntityPipeline: Writing entity data to Postgres")
            show_df(entity_pg_df, 5, env)
            write_dataframe_to_postgres_jdbc(
                entity_pg_df, "entity", dataset, self.config.database_url
            )

            if dataset in SUBDIVIDED_DATASETS:
                logger.info(
                    f"EntityPipeline: {dataset} requires subdivided geometries, writing to entity_subdivided"
                )
                write_entity_subdivided_to_postgres(
                    entity_pg_df, dataset, self.config.database_url
                )
        else:
            logger.info("EntityPipeline: entity_df is empty, skipping Postgres write")
