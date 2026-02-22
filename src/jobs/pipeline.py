"""
Pipeline classes for ETL processes.

Each pipeline class is responsible for extracting, transforming, and loading
data. The BasePipeline enforces the correct entry point (run) and automatically
tracks pipeline performance metrics (timing, status).

Transform, extract/read and load/write functions should be defined outside of
this module and tested independently.
"""

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime

import boto3
from cloudpathlib import AnyPath, S3Path
from pyspark.sql import SparkSession

from jobs.config.metadata import load_metadata
from jobs.transform.entity_transformer import EntityTransformer
from jobs.transform.fact_resource_transformer import FactResourceTransformer
from jobs.transform.fact_transformer import FactTransformer
from jobs.transform.issue_transformer import IssueTransformer
from jobs.utils.df_utils import count_df, normalise_column_names, show_df
from jobs.utils.flatten_csv import flatten_json_column
from jobs.utils.postgres_writer_utils import write_dataframe_to_postgres_jdbc
from jobs.utils.s3_utils import cleanup_dataset_data
from jobs.utils.s3_writer_utils import (
    cleanup_temp_path,
    ensure_schema_fields,
    s3_rename_and_move,
    wkt_to_geojson,
    write_parquet,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    """Universals shared across all pipelines."""

    spark: SparkSession
    dataset: str
    env: str
    collection_data_path: str
    parquet_datasets_path: str
    database_url: str = ""


class BasePipeline(ABC):
    """
    Base class for all pipelines.

    Automatically tracks start/end times and status. Subclasses implement
    execute() with their own typed signature. The public entry point is run().
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.result = {}

    def run(self, **kwargs):
        """Execute the pipeline with automatic timing and result tracking.

        Forwards all keyword arguments to execute(). Each child class
        declares exactly what arguments it needs in its execute() signature.
        """
        start_time = datetime.now()
        logger.info(f"{self.__class__.__name__}: Started at {start_time}")
        try:
            self.execute(**kwargs)
        except Exception:
            logger.exception(f"{self.__class__.__name__}: Failed")
            self.result["status"] = "failed"
            raise
        finally:
            end_time = datetime.now()
            duration = end_time - start_time
            self.result["pipeline"] = self.__class__.__name__
            self.result["dataset"] = self.config.dataset
            self.result["start_time"] = start_time.isoformat()
            self.result["end_time"] = end_time.isoformat()
            self.result["duration_seconds"] = duration.total_seconds()
            self.result.setdefault("status", "success")
            logger.info(f"{self.__class__.__name__}: {self.result}")

    @abstractmethod
    def execute(self, **kwargs):
        """Pipeline-specific logic. Subclasses must implement this."""
        ...


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
            str(base / f"{collection}-collection" / "transformed" / dataset) + "/*.csv"
        )

        logger.info(
            f"EntityPipeline: Reading organisation data from {organisation_path}"
        )
        organisation_df = spark.read.option("header", "true").csv(organisation_path)
        organisation_df.cache()

        logger.info(f"EntityPipeline: Reading transformed data from {transformed_path}")
        transformed_df = spark.read.option("header", "true").csv(transformed_path)
        transformed_df.cache()
        transformed_df.printSchema()
        show_df(transformed_df, 5, env)

        if transformed_df.rdd.isEmpty():
            raise ValueError("EntityPipeline: Transformed DataFrame is empty")

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

        # -- Transform --------------------------------------------------------
        fact_resource_df = FactResourceTransformer().transform(transformed_df, dataset)
        logger.info("EntityPipeline: fact_resource transform completed")
        show_df(fact_resource_df, 5, env)
        count = count_df(fact_resource_df, env)
        if count is not None:
            logger.info(f"EntityPipeline: fact_resource contains {count} records")

        fact_df = FactTransformer().transform(transformed_df, dataset)
        logger.info("EntityPipeline: fact transform completed")
        show_df(fact_df, 5, env)
        fact_count = count_df(fact_df, env)
        if fact_count is not None:
            logger.info(f"EntityPipeline: fact contains {fact_count} records")

        entity_df = EntityTransformer().transform(
            transformed_df, dataset, organisation_df
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
            if isinstance(parquet_base, S3Path):
                cleanup_dataset_data(output_path, dataset)
            write_parquet(df, output_path, partition_by=["dataset"])
            logger.info(f"EntityPipeline: Wrote {table_name} parquet")

        # -- Load: consumer formats (CSV/JSON/GeoJSON) ------------------------
        self._write_consumer_formats(entity_df)

        # -- Load: Postgres ---------------------------------------------------
        self._write_postgres(entity_df)

    def _write_consumer_formats(self, entity_df):
        """Write CSV, JSON, GeoJSON consumer formats for entity data."""
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

        if _is_s3:
            s3_rename_and_move(dataset, "csv", f"{env}-collection-data")
            s3_client = boto3.client("s3")
            self._write_json_s3(s3_client, temp_df, dataset, env)
            self._write_geojson_s3(s3_client, temp_df, dataset, env)
        else:
            self._write_json_local(temp_df, dataset, base)
            self._write_geojson_local(temp_df, dataset, base)

    def _write_json_s3(self, s3_client, temp_df, dataset, env):
        """Write entity JSON to S3."""
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

        target_key = f"dataset/{dataset}.json"
        try:
            s3_client.head_object(Bucket=f"{env}-collection-data", Key=target_key)
            s3_client.delete_object(Bucket=f"{env}-collection-data", Key=target_key)
        except s3_client.exceptions.ClientError:
            pass

        s3_client.put_object(
            Bucket=f"{env}-collection-data",
            Key=target_key,
            Body=json_buffer,
        )
        logger.info(f"EntityPipeline: JSON file written to {target_key}")

    def _write_geojson_s3(self, s3_client, temp_df, dataset, env):
        """Write entity GeoJSON to S3 using multipart upload."""
        row_count = temp_df.count()
        target_key_geojson = f"dataset/{dataset}.geojson"

        try:
            s3_client.head_object(
                Bucket=f"{env}-collection-data", Key=target_key_geojson
            )
            s3_client.delete_object(
                Bucket=f"{env}-collection-data", Key=target_key_geojson
            )
        except s3_client.exceptions.ClientError:
            pass

        mpu = s3_client.create_multipart_upload(
            Bucket=f"{env}-collection-data", Key=target_key_geojson
        )
        parts = []
        part_num = 1

        try:
            header = '{"type":"FeatureCollection","name":"' + dataset + '","features":['
            buffer = header

            batch_size = 10000
            num_partitions = max(1, row_count // batch_size)

            first_row = True
            for partition_id, rows in enumerate(
                temp_df.repartition(num_partitions).toLocalIterator()
            ):
                row_dict = rows.asDict()
                geometry_wkt = row_dict.pop("geometry", None)
                row_dict.pop("point", None)

                for key, value in row_dict.items():
                    if isinstance(value, (date, datetime)):
                        row_dict[key] = value.isoformat() if value else ""
                    elif value is None:
                        row_dict[key] = ""

                geojson_geom = wkt_to_geojson(geometry_wkt) if geometry_wkt else None
                feature = {
                    "type": "Feature",
                    "properties": row_dict,
                    "geometry": geojson_geom,
                }

                if not first_row:
                    buffer += ","
                first_row = False
                buffer += json.dumps(feature)

                if len(buffer.encode("utf-8")) > 5 * 1024 * 1024:
                    part = s3_client.upload_part(
                        Bucket=f"{env}-collection-data",
                        Key=target_key_geojson,
                        PartNumber=part_num,
                        UploadId=mpu["UploadId"],
                        Body=buffer,
                    )
                    parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                    part_num += 1
                    buffer = ""

            buffer += "]}"
            part = s3_client.upload_part(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                PartNumber=part_num,
                UploadId=mpu["UploadId"],
                Body=buffer,
            )
            parts.append({"PartNumber": part_num, "ETag": part["ETag"]})

            s3_client.complete_multipart_upload(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                UploadId=mpu["UploadId"],
                MultipartUpload={"Parts": parts},
            )
            logger.info(f"EntityPipeline: GeoJSON file written to {target_key_geojson}")
        except Exception as e:
            logger.error(f"Error during GeoJSON multipart upload: {e}")
            s3_client.abort_multipart_upload(
                Bucket=f"{env}-collection-data",
                Key=target_key_geojson,
                UploadId=mpu["UploadId"],
            )
            raise

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
            row_dict.pop("point", None)

            for key, value in row_dict.items():
                if isinstance(value, (date, datetime)):
                    row_dict[key] = value.isoformat() if value else ""
                elif value is None:
                    row_dict[key] = ""

            geojson_geom = wkt_to_geojson(geometry_wkt) if geometry_wkt else None
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
        else:
            logger.info("EntityPipeline: entity_df is empty, skipping Postgres write")


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

        # -- Transform --------------------------------------------------------
        issue_df = IssueTransformer().transform(issue_df, dataset)
        logger.info("IssuePipeline: issue transform completed")

        # -- Load -------------------------------------------------------------
        parquet_base = AnyPath(self.config.parquet_datasets_path)
        issue_output_path = str(parquet_base / "issue")
        if isinstance(parquet_base, S3Path):
            cleanup_dataset_data(issue_output_path, dataset)
        write_parquet(issue_df, issue_output_path, partition_by=["dataset"])
        logger.info("IssuePipeline: Wrote issue parquet")
