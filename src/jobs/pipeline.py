"""
Pipeline classes for ETL processes.

Each pipeline class is responsible for extracting, transforming, and loading
data. The BasePipeline enforces the correct entry point (run) and automatically
tracks pipeline performance metrics (timing, status).

Transform, extract/read and load/write functions should be defined outside of
this module and tested independently.
"""

import csv
import json
import logging
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from functools import reduce

import boto3
from cloudpathlib import AnyPath, S3Path
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    countDistinct,
    explode,
    first,
    lit,
    lower,
    row_number,
    split,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (
    when,
)

from jobs.config.metadata import load_metadata
from jobs.read import read_old_resources
from jobs.transform.column_field_transformer import transform_column_field
from jobs.transform.dataset_resource_transformer import transform_dataset_resource
from jobs.transform.entity_transformer import transform_entity
from jobs.transform.fact_resource_transformer import transform_fact_resource
from jobs.transform.fact_transformer import transform_fact
from jobs.transform.filter import filter_old_resources
from jobs.transform.issue_transformer import transform_issue
from jobs.transform.task_transformer import (
    transform_issues_to_tasks,
    transform_log_to_tasks,
)
from jobs.utils.df_utils import count_df, normalise_column_names, show_df
from jobs.utils.flatten_csv import flatten_json_column
from jobs.utils.postgres_writer_utils import (
    SUBDIVIDED_DATASETS,
    write_dataframe_to_postgres_jdbc,
    write_entity_subdivided_to_postgres,
    write_table_to_postgres,
    write_task_to_postgres,
)
from jobs.utils.s3_writer_utils import (
    cleanup_temp_path,
    ensure_schema_fields,
    resolve_geometry,
    s3_rename_and_move,
    write_delta,
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

        # -- Filter old resources ---------------------------------------------
        old_resource_path = (
            base
            / "config"
            / "collection"
            / f"{collection}-collection"
            / "old-resource.csv"
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

        entity_df = transform_entity(transformed_df, dataset, organisation_df)
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
            base
            / "config"
            / "collection"
            / f"{collection}-collection"
            / "old-resource.csv"
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


ISSUE_TYPE_URL = "https://raw.githubusercontent.com/digital-land/specification/main/content/issue-type.csv"


def _load_issue_type_df(spark):
    with urllib.request.urlopen(ISSUE_TYPE_URL) as response:
        lines = [line.decode("utf-8") for line in response.readlines()]
        reader = csv.DictReader(lines)
        rows = [
            (row["issue-type"], row["severity"], row["responsibility"])
            for row in reader
        ]
    return spark.createDataFrame(rows, ["issue_type", "severity", "responsibility"])


def _backfill_dataset_from_source(log_df, endpoint_dataset_df):
    """
    Fill in missing dataset values for failed log entries using the endpoint → source lookup.

    Rows with dataset already set are left unchanged. Rows with dataset="" are joined
    against endpoint_dataset_df; if an endpoint serves multiple datasets the row expands
    to one row per dataset.
    """
    missing_df = log_df.filter(col("dataset") == "")
    resolved_df = (
        missing_df.drop("dataset")
        .join(endpoint_dataset_df, on="endpoint", how="left")
        .fillna("", subset=["dataset"])
    )
    return log_df.filter(col("dataset") != "").unionByName(resolved_df)


def _backfill_organisation_from_source(log_df, endpoint_organisation_df):
    """
    Fill in missing organisation values for failed log entries using the endpoint → source lookup.

    Mirrors _backfill_dataset_from_source: rows with organisation already set are left
    unchanged, rows with organisation="" are joined against endpoint_organisation_df.
    """
    missing_df = log_df.filter(col("organisation") == "")
    resolved_df = (
        missing_df.drop("organisation")
        .join(endpoint_organisation_df, on="endpoint", how="left")
        .fillna("", subset=["organisation"])
    )
    return log_df.filter(col("organisation") != "").unionByName(resolved_df)


def _active_resources_from_log(log_df, endpoint_attrs_df):
    """The resource each endpoint is currently serving, derived from the log.

    dataset/organisation are attributed from source.csv (endpoint_attrs_df),
    keyed on that endpoint, rather than from resource.csv's flattened sets.
    """
    latest_per_endpoint = Window.partitionBy("endpoint").orderBy(
        col("entry_date").desc(), col("resource").asc()
    )
    current = (
        log_df.filter(
            (col("status") == "200")
            & col("resource").isNotNull()
            & (col("resource") != "")
        )
        .withColumn("_rank", row_number().over(latest_per_endpoint))
        .filter(col("_rank") == 1)
        .select("endpoint", "resource")
    )

    if endpoint_attrs_df is not None:
        current = current.join(endpoint_attrs_df, on="endpoint", how="left")
    else:
        current = current.withColumn("dataset", lit("")).withColumn(
            "organisation", lit("")
        )

    return (
        current.select("resource", "dataset", "organisation", "endpoint")
        .fillna("", subset=["dataset", "organisation"])
        .distinct()
    )


class TaskPipeline(BasePipeline):
    """
    Cross-collection pipeline for generating task data from log and issue files.

    Unlike other pipelines, this reads across all collections at once using
    wildcard S3 paths rather than processing a single dataset/collection.
    Writes a Delta Lake table — full overwrite each run since the table is
    regenerated from scratch nightly.
    """

    def execute(self):
        spark = self.config.spark
        base = AnyPath(self.config.collection_data_path)

        # -- Resolve file paths ---------------------------------------------------
        log_files = [str(p) for p in base.glob("*-collection/collection/log.csv")]
        logger.info(f"TaskPipeline: Found {len(log_files)} log files")

        issue_files = [str(p) for p in base.glob("*-collection/issue/*/*.csv")]
        logger.info(f"TaskPipeline: Found {len(issue_files)} issue files")

        source_files = [str(p) for p in base.glob("*-collection/collection/source.csv")]
        logger.info(f"TaskPipeline: Found {len(source_files)} source files")

        log_df = spark.read.option("header", "true").csv(log_files)
        log_df = normalise_column_names(log_df)

        # -- Endpoint → dataset/organisation lookups (from source.csv) ----------
        # source.csv maps endpoint hash → pipelines (dataset name) + organisation,
        # with ';' separating multiple datasets when one endpoint serves several.
        if source_files:
            source_df = spark.read.option("header", "true").csv(source_files)
            source_df = normalise_column_names(source_df)
            active_source_df = source_df.filter(
                col("end_date").isNull() | (col("end_date") == "")
            )

            # Exploded (one row per endpoint+dataset) — backfills failed log
            # entries, where a blank resource can't tell us the dataset.
            endpoint_dataset_df = active_source_df.select(
                col("endpoint"),
                explode(split(col("pipelines"), ";")).alias("dataset"),
            ).distinct()

            # dropDuplicates, not distinct because organisation isn't part of the reference hash
            endpoint_organisation_df = active_source_df.select(
                "endpoint", "organisation"
            ).dropDuplicates(["endpoint"])

            # One row per endpoint carrying its dataset(s) as the raw ';' string
            # and organisation — attributes the active resource below. Kept as a
            # single row per endpoint so the issue join stays one-to-one.
            endpoint_attrs_df = active_source_df.select(
                "endpoint",
                col("pipelines").alias("dataset"),
                "organisation",
            ).dropDuplicates(["endpoint"])
        else:
            endpoint_dataset_df = None
            endpoint_organisation_df = None
            endpoint_attrs_df = None

        # -- Active resources (current resource per endpoint, from the log) ------
        active_df = _active_resources_from_log(log_df, endpoint_attrs_df)
        active_df.cache()
        logger.info(
            "TaskPipeline: Active resources loaded (current resource per endpoint from log)"
        )

        # -- Log tasks --------------------------------------------------------
        log_df = log_df.join(
            active_df.select("resource", "dataset", "organisation"),
            on="resource",
            how="left",
        )
        log_df = log_df.fillna("", subset=["dataset", "organisation"])

        if endpoint_dataset_df is not None:
            log_df = _backfill_dataset_from_source(log_df, endpoint_dataset_df)
        if endpoint_organisation_df is not None:
            log_df = _backfill_organisation_from_source(
                log_df, endpoint_organisation_df
            )

        log_tasks = transform_log_to_tasks(log_df)

        # -- Issue tasks ------------------------------------------------------
        if not issue_files:
            logger.warning("TaskPipeline: No issue files found — skipping issue tasks")
            issue_tasks = None
        else:
            issue_df = spark.read.option("header", "true").csv(issue_files)
            issue_df = normalise_column_names(issue_df)
            issue_df = issue_df.join(
                active_df.select("resource", "organisation", "endpoint"),
                on="resource",
                how="inner",
            )
            issue_df = issue_df.fillna("", subset=["organisation", "endpoint"])

            issue_type_df = _load_issue_type_df(spark)
            issue_df = issue_df.join(issue_type_df, on="issue_type", how="left")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"TaskPipeline: {issue_df.count()} issue rows for active resources after joining with issue type metadata"
                )
                logger.debug(
                    f"TaskPipeline: {issue_df.filter(col('severity').isin('error', 'warning', 'notice')).count()} rows with severity in (error, warning, notice) — these become issue tasks"
                )

            issue_tasks = transform_issues_to_tasks(issue_df)

        # -- Union and write --------------------------------------------------
        frames = [df for df in [log_tasks, issue_tasks] if df is not None]

        if not frames:
            logger.warning("TaskPipeline: No tasks generated — nothing to write")
            return

        tasks_df = (
            frames[0]
            if len(frames) == 1
            else reduce(lambda a, b: a.unionByName(b), frames)
        )

        output_path = str(AnyPath(self.config.parquet_datasets_path) / "task")
        logger.info(f"TaskPipeline: Writing tasks to {output_path}...")
        (
            tasks_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )
        logger.info(f"TaskPipeline: Delta table written to {output_path}")

        if self.config.database_url:
            logger.info("TaskPipeline: Writing tasks to Postgres...")
            self._write_postgres(tasks_df)

    def _write_postgres(self, tasks_df):
        write_task_to_postgres(tasks_df, self.config.database_url)


def _owner_side(eq):
    """Owner lens: per (dataset, organisation) that owns entities, its quality
    (authoritative if it owns any authoritative entity, else some) and the count
    of entities it owns."""
    agg = eq.groupBy("dataset", "organisation").agg(
        spark_sum(when(col("quality") == "authoritative", 1).otherwise(0)).alias(
            "auth_owned"
        ),
        countDistinct("entity").alias("owned_entity_count"),
    )
    return agg.select(
        "dataset",
        "organisation",
        lit(True).alias("owns_entities"),
        when(col("auth_owned") > 0, lit("authoritative"))
        .otherwise(lit("some"))
        .alias("owner_quality"),
        col("owned_entity_count"),
    )


def _seeder_alt_sources(eq, lookup_df, entity_org_df, active_orgs):
    """Seeder (alt-source) detection. An active org that seeded 'some'-quality
    entities it does NOT own and is NOT designated for counts as a 'some'
    contributor. LA-type orgs must have seeded for >1 distinct owner to
    count (stale-lookup guard); other org types need >=1."""
    some_ent = eq.filter(col("quality") == "some").select(
        "dataset", "entity", col("organisation").alias("owner_org")
    )
    some_owner_orgs = some_ent.select(
        "dataset", col("owner_org").alias("organisation")
    ).distinct()

    lkp = lookup_df.select("entity", col("organisation").alias("seeder"))
    candidates = some_ent.join(lkp, on="entity", how="inner")

    # seeder must not itself own 'some' entities in this dataset ...
    candidates = candidates.join(
        some_owner_orgs.select("dataset", col("organisation").alias("seeder")),
        on=["dataset", "seeder"],
        how="left_anti",
    )
    # ... must not be designated for this dataset ...
    candidates = candidates.join(
        entity_org_df.select("dataset", col("organisation").alias("seeder")),
        on=["dataset", "seeder"],
        how="left_anti",
    )
    # ... and must be active.
    candidates = candidates.join(
        active_orgs.select(col("organisation").alias("seeder")),
        on="seeder",
        how="left_semi",
    )

    coverage = candidates.groupBy("dataset", "seeder").agg(
        countDistinct("owner_org").alias("owner_coverage"),
        countDistinct("entity").alias("seeded_count"),
    )
    is_la = (
        col("seeder").startswith("local-authority:")
        | col("seeder").startswith("national-park-authority:")
        | col("seeder").startswith("development-corporation:")
    )
    alt = coverage.filter(
        (is_la & (col("owner_coverage") > 1)) | (~is_la & (col("owner_coverage") >= 1))
    )
    return alt.select(
        "dataset",
        col("seeder").alias("organisation"),
        lit("some").alias("seeder_quality"),
        col("seeded_count").alias("seeder_entity_count"),
    )


def _live_datasets(dataset_df, env):
    """The datasets the platform builds in `env` and has not retired.

    Mirrors is_dataset_available in airflow-dags (dags/utils.py): a
    `production` dataset is built in every environment, `staging` only in
    staging and development, `development` only in development, and a blank
    environment is not built anywhere. An end-dated dataset is retired
    whatever its environment (e.g. development-plan-document, which is
    production but was end-dated in February).
    """
    available = col("environment") == "production"
    if env in ("staging", "development"):
        available = available | (col("environment") == "staging")
    if env == "development":
        available = available | (col("environment") == "development")

    return (
        dataset_df.filter(available)
        .filter(col("end_date").isNull() | (col("end_date") == ""))
        .select("dataset")
        .distinct()
    )


def _build_provision_quality(
    providers_df, org_df, entity_org_df, lookup_df, entity_quality_df
):
    """Base table: one row per (dataset, organisation) that has an active endpoint
    OR owns entities OR is a detected seeder. Nothing dropped; flags distinguish
    the cases. Owner/provider classification + seeder detection."""
    # map each owned entity to its owner organisation reference
    eq = entity_quality_df.join(
        org_df.select("organisation", "organisation_entity"),
        on="organisation_entity",
        how="inner",
    )

    owner_side = _owner_side(eq)
    active_orgs = org_df.filter(col("org_active")).select("organisation").distinct()
    seeder_side = _seeder_alt_sources(eq, lookup_df, entity_org_df, active_orgs)

    providers = providers_df.select("dataset", "organisation").distinct()
    designated = entity_org_df.select("dataset", "organisation").distinct()

    keys = (
        providers.unionByName(owner_side.select("dataset", "organisation"))
        .unionByName(seeder_side.select("dataset", "organisation"))
        .distinct()
    )

    pq = (
        keys.join(
            providers_df.withColumn("has_active_endpoint", lit(True)),
            on=["dataset", "organisation"],
            how="left",
        )
        .join(owner_side, on=["dataset", "organisation"], how="left")
        .join(seeder_side, on=["dataset", "organisation"], how="left")
        .join(
            designated.withColumn("is_designated_provider", lit(True)),
            on=["dataset", "organisation"],
            how="left",
        )
        .join(
            org_df.select("organisation", "organisation_name").distinct(),
            on="organisation",
            how="left",
        )
    )

    return pq.select(
        "dataset",
        "organisation",
        "organisation_name",
        coalesce(col("has_active_endpoint"), lit(False)).alias("has_active_endpoint"),
        coalesce(col("has_active_resource"), lit(False)).alias("has_active_resource"),
        coalesce(col("owns_entities"), lit(False)).alias("owns_entities"),
        coalesce(col("is_designated_provider"), lit(False)).alias(
            "is_designated_provider"
        ),
        coalesce(col("owner_quality"), col("seeder_quality")).alias("quality"),
        coalesce(col("owned_entity_count"), col("seeder_entity_count"), lit(0)).alias(
            "entity_count"
        ),
        lit(None).cast("double").alias("quality_score"),
    )


def _build_dataset_quality(provision_quality):
    """Rollup per dataset. Only classified (auth/some) rows count; entity total is
    from owned counts so seeded rows don't double-count."""
    classified = provision_quality.filter(col("quality").isNotNull())
    return (
        classified.groupBy("dataset")
        .agg(
            countDistinct(
                when(col("quality") == "authoritative", col("organisation"))
            ).alias("authoritative_organisations"),
            countDistinct(when(col("quality") == "some", col("organisation"))).alias(
                "some_organisations"
            ),
            countDistinct("organisation").alias("total_organisations"),
            spark_sum(
                when(col("owns_entities"), col("entity_count")).otherwise(0)
            ).alias("total_entities"),
        )
        .withColumn("quality_score", lit(None).cast("double"))
    )


def _build_organisation_quality(provision_quality):
    """Rollup per organisation across datasets."""
    classified = provision_quality.filter(col("quality").isNotNull())
    return (
        classified.groupBy("organisation")
        .agg(
            first("organisation_name", ignorenulls=True).alias("organisation_name"),
            countDistinct(
                when(col("quality") == "authoritative", col("dataset"))
            ).alias("authoritative_datasets"),
            countDistinct(when(col("quality") == "some", col("dataset"))).alias(
                "some_datasets"
            ),
            countDistinct("dataset").alias("total_datasets"),
            spark_sum(
                when(col("owns_entities"), col("entity_count")).otherwise(0)
            ).alias("total_entities_owned"),
        )
        .withColumn("quality_score", lit(None).cast("double"))
    )


def _drop_blank_organisations(df):
    """Drop rows with no organisation — a blank org can't key a table
    (organisation is a NOT NULL primary key). Rollups already exclude them."""
    return df.filter(col("organisation").isNotNull() & (col("organisation") != ""))


PROVISION_QUALITY_PG_TYPES = [
    ("dataset", "TEXT"),
    ("organisation", "TEXT"),
    ("organisation_name", "TEXT"),
    ("has_active_endpoint", "BOOLEAN"),
    ("has_active_resource", "BOOLEAN"),
    ("owns_entities", "BOOLEAN"),
    ("is_designated_provider", "BOOLEAN"),
    ("quality", "TEXT"),
    ("entity_count", "BIGINT"),
    ("quality_score", "DOUBLE PRECISION"),
]

DATASET_QUALITY_PG_TYPES = [
    ("dataset", "TEXT"),
    ("authoritative_organisations", "INTEGER"),
    ("some_organisations", "INTEGER"),
    ("total_organisations", "INTEGER"),
    ("total_entities", "BIGINT"),
    ("quality_score", "DOUBLE PRECISION"),
]

ORGANISATION_QUALITY_PG_TYPES = [
    ("organisation", "TEXT"),
    ("organisation_name", "TEXT"),
    ("authoritative_datasets", "INTEGER"),
    ("some_datasets", "INTEGER"),
    ("total_datasets", "INTEGER"),
    ("total_entities_owned", "BIGINT"),
    ("quality_score", "DOUBLE PRECISION"),
]


class ProvisionQualityPipeline(BasePipeline):
    """
    Cross-collection pipeline computing provider/organisation quality per
    (dataset, organisation). Reads across all collections at once (wildcard S3
    paths) like TaskPipeline. Phase 1 writes three CSVs; phase 2 will add Delta
    + Postgres. Classification follows the agreed provider/organisation
    quality definitions (see the Provision Quality technical documentation).
    """

    def execute(self, entity_data_path, output_path):
        spark = self.config.spark
        base = AnyPath(self.config.collection_data_path)

        # -- Active providers (source.csv → who submits) ------------------------
        # Non-empty endpoint, empty end_date; `pipelines` (';'-split) = dataset(s).
        collections = self._collection_names(base)
        source_files = self._collection_files(base, collections, "source.csv")
        logger.info(f"ProvisionQuality: Found {len(source_files)} source files")
        source_df = self._read_csvs_by_name(
            spark, source_files, ["endpoint", "end_date", "organisation", "pipelines"]
        )
        active_sources = (
            source_df.filter(
                (col("endpoint").isNotNull() & (col("endpoint") != ""))
                & (col("end_date").isNull() | (col("end_date") == ""))
            )
            .select(
                explode(split(col("pipelines"), ";")).alias("dataset"),
                col("organisation"),
                col("endpoint"),
            )
            .distinct()
        )

        # -- Active resources (resource.csv → is data still arriving) -----------
        # A resource's end-date is the last date the collector saw it, so blank
        # means it was fetched today. An endpoint can be configured and active
        # while nothing actually arrives. `endpoints` is ';'-joined where
        # several endpoints produced identical content.
        resource_files = self._collection_files(base, collections, "resource.csv")
        logger.info(f"ProvisionQuality: Found {len(resource_files)} resource files")
        resource_df = self._read_csvs_by_name(
            spark, resource_files, ["endpoints", "end_date"]
        )
        delivering_endpoints = (
            resource_df.filter(col("end_date").isNull() | (col("end_date") == ""))
            .select(explode(split(col("endpoints"), ";")).alias("endpoint"))
            .distinct()
        )

        # an organisation is still delivering a dataset if ANY of its active
        # endpoints for it still has a resource arriving
        delivering = (
            active_sources.join(delivering_endpoints, on="endpoint", how="left_semi")
            .select("dataset", "organisation")
            .distinct()
        )
        providers_df = (
            active_sources.select("dataset", "organisation")
            .distinct()
            .join(
                delivering.withColumn("has_active_resource", lit(True)),
                on=["dataset", "organisation"],
                how="left",
            )
        )

        # -- Organisation reference (organisation.csv) --------------------------
        org_path = str(
            base / "organisation-collection" / "dataset" / "organisation.csv"
        )
        org_df = normalise_column_names(
            spark.read.option("header", "true").csv(org_path)
        )
        # org<->entity id, human name, active flag (empty end_date)
        org_df = org_df.select(
            col("organisation"),
            col("entity").alias("organisation_entity"),
            col("name").alias("organisation_name"),
            (col("end_date").isNull() | (col("end_date") == "")).alias("org_active"),
        )

        # -- Config: designated provisions + seeding lookup ---------------------
        config_base = base / "config" / "pipeline"
        eo_files = [str(p) for p in config_base.glob("*/entity-organisation.csv")]
        entity_org_df = self._read_csvs_by_name(
            spark, eo_files, ["dataset", "organisation"]
        ).distinct()  # designated (dataset, org)

        lookup_files = [str(p) for p in config_base.glob("*/lookup.csv")]
        lookup_df = self._read_csvs_by_name(
            spark, lookup_files, ["entity", "organisation"]
        )  # who seeded each entity

        # -- Live datasets (specification/dataset.csv) --------------------------
        # Restrict to datasets the platform still builds in this environment;
        # retired ones linger in the CSVs (e.g. local-plan-timetable) in s3.
        dataset_path = str(base / "specification" / "dataset.csv")
        dataset_df = normalise_column_names(
            spark.read.option("header", "true").csv(dataset_path)
        )
        live_datasets_df = _live_datasets(dataset_df, self.config.env)

        # -- Entity + quality (SWAPPABLE SEAM) ----------------------------------
        entity_quality_df = self.load_entity_quality(spark, entity_data_path)

        # -- Classification + rollups ------------------------------------------
        provision_quality = _build_provision_quality(
            providers_df, org_df, entity_org_df, lookup_df, entity_quality_df
        ).localCheckpoint(
            eager=True
        )  # materialise once AND truncate the huge plan

        # Log what the specification filter removes before applying it, so a
        # dataset disappearing from the output is never silent.
        dropped = (
            provision_quality.select("dataset")
            .distinct()
            .join(live_datasets_df, on="dataset", how="left_anti")
        )
        dropped_names = sorted(row["dataset"] for row in dropped.collect())
        if dropped_names:
            logger.info(
                f"ProvisionQuality: excluding {len(dropped_names)} dataset(s) not live "
                f"in {self.config.env}: {', '.join(dropped_names)}"
            )
        provision_quality = provision_quality.join(
            live_datasets_df, on="dataset", how="left_semi"
        )

        provision_quality = _drop_blank_organisations(provision_quality)

        dataset_quality = _build_dataset_quality(provision_quality)
        organisation_quality = _build_organisation_quality(provision_quality)

        # -- Write (phase 1: CSV) ----------------------------------------------
        self._write_single_csv(
            provision_quality.orderBy(
                col("dataset"),
                lower(col("organisation_name")).asc_nulls_last(),
            ),
            output_path,
            "provision-quality",
        )
        self._write_single_csv(
            dataset_quality.orderBy("dataset"), output_path, "dataset-quality"
        )
        self._write_single_csv(
            organisation_quality.orderBy(lower(col("organisation_name"))),
            output_path,
            "organisation-quality",
        )

        # -- Write Delta (canonical) + Postgres (serving) ----------------------
        outputs = [
            ("provision_quality", provision_quality, PROVISION_QUALITY_PG_TYPES),
            ("dataset_quality", dataset_quality, DATASET_QUALITY_PG_TYPES),
            (
                "organisation_quality",
                organisation_quality,
                ORGANISATION_QUALITY_PG_TYPES,
            ),
        ]
        for name, frame, _ in outputs:
            delta_path = str(AnyPath(self.config.parquet_datasets_path) / name)
            logger.info(f"ProvisionQuality: Writing Delta table to {delta_path}")
            frame.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).save(delta_path)

        if self.config.database_url:
            for name, frame, pg_types in outputs:
                logger.info(f"ProvisionQuality: Writing {name} to Postgres")
                write_table_to_postgres(frame, name, pg_types, self.config.database_url)
        else:
            logger.info(
                "ProvisionQuality: No database_url provided — skipping Postgres writes"
            )

    def _read_csvs_by_name(self, spark, files, columns):
        """Read many CSVs per-file and unionByName on the named columns.

        A single spark.read.csv([...]) maps columns by POSITION and uses one
        file's header for all files, so a file whose column SET differs — e.g.
        prod tree-preservation-order's source.csv is missing the leading
        `source` column — gets shifted (its `pipelines` value lands in the
        `organisation` slot, producing phantom provider rows). Reading each
        file against its OWN header and selecting by name removes that risk.
        Mirrors load_entity_quality's guard.
        """
        frames = []
        for f in files:
            df = normalise_column_names(spark.read.option("header", "true").csv(f))
            if not all(c in df.columns for c in columns):
                logger.warning(f"ProvisionQuality: {f} missing {columns} — skipping")
                continue
            frames.append(df.select(*columns))
        if not frames:
            raise ValueError(f"No usable CSVs with columns {columns}")
        return reduce(lambda a, b: a.unionByName(b), frames)

    def _collection_names(self, base):
        """Top-level {collection}-collection folder names via one delimited
        listing. glob() with a '/' in the pattern lists the whole bucket and
        filters client-side; iterdir() is a single Delimiter='/' call."""
        return sorted(p.name for p in base.iterdir() if p.name.endswith("-collection"))

    def _collection_files(self, base, collections, filename):
        """Build the known {collection}/collection/{filename} paths directly and
        keep the ones that exist, avoiding a full-bucket glob per file."""
        paths = [base / c / "collection" / filename for c in collections]
        return [str(p) for p in paths if p.exists()]

    def load_entity_quality(self, spark, entity_data_path):
        """SWAPPABLE SEAM. Phase 1: read the flattened per-dataset entity CSVs
        (one {dataset}.csv each) and return (dataset, organisation_entity,
        quality, entity). Read per file + union because each dataset's flattened
        CSV has its own column set — a single multi-file read would misalign
        headers. Future: swap the body to read the per-dataset Delta tables."""
        entity_files = [str(p) for p in AnyPath(entity_data_path).glob("*.csv")]
        logger.info(f"ProvisionQuality: Found {len(entity_files)} entity CSVs")
        frames = []
        for f in entity_files:
            dataset = AnyPath(f).stem
            df = spark.read.option("header", "true").csv(f)
            if "organisation-entity" not in df.columns or "quality" not in df.columns:
                logger.warning(
                    f"ProvisionQuality: {dataset} flattened CSV missing "
                    "organisation-entity/quality — skipping"
                )
                continue
            frames.append(
                df.select(
                    col("entity"),
                    col("`organisation-entity`").alias("organisation_entity"),
                    col("quality"),
                ).withColumn("dataset", lit(dataset))
            )
        if not frames:
            raise ValueError(f"No usable entity CSVs found under {entity_data_path}")
        return reduce(lambda a, b: a.unionByName(b), frames)

    def _write_single_csv(self, df, output_path, name):
        """Write df as a single header CSV at output_path/name.csv.

        Spark writes a directory of part-files, so we coalesce(1), then move the
        one part-file to the target name. The outputs are small aggregates
        (hundreds/thousands of rows), so a driver-side move is fine.
        """
        tmp_dir = AnyPath(output_path) / f"_tmp_{name}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            str(tmp_dir)
        )
        part = next(p for p in tmp_dir.glob("part-*.csv"))
        target = AnyPath(output_path) / f"{name}.csv"
        target.write_bytes(part.read_bytes())
        for p in tmp_dir.glob("*"):
            p.unlink()
        logger.info(f"ProvisionQuality: Wrote {target}")
