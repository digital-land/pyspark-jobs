from __future__ import annotations

import json
import logging
from typing import List, Optional

from delta.tables import DeltaTable
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

logger = logging.getLogger(__name__)

# Mapping from generic datatype strings to Spark-compatible type strings
_SPARK_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "integer": "int",
    "long": "bigint",
    "bigint": "bigint",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}


class FieldSchema(BaseModel):
    field: str
    name: Optional[str] = None  # Human-readable label, e.g. "Entry Date"
    datatype: str
    nullable: bool = True

    def to_spark_type(self) -> str:
        """Convert the datatype to a Spark-compatible type string."""
        return _SPARK_TYPE_MAP.get(self.datatype.lower(), self.datatype)


class SchemaDiff(BaseModel):
    """The difference between a registered schema and an existing Delta table schema."""

    added: List[FieldSchema] = []
    removed: List[str] = []
    type_changed: List[tuple] = []  # list of (field_name, old_type, new_type)

    @property
    def is_empty(self) -> bool:
        return not self.added and not self.removed and not self.type_changed

    @property
    def has_destructive_changes(self) -> bool:
        return bool(self.removed or self.type_changed)


class DatasetSchema(BaseModel):
    name: str
    fields: List[FieldSchema]

    @classmethod
    def from_json(cls, path: str) -> DatasetSchema:
        with open(path) as f:
            return cls.model_validate(json.load(f))

    def enforce(self, df: DataFrame) -> DataFrame:
        """Add missing fields as typed nulls and select schema fields in order."""
        for field_spec in self.fields:
            if field_spec.field not in df.columns:
                df = df.withColumn(
                    field_spec.field, lit(None).cast(field_spec.to_spark_type())
                )
        return df.select([f.field for f in self.fields])

    def diff(self, spark: SparkSession, table_path: str) -> SchemaDiff:
        """
        Compare this schema against an existing Delta table.

        Returns a SchemaDiff describing what has changed. Ignores
        processed_timestamp as it is managed by the transformers, not the schema.
        """
        if not DeltaTable.isDeltaTable(spark, table_path):
            return SchemaDiff()

        current = {
            f.name: f.dataType.simpleString()
            for f in DeltaTable.forPath(spark, table_path).toDF().schema.fields
            if f.name != "processed_timestamp"
        }
        desired = {f.field: f.to_spark_type() for f in self.fields}

        added = [f for f in self.fields if f.field not in current]
        removed = [name for name in current if name not in desired]
        type_changed = [
            (name, current[name], desired[name])
            for name in desired
            if name in current and current[name] != desired[name]
        ]

        return SchemaDiff(added=added, removed=removed, type_changed=type_changed)

    def migrate(
        self, spark: SparkSession, table_path: str, allow_destructive: bool = False
    ) -> dict:
        """
        Migrate an existing Delta table to match this schema.

        Safe changes (adding columns) are always applied.
        Destructive changes (removing columns, type changes) require
        allow_destructive=True. When skipped, they are logged as warnings
        so the operator knows what is pending.

        Returns a summary dict of what was applied and what was skipped.
        """
        summary = {"added": [], "removed": [], "type_changed": [], "skipped": []}

        diff = self.diff(spark, table_path)

        if diff.is_empty:
            logger.info(f"migrate: {self.name} — no changes needed")
            return summary

        # Always safe: add new columns
        for field_spec in diff.added:
            spark.sql(
                f"ALTER TABLE delta.`{table_path}` "
                f"ADD COLUMN (`{field_spec.field}` {field_spec.to_spark_type()})"
            )
            logger.info(f"migrate: {self.name} — added column '{field_spec.field}'")
            summary["added"].append(field_spec.field)

        if diff.has_destructive_changes:
            if not allow_destructive:
                skipped = [name for name in diff.removed] + [
                    name for name, _, _ in diff.type_changed
                ]
                for name in skipped:
                    logger.warning(
                        f"migrate: {self.name} — skipping destructive change on "
                        f"'{name}' (re-run with allow_destructive=True to apply)"
                    )
                summary["skipped"] = skipped
                return summary

            # Destructive: handle removes and type changes in a single rewrite
            df = spark.read.format("delta").load(table_path)

            for field_name, old_type, new_type in diff.type_changed:
                logger.info(
                    f"migrate: {self.name} — casting '{field_name}' "
                    f"from {old_type} to {new_type}"
                )
                df = df.withColumn(field_name, col(field_name).cast(new_type))
                summary["type_changed"].append(field_name)

            # Select only desired columns plus processed_timestamp (drops removed ones)
            keep = [f.field for f in self.fields]
            if "processed_timestamp" in df.columns:
                keep.append("processed_timestamp")
            df = df.select(keep)

            for name in diff.removed:
                logger.info(f"migrate: {self.name} — removing column '{name}'")
                summary["removed"].append(name)

            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).partitionBy("dataset").save(table_path)

        return summary


_REGISTRY: dict[str, DatasetSchema] = {}


def register(schema: DatasetSchema) -> DatasetSchema:
    """Register a schema by name. Returns the schema for convenience."""
    _REGISTRY[schema.name] = schema
    return schema


def get_schema(name: str) -> DatasetSchema:
    """Retrieve a registered schema by name."""
    if name not in _REGISTRY:
        raise KeyError(f"No schema registered for '{name}'")
    return _REGISTRY[name]


register(
    DatasetSchema(
        name="entity",
        fields=[
            FieldSchema(field="dataset", name="Dataset", datatype="string"),
            FieldSchema(field="end_date", name="End Date", datatype="date"),
            FieldSchema(field="entity", name="Entity", datatype="bigint"),
            FieldSchema(field="entry_date", name="Entry Date", datatype="date"),
            FieldSchema(field="geometry", name="Geometry", datatype="string"),
            FieldSchema(field="json", name="JSON", datatype="string"),
            FieldSchema(field="name", name="Name", datatype="string"),
            FieldSchema(
                field="organisation_entity",
                name="Organisation Entity",
                datatype="bigint",
            ),
            FieldSchema(field="point", name="Point", datatype="string"),
            FieldSchema(field="prefix", name="Prefix", datatype="string"),
            FieldSchema(field="quality", name="Quality", datatype="string"),
            FieldSchema(field="reference", name="Reference", datatype="string"),
            FieldSchema(field="start_date", name="Start Date", datatype="date"),
            FieldSchema(field="typology", name="Typology", datatype="string"),
        ],
    )
)

register(
    DatasetSchema(
        name="fact",
        fields=[
            FieldSchema(field="dataset", name="Dataset", datatype="string"),
            FieldSchema(field="end_date", name="End Date", datatype="string"),
            FieldSchema(field="entity", name="Entity", datatype="bigint"),
            FieldSchema(field="entry_date", name="Entry Date", datatype="string"),
            FieldSchema(field="fact", name="Fact", datatype="string"),
            FieldSchema(field="field", name="Field", datatype="string"),
            FieldSchema(field="priority", name="Priority", datatype="integer"),
            FieldSchema(
                field="reference_entity", name="Reference Entity", datatype="bigint"
            ),
            FieldSchema(field="start_date", name="Start Date", datatype="string"),
            FieldSchema(field="value", name="Value", datatype="string"),
        ],
    )
)

register(
    DatasetSchema(
        name="fact_resource",
        fields=[
            FieldSchema(field="dataset", name="Dataset", datatype="string"),
            FieldSchema(field="end_date", name="End Date", datatype="string"),
            FieldSchema(field="entry_date", name="Entry Date", datatype="string"),
            FieldSchema(field="entry_number", name="Entry Number", datatype="string"),
            FieldSchema(field="fact", name="Fact", datatype="string"),
            FieldSchema(field="priority", name="Priority", datatype="integer"),
            FieldSchema(field="resource", name="Resource", datatype="string"),
            FieldSchema(field="start_date", name="Start Date", datatype="string"),
        ],
    )
)

register(
    DatasetSchema(
        name="dataset_resource",
        fields=[
            FieldSchema(field="dataset", name="Dataset", datatype="string"),
            FieldSchema(field="entry_date", name="Entry Date", datatype="string"),
            FieldSchema(field="entity_count", name="Entity Count", datatype="integer"),
            FieldSchema(field="entry_count", name="Entry Count", datatype="integer"),
            FieldSchema(field="line_count", name="Line Count", datatype="integer"),
            FieldSchema(field="mime_type", name="Mime Type", datatype="string"),
            FieldSchema(field="internal_path", name="Internal Path", datatype="string"),
            FieldSchema(
                field="internal_mime_type", name="Internal Mime Type", datatype="string"
            ),
            FieldSchema(field="resource", name="Resource", datatype="string"),
        ],
    )
)

register(
    DatasetSchema(
        name="column_field",
        fields=[
            FieldSchema(field="dataset", name="Dataset", datatype="string"),
            FieldSchema(field="entry_date", name="Entry Date", datatype="string"),
            FieldSchema(field="field", name="Field", datatype="string"),
            FieldSchema(field="resource", name="Resource", datatype="string"),
            FieldSchema(field="column", name="Column", datatype="string"),
        ],
    )
)

register(
    DatasetSchema(
        name="issue",
        fields=[
            FieldSchema(field="end_date", name="End Date", datatype="string"),
            FieldSchema(field="entity", name="Entity", datatype="bigint"),
            FieldSchema(field="entry_date", name="Entry Date", datatype="string"),
            FieldSchema(field="entry_number", name="Entry Number", datatype="string"),
            FieldSchema(field="field", name="Field", datatype="string"),
            FieldSchema(field="issue_type", name="Issue Type", datatype="string"),
            FieldSchema(field="line_number", name="Line Number", datatype="string"),
            FieldSchema(field="dataset", name="Dataset", datatype="string"),
            FieldSchema(field="resource", name="Resource", datatype="string"),
            FieldSchema(field="start_date", name="Start Date", datatype="string"),
            FieldSchema(field="value", name="Value", datatype="string"),
            FieldSchema(field="message", name="Message", datatype="string"),
        ],
    )
)
