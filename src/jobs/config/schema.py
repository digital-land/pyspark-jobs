from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

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
