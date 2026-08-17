"""
Pipeline classes for ETL processes.

Each pipeline class is responsible for extracting, transforming, and loading
data. BasePipeline (base.py) enforces the correct entry point (run) and
automatically tracks pipeline performance metrics (timing, status). Each
pipeline lives in its own module; this package re-exports the public classes
and constants so callers can `from jobs.pipeline import EntityPipeline` etc.
without needing to know which submodule a given pipeline lives in.

Transform, extract/read and load/write functions should be defined outside
of this package and tested independently.
"""

from jobs.pipeline.base import BasePipeline, PipelineConfig
from jobs.pipeline.column_field import ColumnFieldPipeline
from jobs.pipeline.dataset_resource import DatasetResourcePipeline
from jobs.pipeline.entity import EntityPipeline
from jobs.pipeline.issue import IssuePipeline
from jobs.pipeline.provision_quality import (
    DATASET_QUALITY_PG_TYPES,
    ORGANISATION_QUALITY_PG_TYPES,
    PROVISION_QUALITY_PG_TYPES,
    ProvisionQualityPipeline,
)
from jobs.pipeline.task import TaskPipeline

__all__ = [
    "BasePipeline",
    "PipelineConfig",
    "ColumnFieldPipeline",
    "DatasetResourcePipeline",
    "EntityPipeline",
    "IssuePipeline",
    "ProvisionQualityPipeline",
    "TaskPipeline",
    "PROVISION_QUALITY_PG_TYPES",
    "DATASET_QUALITY_PG_TYPES",
    "ORGANISATION_QUALITY_PG_TYPES",
]
