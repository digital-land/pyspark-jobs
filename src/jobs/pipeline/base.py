"""Base classes shared by every pipeline."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import SparkSession

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
