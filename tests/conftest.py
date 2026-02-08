import os
import sys

import pytest
from sedona.spark import SedonaContext


@pytest.fixture(scope="session")
def spark():
    """Create a local Sedona-enabled Spark session shared across all tests."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SedonaContext.builder()
        .master("local[1]")
        .appName("TestSession")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1",
        )
        .getOrCreate()
    )
    sedona = SedonaContext.create(session)
    yield sedona
    session.stop()
