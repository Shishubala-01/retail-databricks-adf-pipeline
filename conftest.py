import sys
import os

# Make src/ importable as "quality_checks" without installing the project as a package -
# simplest option for a project this size.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    One local Spark session shared across the whole test run (session-scoped) -
    starting a new one per test would make the suite painfully slow, since each
    SparkSession has real startup overhead even running locally.
    local[1] = run with a single thread, which is all a unit test needs and
    keeps output deterministic (no surprise parallelism reordering rows).
    """
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("quality-checks-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()
