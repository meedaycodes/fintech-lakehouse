import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# tests/ is a sibling of src/, not inside it.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("chip-lakehouse-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
