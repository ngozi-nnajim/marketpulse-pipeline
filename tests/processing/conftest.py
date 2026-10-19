"""Shared fixtures for Spark tests (run inside the Spark Docker image)."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    """Provide one small local Spark session shared by all tests.

    scope="session" creates it once, because starting Spark takes seconds.
    """
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("marketpulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
