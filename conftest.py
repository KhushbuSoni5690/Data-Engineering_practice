from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from common.spark_session import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = get_spark("PipelineTests")
    session.conf.set("spark.sql.shuffle.partitions", "2")

    try:
        yield session
    finally:
        session.stop()