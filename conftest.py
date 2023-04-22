import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def f_current_date_mock():
    import pyspark.sql.functions as f
    old_current_date_f = f.current_date
    f.current_date = lambda: f.to_date(f.lit("2020-01-01"))
    yield
    f.current_date = old_current_date_f

