import pytest
import findspark
findspark.init()
from pyspark.sql import SparkSession
import config

@pytest.fixture(scope="session", autouse=True)
def spark(request):
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.sparkContext.stop())
    spark.sparkContext.setLogLevel('WARN')

    return spark
