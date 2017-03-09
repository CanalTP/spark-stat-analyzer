import pytest
import findspark
findspark.init()
from pyspark.sql import SparkSession
import logging
from includes.logger import init_logger, get_logger
import config

@pytest.fixture(scope="session", autouse=True)
def spark(request):
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.sparkContext.stop())

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
    init_logger(config.logger.get("level", ""))

    return spark
