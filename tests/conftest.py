import pytest
import findspark
findspark.init()
from pyspark.sql import SparkSession
import logging
from tests.integrations.utils import init_database, init_schema
from includes.logger import init_logger, get_logger
import config


def pytest_addoption(parser):
    parser.addoption("--create_database", action="store_true", help="skip create database")


@pytest.fixture(scope="session", autouse=True)
def spark(request):
    spark = SparkSession.builder \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    request.addfinalizer(lambda: spark.sparkContext.stop())

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
    init_logger(config.logger.get("level", ""))

    msg = '--- skip database creating ---'
    if request.config.getvalue("create_database"):
        msg = '--- Create database ---'
        init_database()
        init_schema()

    get_logger().debug(msg)

    return spark
