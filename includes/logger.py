def get_logger(sparkContext):
    return sparkContext._jvm.org.apache.log4j.LogManager.getLogger("spark-stat-analyzer")
