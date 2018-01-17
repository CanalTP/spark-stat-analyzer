from includes.database import Database
import sys, os
import config
import argparse
from pyspark.sql import SparkSession
from includes import utils, logger
from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("-i", "--input", help="Directory.", required=True, type=utils.check_and_get_path)
    parser.add_argument("-s", "--start_date", help="The start date - format YYYY-MM-DD.",
                        type=utils.date_format, required=True)
    parser.add_argument("-e", "--end_date", help="The end date - format YYYY-MM-DD.",
                        type=utils.date_format, required=True)
    parser.add_argument("-a", "--analyzer", help="Analyzer name.",
                        required=True, type=utils.analyzer_value)

    args = parser.parse_args()
    status = 'OK'
    exit_code = os.EX_OK
    spark_session = SparkSession.builder.appName(__file__).getOrCreate()
    sc = spark_session.sparkContext
    try:
        sc.setLogLevel(config.logger.get("spark_level", "ERROR"))
        logger.init_basic_logger(config.logger.get("level", ""))

        database = Database(dbname=config.db["dbname"], user=config.db["user"],
                            password=config.db["password"], schema=config.db["schema"],
                            host=config.db['host'], port=config.db['port'],
                            insert_count=config.db['insert_count'],
                            auto_connect=False,
                            error_logger=logger.get_spark_logger(spark_session.sparkContext),
                            info_logger=logger.get_basic_logger())

        analyzer = args.analyzer(args.input, args.start_date, args.end_date, spark_session, database)
        try:
            analyzer.launch()
        except Exception as e:
            logger.get_spark_logger(sc).error("Error({type}): {msg}".format(type=type(e), msg=str(e)))
            status = 'KO'
            exit_code = os.EX_SOFTWARE
        finally:
            analyzer.terminate(datetime.now(), status)
    except Exception as e:
        logger.get_spark_logger(sc).error("Error({type}): {msg}".format(type=type(e), msg=str(e)))
        exit_code =  os.EX_CONFIG

    sys.exit(exit_code)
