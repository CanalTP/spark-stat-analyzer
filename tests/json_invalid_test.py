import pytest
from datetime import date, datetime
from analyzers import AnalyzeCoverageJourneys,AnalyzeErrors
import os

pytestmark = pytest.mark.usefixtures("spark")


def test_json_invalid_with_rdd(spark):
    path = os.getcwd() + "/tests/fixtures/json_invalid"
    start_date = date(2017, 1, 1)
    end_date = date(2017, 1, 1)

    analyzer = AnalyzeCoverageJourneys(storage_path=path, start_date=start_date, end_date=end_date,
                                                   spark_session=spark, database=None,
                                                   current_datetime=datetime(2017, 2, 15, 15, 10))
    files = analyzer.get_files_to_analyze()

    expected_files = [path + '/2017/01/01/json_invalid.json.log']
    
    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0

    results = analyzer.get_data(rdd_mode=True)
    print(results)
    
def test_json_invalid_without_rdd(spark):
    path = os.getcwd() + "/tests/fixtures/json_invalid"
    start_date = date(2017, 1, 1)
    end_date = date(2017, 1, 1)

    analyzer = AnalyzeErrors(storage_path=path, start_date=start_date, end_date=end_date,
                                                   spark_session=spark, database=None,
                                                   current_datetime=datetime(2017, 2, 15, 15, 10))
    files = analyzer.get_files_to_analyze()

    expected_files = [path + '/2017/01/01/json_invalid.json.log']
    
    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0

    results = analyzer.get_data()
    print(results)
    

    

