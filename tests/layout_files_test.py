import pytest
from datetime import date, datetime
from analyzers import Analyzer
import os

pytestmark = pytest.mark.usefixtures("spark")

def test_layout_by_date(spark):
    path = os.getcwd() + "/tests/fixtures/layout_by_date"
    start_date = date(2017, 1, 1)
    end_date = date(2017, 1, 1)

    analyzer = Analyzer(storage_path=path, start_date=start_date, end_date=end_date,
                                     spark_session=spark, database=None, current_datetime=datetime(2017, 2, 15, 15, 10))

    files = analyzer.get_files_to_analyze()

    expected_files = [path + '/2017/01/01/layout_by_date.json.log']

    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0
    for fic in expected_files:
        assert fic in files

    

def test_layout_by_coverage(spark):
    path = os.getcwd() + "/tests/fixtures/layout_by_coverage"
    start_date = date(2017, 1, 15)
    end_date = date(2017, 1, 15)
    
    analyzer = Analyzer(storage_path=path, start_date=start_date, end_date=end_date,
                                                   spark_session=spark, database=None,
                                                   current_datetime=datetime(2017, 2, 15, 15, 10))
  
    files = analyzer.get_files_to_analyze()
    
    
    expected_files = [path + '/npdc/2017/01/npdc_20170115.json.log.gz',
                      path + '/auv/2017/01/auv_20170115.json.log.gz',
                      path + '/fr-idf/2017/01/fr-idf_20170115.json.log.gz',
                      path + '/fr-foo/2017/01/fr-foo_20170115.json.log.gz'
                      ]

    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0
    for fic in expected_files:
        assert fic in files
