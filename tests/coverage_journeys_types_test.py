import pytest
from datetime import date, datetime
from analyzers.coverage_journeys_types import AnalyzeCoverageJourneysTypes
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys_types"

def test_count_journeys_types_20170120(spark):
    start_date = date(2017, 1, 20)
    end_date = date(2017, 1, 20)
    expected_results = [
        (datetime.utcfromtimestamp(1484924800).date(), u'fr-foo', u'address', u'poi', 1),
        (datetime.utcfromtimestamp(1484924800).date(), u'fr-foo', u'stop_area', u'poi', 2),
        (datetime.utcfromtimestamp(1484924800).date(), u'fr-foo', u'stop_point', u'stop_area', 3)
    ]

    analyzer = AnalyzeCoverageJourneysTypes(storage_path=path,
                                            start_date=start_date,
                                            end_date=end_date,
                                            spark_session=spark,
                                            database=None,
                                            current_datetime=datetime(2017, 3, 15, 15, 10))
    results = analyzer.get_data(rdd_mode=True)

    assert len(results) == len(expected_results)
    for result in results:
        assert result in expected_results

    assert analyzer.get_log_analyzer_stats(datetime(2017, 3, 15, 15, 12)) == \
           "[OK] [2017-03-15 15:12:00] [2017-03-15 15:10:00] [CoverageJourneysTypes] [120]"

def test_count_journeys_types_20190511(spark):
    start_date = date(2019, 5, 11)
    end_date = date(2019, 5, 11)
    expected_results = [
        (datetime.utcfromtimestamp(1557532800).date(), u'fr-foo', u'stop_point', u'..\\windows\\win.inistop_area', 1)
    ]

    analyzer = AnalyzeCoverageJourneysTypes(storage_path=path,
                                            start_date=start_date,
                                            end_date=end_date,
                                            spark_session=spark,
                                            database=None,
                                            current_datetime=datetime(2019, 5, 11, 0, 0))
    results = analyzer.get_data(rdd_mode=True)

    assert len(results) == len(expected_results)
    for result in results:
        assert result in expected_results

    assert analyzer.get_log_analyzer_stats(datetime(2019, 5, 11, 0, 0)) == \
           "[OK] [2019-05-11 00:00:00] [2019-05-11 00:00:00] [CoverageJourneysTypes] [0]"
