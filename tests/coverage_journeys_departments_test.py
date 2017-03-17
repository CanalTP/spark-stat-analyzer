import pytest
from datetime import date, datetime
from analyzers.coverage_journeys_departments import AnalyzeCoverageJourneysDepartments
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys_departments"


def test_count_journeys(spark):
    start_date = date(2017, 3, 16)
    end_date = date(2017, 3, 16)
    expected_results = [
        (datetime.utcfromtimestamp(1489670672).date(), u'fr-foo', 0, "94", "75", 1),
        (datetime.utcfromtimestamp(1489670672).date(), u'fr-foo', 0, "75", "75", 1),
        (datetime.utcfromtimestamp(1489670672).date(), u'fr-foo', 0, "", "75", 1),
        (datetime.utcfromtimestamp(1489670672).date(), u'fr-foo', 0, "94", "", 2),
        (datetime.utcfromtimestamp(1489670672).date(), u'fr-foo', 0, "75", "29", 2),
        (datetime.utcfromtimestamp(1489670672).date(), u'fr-foo', 0, "", "", 2),
    ]

    analyzer = AnalyzeCoverageJourneysDepartments(storage_path=path,
                                                  start_date=start_date,
                                                  end_date=end_date,
                                                  spark_session=spark,
                                                  database=None,
                                                  current_datetime=datetime(2017, 3, 16, 15, 10))
    results = analyzer.get_data(rdd_mode=True)

    assert len(results) == len(expected_results)
    for result in results:
        assert result in expected_results

    assert analyzer.get_log_analyzer_stats(datetime(2017, 3, 16, 15, 12)) == \
           "[OK] [2017-03-16 15:12:00] [2017-03-16 15:10:00] [CoverageJourneysDepartments] [120]"
