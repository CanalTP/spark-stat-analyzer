import pytest
from datetime import date, datetime
from analyzers.coverage_journeys_duration import AnalyzeCoverageJourneysDuration
import os

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys_duration"


def test_count_journeys(spark):
    start_date = date(2017, 3, 14)
    end_date = date(2017, 3, 14)
    expected_results = [
        (datetime.utcfromtimestamp(1489496455).date(), u'fr-foo', 0, 0, 1),
        (datetime.utcfromtimestamp(1489496455).date(), u'fr-foo', 0, 1, 2),
        (datetime.utcfromtimestamp(1489496455).date(), u'fr-foo', 0, 2, 1),
        (datetime.utcfromtimestamp(1489496455).date(), u'fr-foo', 0, 22, 1),
    ]

    analyzer = AnalyzeCoverageJourneysDuration(storage_path=path,
                                               start_date=start_date,
                                               end_date=end_date,
                                               spark_session=spark,
                                               database=None,
                                               current_datetime=datetime(2017, 3, 14, 15, 10))
    results = analyzer.get_data(rdd_mode=True)

    assert len(results) == len(expected_results)
    for result in results:
        assert result in expected_results

    assert analyzer.get_log_analyzer_stats(datetime(2017, 3, 14, 15, 12)) == \
           "[OK] [2017-03-14 15:12:00] [2017-03-14 15:10:00] [CoverageJourneysDuration] [120]"
