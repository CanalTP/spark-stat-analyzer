import pytest
from datetime import date, datetime
from analyzers import AnalyzeCoverageJourneyAnticipations
import os

pytestmark = pytest.mark.usefixtures("spark")


def test_coverage_journey_anticipations(spark):
    path = os.getcwd() + "/tests/fixtures/coverage_journey_anticipations"
    start_date = date(2017, 1, 16)
    end_date = date(2017, 1, 16)

    analyzer = AnalyzeCoverageJourneyAnticipations(storage_path=path, start_date=start_date, end_date=end_date,
                                                   spark_session=spark, database=None,
                                                   current_datetime=datetime(2017, 2, 15, 15, 10))

    files = analyzer.get_files_to_analyze()

    expected_files = [path + '/2017/01/16/coverage_journey_anticipations.json.log']

    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0

    results = analyzer.get_data(rdd_mode=True)
    expected_results = [('transilien', 2, 0, date(2017, 1, 16), 1),
                        ('transilien', 4, 0, date(2017, 1, 16), 1),
                        ('transilien', 5, 1, date(2017, 1, 16), 1),
                        ('transilien', 5, 0, date(2017, 1, 16), 2),
                        ('transilien', 1, 0, date(2017, 1, 16), 1),
                        ('fr-cen', 5, 1, date(2017, 1, 16), 1),
                        ('transilien', 3, 0, date(2017, 1, 16), 2)]
    assert len(results) == len(expected_results)
    assert results == expected_results
    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [CoverageJourneyAnticipations] [120]"
