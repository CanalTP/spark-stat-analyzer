import pytest
from datetime import date, datetime
from analyzers import AnalyseUsers
import os

pytestmark = pytest.mark.usefixtures("spark")


def test_users(spark):
    path = os.getcwd() + "/tests/fixtures/users"
    start_date = date(2017, 1, 15)
    end_date = date(2017, 1, 15)

    analyzer = AnalyseUsers(storage_path=path, start_date=start_date, end_date=end_date,
                               spark_session=spark, database=None)

    files = analyzer.get_files_to_analyze()

    expected_files = [path + '/2017/01/15/users.json.log']

    assert len(files) == len(expected_files)
    assert len(set(files) - set(expected_files)) == 0

    results = analyzer.get_data()

    expected_results = {666: {'user_name': u'Billy', 'first_date': 1484467930},
                        42: {'user_name': u'Kenny last name', 'first_date': 1484459770},
                        15: {'user_name': u'Bobby new name', 'first_date': 1484467930},
                        0: {'user_name': u'unknown', 'first_date': 1484469930}}
    assert len(results) == len(expected_results)
    formatted_results = {elt.user_id: {"user_name": elt.user_name, "first_date": elt.first_date}
                         for elt in results}
    for row_id, row in formatted_results.items():
        assert row == expected_results[row_id]


def test_users_empty_file(spark):
    path = os.getcwd() + "/tests/fixtures/users"
    start_date = date(2017, 1, 16)
    end_date = date(2017, 1, 16)
    analyzer = AnalyseUsers(storage_path=path, start_date=start_date, end_date=end_date,
                               spark_session=spark, database=None, current_datetime=datetime(2017, 2, 15, 15, 10))

    files = analyzer.get_files_to_analyze()
    expected_files = [path + '/2017/01/16/users.json.log']
    assert len(files) == len(expected_files)
    results = analyzer.get_data()
    assert len(results) == 0
    assert analyzer.get_log_analyzer_stats(datetime(2017, 2, 15, 15, 12)) == \
           "[OK] [2017-02-15 15:12:00] [2017-02-15 15:10:00] [UsersUpdater] [120]"
