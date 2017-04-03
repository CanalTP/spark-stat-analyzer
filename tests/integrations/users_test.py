from tests.integrations.mechanism import Mechanism
from datetime import datetime

columns = ['id', 'user_name', 'date_first_request']


class TestAnalyzeUsers(Mechanism):
    def test_users(self):
        self.launch(analyzer='users', start_date='2017-01-15', end_date='2017-01-17')
        result = self.get_data(table_name='users', columns=columns)
        expected_results = [
            (15, 'New Bobby', datetime(2017, 1, 15, 8, 12, 10)),
            (42, 'Kenny', datetime(2017, 1, 15, 5, 56, 10)),
            (666, 'New Billy', datetime(2017, 1, 15, 8, 12, 10)),
            (45, 'Paul', datetime(2017, 1, 17, 2, 56, 10))
        ]

        assert result == expected_results

    def test_users_no_update_on_null_date(self):
        self.launch_migration_only()
        self.insert_data('users', columns, [(42, 'Kenny', None)])
        self.launch(analyzer='users', start_date='2017-01-15', end_date='2017-01-15')
        result = self.get_data(table_name='users', columns=columns)
        expected_results = [(42, 'Kenny last name', None),
                            (15, 'Bobby new name', datetime(2017, 1, 15, 8, 12, 10)),
                            (666, 'Billy', datetime(2017, 1, 15, 8, 12, 10))]

        assert result == expected_results
