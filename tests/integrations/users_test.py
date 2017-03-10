from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeUsers(Mechanism):

    def test_users(self):
        self.launch(analyzer='users', start_date='2017-01-15', end_date='2017-01-17')
        result = self.get_data(table_name='users', columns=['id', 'user_name', 'date_first_request'])
        expected_results = [
            ('15', 'New Bobby', datetime(2017, 1, 15, 8, 12, 10)),
            ('42', 'Kenny', datetime(2017, 1, 15, 5, 56, 10)),
            ('666', 'New Billy', datetime(2017, 1, 15, 8, 12, 10)),
            ('45', 'Paul', datetime(2017, 1, 17, 2, 56, 10))
        ]

        assert same_list_tuple(result, expected_results)
