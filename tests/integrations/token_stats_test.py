from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeTokenStats(Mechanism):

    def test_token_stats(self):
        self.launch(analyzer='token_stats', start_date='2017-01-15', end_date='2017-01-17')
        result = self.get_data(table_name='token_stats',
                               columns=['token', 'request_date', 'nb_req'])

        expected_results = [
            ('token:2', datetime(2017, 1, 15, 0, 0), 1),
            ('token:2', datetime(2017, 1, 16, 0, 0), 1),
            ('token:3', datetime(2017, 1, 15, 0, 0), 6),
            ('token:1', datetime(2017, 1, 15, 0, 0), 2),
            ('token:1', datetime(2017, 1, 16, 0, 0), 2),
        ]

        assert same_list_tuple(result, expected_results)
        assert self.partitionned_table_exists('token_stats_y2017m01')
