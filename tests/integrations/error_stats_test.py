from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeErrorStats(Mechanism):

    def test_error_stats(self):
        self.launch(analyzer='error_stats', start_date='2017-01-15', end_date='2017-01-15')
        result = self.get_data(table_name='error_stats',
                               columns=['region_id', 'api', 'user_id', 'app_name',
                                        'is_internal_call', 'request_date', 'err_id',
                                        'nb_req'])

        expected_results = [
            ('fr-idf', 'v1.some_api', 42, 'my_app', 1, datetime(2017, 1, 17, 0, 0), 'some_error_id', 1),
            ('fr-foo', 'v1.some_api', 42, 'my_app', 0, datetime(2017, 1, 17, 0, 0), 'some_error_id', 1),
            ('fr-idf', 'v1.some_api', 42, 'my_app', 0, datetime(2017, 1, 17, 0, 0), 'some_error_id', 3),
            ('fr-idf', 'v1.another_api', 42, 'my_app', 0, datetime(2017, 1, 17, 0, 0), 'some_error_id', 1),
            ('fr-idf', 'v1.some_api', 43, 'my_app', 0, datetime(2017, 1, 17, 0, 0), 'some_error_id', 1),
            ('fr-idf', 'v1.some_api', 42, 'my_app', 0, datetime(2017, 1, 17, 0, 0), 'another_error_id', 1),
            ('fr-idf', 'v1.some_api', 42, 'another_app', 0, datetime(2017, 1, 17, 0, 0), 'some_error_id', 1),
            ('fr-idf', 'v1.some_api', 42, 'my_app', 0, datetime(2013, 11, 16, 0, 0), 'some_error_id', 1),
        ]

        assert same_list_tuple(result, expected_results)
        assert self.partitionned_table_exists('error_stats_y2017m01')
