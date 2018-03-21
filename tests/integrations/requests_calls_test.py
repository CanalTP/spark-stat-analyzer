from tests.integrations.mechanism import Mechanism
from datetime import datetime


class TestAnalyzeRequestsCalls(Mechanism):

    def test_requests_calls(self):
        self.launch(analyzer='requests_calls', start_date='2017-01-01', end_date='2017-01-02')
        result = self.get_data(table_name='requests_calls',
                               columns=['region_id', 'api', 'user_id', 'app_name', 'is_internal_call', 'request_date',
                                        'end_point_id', 'nb', 'nb_without_journey', 'object_count'])

        expected_results = [
            ('fr-cen', 'v1.status', 0, '', 0, datetime(2017, 1, 1, 0, 0), 1, 2, 2, 0),
            ('fr-auv', 'v1.journeys', 22, 'test filbleu', 0, datetime(2017, 1, 1, 0, 0), 1, 1, 0, 0),
            ('region:2', 'v1.pt_objects', 25, '', 0, datetime(2017, 1, 1, 0, 0), 1, 4, 4, 0),
            ('', 'v1.coverage', 51, '', 0, datetime(2017, 1, 1, 0, 0), 1, 2, 2, 0),
            ('region:2', 'v1.networks.collection', 25, '', 0, datetime(2017, 1, 1, 0, 0), 1, 2, 2, 8),
            ('region:1', 'v1.stop_areas.collection', 51, '', 0, datetime(2017, 1, 1, 0, 0), 1, 6, 6, 150)
        ]

        assert result == expected_results
        assert self.partitionned_table_exists('requests_calls_y2017m01')
