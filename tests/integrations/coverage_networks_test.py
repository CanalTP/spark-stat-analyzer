from tests.integrations.mechanism import Mechanism
from datetime import datetime


class TestAnalyzeCoverageNetworks(Mechanism):

    def test_coverage_networks(self):
        self.launch(analyzer='coverage_networks', start_date='2017-01-15', end_date='2017-01-15')
        result = self.get_data(table_name='coverage_networks',
                               columns=['region_id', 'network_id', 'network_name', 'is_internal_call', 'request_date','nb'])

        expected_results = [
            ('auv', 'network:CD63', 'one network 63', 1, datetime(2017, 1, 15, 0, 0), 2),
            ('auv', 'network:CD64', 'one network 64', 1, datetime(2017, 1, 15, 0, 0), 1),
            ('auv', 'network:CD21', 'another network 21', 1, datetime(2017, 1, 15, 0, 0), 1),
            ('auv', 'network:CD65', 'one network 65', 1, datetime(2017, 1, 15, 0, 0), 1),
            ('auv', 'network:CD64', 'one network with different name', 1, datetime(2017, 1, 15, 0, 0), 1)
        ]

        assert result == expected_results
        assert self.partitionned_table_exists('coverage_networks_y2017m01')