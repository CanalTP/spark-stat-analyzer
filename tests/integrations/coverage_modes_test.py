from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeCoverageModes(Mechanism):

    def test_coverage_modes(self):
        self.launch(analyzer='coverage_modes', start_date='2017-01-15', end_date='2017-01-15')
        result = self.get_data(table_name='coverage_modes',
                               columns=['region_id', 'type', 'mode', 'commercial_mode_id',
                               'commercial_mode_name', 'is_internal_call', 'request_date', 'nb'])
        expected_results = [
            ('auv', 'public_transport', 'car', 'BUS', 'BUS 1', 1, datetime(2017, 1, 15, 0, 0), 2),
            ('auv', 'public_transport', '', 'commercial_mode:RER', 'RER', 1, datetime(2017, 1, 15, 0, 0), 2)
        ]

        assert same_list_tuple(result, expected_results)
