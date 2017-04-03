from tests.integrations.mechanism import Mechanism
from datetime import datetime


class TestAnalyzeCoverageJourneysTransfers(Mechanism):

    def test_coverage_journeys_transfers(self):
        self.launch(analyzer='coverage_journeys_transfers', start_date='2017-01-15', end_date='2017-01-15')
        result = self.get_data(table_name='coverage_journeys_transfers',
                               columns=['region_id', 'nb_transfers', 'is_internal_call', "request_date", 'nb'])

        expected_results = [
            ('auv', 0, 1, datetime(2017, 1, 15, 0, 0), 1),
            ('auv', 2, 1, datetime(2017, 1, 15, 0, 0), 1)
        ]

        assert result == expected_results
        assert self.partitionned_table_exists('coverage_journeys_transfers_y2017m01')