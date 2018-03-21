from tests.integrations.mechanism import Mechanism
from datetime import datetime


class TestAnalyzeCoverageJourneys(Mechanism):

    def test_coverage_journeys(self):
        self.launch(analyzer='coverage_journeys', start_date='2017-01-20', end_date='2017-01-20')
        result = self.get_data(table_name='coverage_journeys',
                               columns=['request_date', 'region_id', 'is_internal_call', 'nb', 'nb_with_odt'])
        expected_results = [
            (datetime(2017, 1, 21, 0, 0), 'fr-foo', 0, 9, 4),
            (datetime(2017, 1, 21, 0, 0), 'fr-bar', 0, 8, 1)
        ]

        assert result == expected_results
        assert self.partitionned_table_exists('coverage_journeys_y2017m01')
