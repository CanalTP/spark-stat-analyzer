from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeCoverageJourneysDuration(Mechanism):

    def test_coverage_journeys_duration(self):
        self.launch(analyzer='coverage_journeys_duration', start_date='2017-03-14', end_date='2017-03-14')
        result = self.get_data(table_name='coverage_journeys_duration',
                               columns=['request_date', 'region_id', 'is_internal_call', 'duration', 'nb'])

        expected_results = [
            (datetime(2017, 3, 14, 0, 0), 'fr-foo', 0, 22, 1),
            (datetime(2017, 3, 14, 0, 0), 'fr-foo', 0, 2, 1),
            (datetime(2017, 3, 14, 0, 0), 'fr-foo', 0, 0, 1),
            (datetime(2017, 3, 14, 0, 0), 'fr-foo', 0, 1, 2)
        ]

        assert same_list_tuple(result, expected_results)
        assert self.partitionned_table_exists('coverage_journeys_duration_y2017m03')
