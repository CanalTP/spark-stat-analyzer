from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeCoverageJourneysRequestsParams(Mechanism):

    def test_coverage_journeys_requests_params(self):
        self.launch(analyzer='coverage_journeys_requests_params', start_date='2017-01-20', end_date='2017-01-22')
        result = self.get_data(table_name='coverage_journeys_requests_params',
                               columns=['request_date', 'region_id', 'is_internal_call', 'nb_wheelchair'])

        expected_results = [
            (datetime(2017, 1, 22, 0, 0), 'fr-foo', 0, 2),
            (datetime(2017, 1, 22, 0, 0), 'fr-bar', 0, 1),
        ]

        assert same_list_tuple(result, expected_results)
