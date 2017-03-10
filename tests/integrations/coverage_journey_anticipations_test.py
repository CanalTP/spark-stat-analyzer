from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeCoverageJourneyAnticipations(Mechanism):
    def test_coverage_journey_anticipations(self):
        self.launch(analyzer='coverage_journey_anticipations', start_date='2017-01-16', end_date='2017-01-16')
        result = self.get_data(table_name='coverage_journey_anticipations',
                               columns=['region_id', 'is_internal_call', 'difference', 'request_date', 'nb'])
        expected_results = [('fr-cen', 0, -6, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 0, 0, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 0, 2, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 0, 5, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 0, 1, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 1, 7, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 0, 7, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 0, 6, datetime(2017, 1, 16, 0, 0), 1),
                            ('fr-cen', 1, 7, datetime(2017, 1, 16, 0, 0), 1),
                            ('transilien', 0, 3, datetime(2017, 1, 16, 0, 0), 1)]
        assert same_list_tuple(result, expected_results)
