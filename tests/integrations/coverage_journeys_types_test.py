from tests.integrations.mechanism import Mechanism
from datetime import datetime


class TestAnalyzeCoverageJourneysTypes(Mechanism):

    def test_coverage_journeys_types(self):
        self.launch(analyzer='coverage_journeys_types', start_date='2017-01-20', end_date='2017-01-20')
        result = self.get_data(table_name='coverage_journeys_types',
                               columns=['request_date', 'region_id', 'from_type', 'to_type', 'nb'])
        expected_results = [
            (datetime(2017, 1, 20, 0, 0), 'fr-foo', 'address', 'poi', 1),
            (datetime(2017, 1, 20, 0, 0), 'fr-foo', 'stop_point', 'stop_area', 3),
            (datetime(2017, 1, 20, 0, 0), 'fr-foo', 'stop_area', 'poi', 2)
        ]

        for expected_result in expected_results:
            assert expected_result in result
            
        assert self.partitionned_table_exists('coverage_journeys_types_y2017m01')

    def test_coverage_journeys_types_with_null_character(self):
        self.launch(analyzer='coverage_journeys_types', start_date='2019-05-11', end_date='2019-05-11')
        result = self.get_data(table_name='coverage_journeys_types',
                               columns=['request_date', 'region_id', 'from_type', 'to_type', 'nb'],
                               start_date='2019-05-11',
                               end_date='2019-05-11')
        expected_results = [
            (datetime(2019, 5, 11, 0, 0), 'fr-foo', 'stop_point', '..\\windows\\win.inistop_area', 1)
        ]

        assert result == expected_results
        assert self.partitionned_table_exists('coverage_journeys_types_y2019m05')
