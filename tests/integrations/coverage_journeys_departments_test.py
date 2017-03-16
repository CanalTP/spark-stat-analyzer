from tests.integrations.mechanism import Mechanism
from tests.checker import same_list_tuple
from datetime import datetime


class TestAnalyzeCoverageJourneysDepartments(Mechanism):

    def test_coverage_journeys_departments(self):
        self.launch(analyzer='coverage_journeys_departments', start_date='2017-03-16', end_date='2017-03-16')
        result = self.get_data(table_name='coverage_journeys_departments',
                               columns=['request_date', 'region_id', 'is_internal_call',
                                        'departure_department_code', 'arrival_department_code', 'nb_req'])

        expected_results = [
            (datetime(2017, 3, 16, 0, 0), 'fr-foo', 0, '94', '75', 1),
            (datetime(2017, 3, 16, 0, 0), 'fr-foo', 0, '75', '29', 2),
            (datetime(2017, 3, 16, 0, 0), 'fr-foo', 0, '75', '75', 1)]

        assert same_list_tuple(result, expected_results)
        assert self.partitionned_table_exists('coverage_journeys_departments_y2017m03')
