from tests.integrations.mechanism import Mechanism
from datetime import datetime


class TestAnalyzeCoverageStopAreas(Mechanism):

    def test_coverage_stop_areas(self):
        self.launch(analyzer='coverage_stop_areas', start_date='2017-01-15', end_date='2017-01-22')
        result = self.get_data(table_name='coverage_stop_areas',
                               columns=['request_date', 'region_id', 'stop_area_id', 'stop_area_name', 'city_id',
                               'city_name', 'city_insee', 'department_code', 'is_internal_call', 'nb'])

        expected_results = [
            (datetime(2017, 1, 18, 0, 0), 'npdc', 'sa_1', 'stop 1', '', '', '123456', '12', 0, 1),
            (datetime(2017, 1, 18, 0, 0), 'npdc', 'sa_3', 'stop 3', '', 'on the styx', '', '', 0, 1),
            (datetime(2017, 1, 18, 0, 0), 'auv', 'sa_1', 'stop 1', '', '', '', '', 1, 1),
            (datetime(2017, 1, 15, 0, 0), 'auv', 'sa_2', 'stop 2', '', '', '', '', 1, 2),
            (datetime(2017, 1, 15, 0, 0), 'auv', 'sa_3', 'stop 3', '', '', '', '', 1, 2),
            (datetime(2017, 1, 18, 0, 0), 'auv', 'sa_4', 'stop 4', '', '', '', '', 1, 2),
            (datetime(2017, 1, 18, 0, 0), 'npdc', 'sa_4', 'stop 4', 'admin:xxx4', '', '', '', 0, 1),
            (datetime(2017, 1, 15, 0, 0), 'auv', 'sa_1', 'stop 1', '', '', '', '', 1, 1),
            (datetime(2017, 1, 18, 0, 0), 'npdc', 'sa_3', 'stop 3', '', '', '', '', 0, 1),
            (datetime(2017, 1, 15, 0, 0), 'auv', 'sa_4', 'stop 4', '', '', '', '', 1, 2),
            (datetime(2017, 1, 18, 0, 0), 'npdc', 'sa_2', 'stop 2', '', '', '987654', '98', 0, 2),
            (datetime(2017, 1, 18, 0, 0), 'auv', 'sa_2', 'stop 2', '', '', '', '', 1, 2),
            (datetime(2017, 1, 18, 0, 0), 'auv', 'sa_3', 'stop 3', '', '', '', '', 1, 2)
        ]

        assert result == expected_results
        assert self.partitionned_table_exists('coverage_stop_areas_y2017m01')
