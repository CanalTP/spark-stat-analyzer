from operator import add
from analyzers.analyzer import Analyzer
from analyzers.stat_utils import region_id, is_internal_call, request_date


class AnalyzeCoverageJourneys(Analyzer):
    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        nb_journey_with_odt = 0
        journeys = stat_dict.get('journeys', [])

        for journey in journeys:
            if any(section['type'] == 'on_demand_transport' for section in journey.get('sections', [])):
                nb_journey_with_odt += 1
                break


        if not len(journeys):
            return []
        return [
            (
                (
                    request_date(stat_dict),
                    region_id(stat_dict),
                    is_internal_call(stat_dict)
                ),
                (
                    len(journeys),
                    nb_journey_with_odt
                )
            )
        ]

    @staticmethod
    def get_logic_to_reduce_by_key(a, b):
        return (add(a[0], b[0]), add(a[1], b[1]))

    def convert_data(self, data):
        result = data
        data[:] = [(element[0],element[1],element[2]) + element[3] for element in data]

        return result

    def truncate_and_insert(self, data):
        self.database.insert(
            "coverage_journeys",
            ("request_date", "region_id", "is_internal_call", "nb", "nb_with_odt"),
            data,
            self.start_date,
            self.end_date
        )

    def launch(self):
        coverage_journeys = self.get_data(rdd_mode=True)
        self.truncate_and_insert(self.convert_data(coverage_journeys))

    @property
    def analyzer_name(self):
        return "CoverageJourneys"
