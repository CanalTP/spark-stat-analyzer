from analyzers.analyzer import Analyzer
from analyzers.stat_utils import region_id, is_internal_call, request_date


class AnalyzeCoverageJourneysDepartments(Analyzer):
    @staticmethod
    def get_departments(stat_dict):
        journey_request = stat_dict.get("journey_request", None)
        departure_code = ""
        arrival_code = ""

        if journey_request:
            if 'departure_insee' in journey_request or 'arrival_insee' in journey_request:
                departure_insee = journey_request.get("departure_insee", "")
                arrival_insee = journey_request.get("arrival_insee", "")
                departure_code = departure_insee[0:2] if departure_insee else departure_insee
                arrival_code = arrival_insee[0:2] if arrival_insee else arrival_insee

        yield {
            "departure": departure_code,
            "arrival": arrival_code,
        }

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        return map(
            lambda department_codes:
            (
                (
                    request_date(stat_dict),
                    region_id(stat_dict),
                    is_internal_call(stat_dict),
                    department_codes['departure'],
                    department_codes['arrival'],
                ),
                1
            ),
            AnalyzeCoverageJourneysDepartments.get_departments(stat_dict)
        )

    def truncate_and_insert(self, data):
        self.database.insert(
            "coverage_journeys_departments",
            (
                "request_date",
                "region_id",
                "is_internal_call",
                "departure_department_code",
                "arrival_department_code",
                "nb_req"
            ),
            data,
            self.start_date,
            self.end_date
        )

    def launch(self):
        data = self.get_data(rdd_mode=True)
        self.truncate_and_insert(data)

    @property
    def analyzer_name(self):
        return "CoverageJourneysDepartments"
