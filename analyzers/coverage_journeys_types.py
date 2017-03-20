from analyzers.analyzer import Analyzer
from analyzers.stat_utils import region_id, request_date


class AnalyzeCoverageJourneysTypes(Analyzer):
    @staticmethod
    def get_type_from_parameter_value(value):
        result = 'address'

        if (':' in value):
            result = value.partition(':')[0]
        return result

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        types = {}
        available_keys = ['from', 'to']

        if "journey_request" in stat_dict:
            for parameter in stat_dict.get('parameters', []):
                if parameter['key'] in available_keys:
                    types.setdefault(
                        parameter['key'],
                        AnalyzeCoverageJourneysTypes.get_type_from_parameter_value(parameter['value'])
                    )
                if len(types) == 2:
                    result.append(
                        (
                            (
                                request_date(stat_dict),
                                region_id(stat_dict),
                                types['from'],
                                types['to']
                            ),
                            1
                        )
                    )
                    break

        return result

    def truncate_and_insert(self, data):
        self.database.insert(
            "coverage_journeys_types",
            ("request_date", "region_id", "from_type", "to_type", "nb"),
            data,
            self.start_date,
            self.end_date
        )

    def launch(self):
        coverage_journeys_types = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_journeys_types)

    @property
    def analyzer_name(self):
        return "CoverageJourneysTypes"
