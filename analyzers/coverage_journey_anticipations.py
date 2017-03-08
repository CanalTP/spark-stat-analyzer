from analyzers import Analyzer
from datetime import datetime
from analyzers.stat_utils import region_id, is_internal_call, request_date


class AnalyzeCoverageJourneyAnticipations(Analyzer):
    @staticmethod
    def get_coverage_journey_anticipations(stat_dict):
        journey_request = stat_dict.get('journey_request', None)
        if journey_request and 'requested_date_time' in journey_request:
            yield abs(
                (datetime.utcfromtimestamp(journey_request['requested_date_time']).date() -
                 datetime.utcfromtimestamp(stat_dict['request_date']).date()).days
            )

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        return map(
            lambda coverage_journey_anticipation_difference:
            (
                (
                    region_id(stat_dict),
                    coverage_journey_anticipation_difference,
                    is_internal_call(stat_dict),
                    request_date(stat_dict)
                ),
                1
            ),
            AnalyzeCoverageJourneyAnticipations.get_coverage_journey_anticipations(stat_dict)
        )

    def truncate_and_insert(self, data):
        if len(data):
            self.database.insert("coverage_journey_anticipations",
                                 ("region_id",
                                  "difference",
                                  "is_internal_call",
                                  "request_date",
                                  "nb")
                                 , data, self.start_date, self.end_date)

    def launch(self):
        coverage_journey_anticipations = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_journey_anticipations)

    @property
    def analyzer_name(self):
        return "CoverageJourneyAnticipations"
