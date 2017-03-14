from analyzers.analyzer import Analyzer
from analyzers.stat_utils import region_id, is_internal_call, request_date
from math import floor


class AnalyzeCoverageJourneysDuration(Analyzer):
    @staticmethod
    def get_durations(stat_dict):
        journeys = stat_dict.get("journeys", None)
        if not journeys or not len(journeys):
            return
        coverages = stat_dict.get("coverages", None)
        if not coverages or not len(coverages):
            return
        for journey in journeys:
            yield floor(journey.get("duration") / 60)

    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        return map(
            lambda duration:
            (
                (
                    region_id(stat_dict),
                    request_date(stat_dict),
                    is_internal_call(stat_dict),
                    duration,
                ),
                1
            ),
            AnalyzeCoverageJourneysDuration.get_durations(stat_dict)
        )

    def truncate_and_insert(self, data):
        self.database.insert(
            "coverage_journeys_duration",
            ("region_id", "request_date", "is_internal_call", "duration", "nb"),
            data,
            self.start_date,
            self.end_date
        )

    def launch(self):
        data = self.get_data(rdd_mode=True)
        self.truncate_and_insert(data)

    @property
    def analyzer_name(self):
        return "CoverageJourneysDuration"
