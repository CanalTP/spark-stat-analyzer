from analyzers.stat_utils import region_id, is_internal_call, request_date
from analyzers import Analyzer


class AnalyzeCoverageJourneysNetworksTransfers(Analyzer):
    @staticmethod
    def get_tuples_from_stat_dict(stat_dict):
        result = []
        for journey in stat_dict.get("journeys", []):
            if 'nb_transfers' in journey:
                networks_of_journey = []
                for section in journey.get('sections', []):
                    network_id = section.get('network_id', '')
                    if network_id != '' and network_id not in networks_of_journey:
                        networks_of_journey.append(network_id)
                if len(networks_of_journey):
                    result.append(
                        (
                            (
                                region_id(stat_dict),
                                journey.get("nb_transfers"),
                                len(networks_of_journey),
                                is_internal_call(stat_dict),
                                request_date(stat_dict)
                            ),
                            1
                        )
                    )
        return result

    def truncate_and_insert(self, data):
        columns = ('region_id', 'nb_transfers', 'nb_networks', 'is_internal_call', 'request_date', 'nb_journeys')
        self.database.insert(table_name='coverage_journeys_networks_transfers', columns=columns, data=data,
                             start_date=self.start_date,
                             end_date=self.end_date)

    def launch(self):
        coverage_journeys_networks_transfers = self.get_data(rdd_mode=True)
        self.truncate_and_insert(coverage_journeys_networks_transfers)

    @property
    def analyzer_name(self):
        return "CoverageJourneysNetworksTransfers"
