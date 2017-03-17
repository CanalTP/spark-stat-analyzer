import pytest
import os
from datetime import date, datetime
from analyzers import AnalyzeCoverageJourneysNetworksTransfers
from tests.checker import same_list_tuple

pytestmark = pytest.mark.usefixtures("spark")
path = os.getcwd() + "/tests/fixtures/coverage_journeys_transfers"

@pytest.mark.parametrize("data", [
    {},
    {"journeys": []},
    {"journeys": [{"name": "value", "sections": [{"network_id": "pouet"}]}]},
    {"journeys": [{"name": "value"}]},
    {"journeys": [{"nb_transfers": 3}]},
    {"journeys": [{"nb_transfers": 3, "sections": []}]},
    {"journeys": [{"nb_transfers": 3, "sections": [{"something": True}]}]},
])
def test_get_tuples_from_stat_dict_empty_result(data):
    result = AnalyzeCoverageJourneysNetworksTransfers.get_tuples_from_stat_dict(data)
    assert result == []
