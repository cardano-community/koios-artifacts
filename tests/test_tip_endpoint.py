from helpers.requests import get_request
from helpers.api_schema import schema


def test_tip_endpoint():
    tip_response = get_request("tip")
    schema["/tip"]["GET"].validate_response(tip_response)

    compare_tip_response = get_request("tip", is_local=False)

    assert tip_response.json() == compare_tip_response.json()
