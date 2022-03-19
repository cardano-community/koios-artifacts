from helpers.requests import get_request
from helpers.api_schema import schema


def test_genesis_endpoint():
    genesis_response = get_request("genesis")
    schema["/genesis"]["GET"].validate_response(genesis_response)

    compare_genesis_response = get_request("genesis", is_local=False)

    assert genesis_response.json() == compare_genesis_response.json()
