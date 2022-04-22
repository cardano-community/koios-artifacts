from tests.helpers.requests import get_request


def test_genesis_endpoint(api_schema, local_url, compare_url):
    genesis_response = get_request(f"{local_url}/genesis")
    api_schema["/genesis"]["GET"].validate_response(genesis_response)

    compare_genesis_response = get_request(f"{compare_url}/genesis")

    assert genesis_response.json() == compare_genesis_response.json()
