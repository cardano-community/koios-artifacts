from tests.helpers.requests import get_request


def test_tip_endpoint(api_schema, local_url, compare_url):
    tip_response = get_request(f"{local_url}/tip")
    api_schema["/tip"]["GET"].validate_response(tip_response)

    compare_tip_response = get_request(f"{compare_url}/tip")

    assert tip_response.json() == compare_tip_response.json()
