import pytest
import random
from tests.helpers.requests import get_request

random_asset_address = ""


@pytest.mark.order(1)
def test_asset_list_endpoint(api_schema, local_url, compare_url):

    asset_list_response = get_request(f"{local_url}/asset_list")
    api_schema["/asset_list"]["GET"].validate_response(asset_list_response)

    random_asset = random.choice(asset_list_response.json())
    random_asset_policy, random_asset_name = (
        random_asset["policy_id"],
        random_asset["asset_names"]["hex"][0],
    )

    asset_info_response = get_request(
        f"{local_url}/asset_info?_asset_policy={random_asset_policy}&_asset_name={random_asset_name}"
    )
    api_schema["/asset_info"]["GET"].validate_response(asset_info_response)

    compare_asset_info_response = get_request(
        f"{compare_url}/asset_info?_asset_policy={random_asset_policy}&_asset_name={random_asset_name}"
    )

    assert asset_info_response.json() == compare_asset_info_response.json()

    asset_address_list_response = get_request(
        f"{local_url}/asset_address_list?_asset_policy={random_asset_policy}&_asset_name={random_asset_name}"
    )

    random_asset_address = random.choice(asset_address_list_response.json())


@pytest.mark.order(2)
def test_address_info_endpoint(api_schema, compare_url, local_url):

    address_info_response = get_request(
        f"{local_url}/address_info?_address={random_asset_address}"
    )
    api_schema["/address_info"]["GET"].validate_response(address_info_response)

    compare_address_info_response = get_request(
        f"{compare_url}/address_info?_address={random_asset_address}"
    )

    assert address_info_response.json() == compare_address_info_response.json()
