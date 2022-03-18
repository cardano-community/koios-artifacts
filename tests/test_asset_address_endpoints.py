import pytest
import random
from helpers.requests import get_request
from helpers.api_schema import schema

random_asset_address = ""


@pytest.mark.order(1)
def test_asset_list_endpoint():
    global random_asset_address

    asset_list_response = get_request("asset_list")
    schema["/asset_list"]["GET"].validate_response(asset_list_response)

    random_asset = random.choice(asset_list_response.json())
    random_asset_policy, random_asset_name = (
        random_asset["policy_id"],
        random_asset["asset_names"]["hex"],
    )

    asset_info_response = get_request(
        f"asset_info?_asset_policy={random_asset_policy}&_asset_name={random_asset_name}"
    )
    schema["/asset_info"]["GET"].validate_response(asset_info_response)

    compare_asset_info_response = get_request(
        f"asset_info?_asset_policy={random_asset_policy}&_asset_name={random_asset_name}",
        is_local=False,
    )

    assert asset_info_response.json() == compare_asset_info_response.json()

    asset_address_list_response = get_request(
        f"asset_address_list?_asset_policy={random_asset_policy}&_asset_name={random_asset_name}"
    )

    random_asset_address = random.choice(asset_address_list_response.json())


@pytest.mark.order(2)
def test_address_info_endpoint():
    global random_asset_address

    address_info_response = get_request(f"address_info?_address={random_asset_address}")
    schema["/address_info"]["GET"].validate_response(address_info_response)

    compare_address_info_response = get_request(
        f"address_info?_address={random_asset_address}", is_local=False
    )

    assert address_info_response.json() == compare_address_info_response.json()
