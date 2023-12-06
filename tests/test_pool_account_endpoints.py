import pytest
import random
from tests.helpers.requests import get_request, post_request

pool_reward_address = ""


@pytest.mark.order(1)
def test_pool_list_endpoint(api_schema, local_url, compare_url):

    pool_list_response = get_request(f"{local_url}/pool_list")
    api_schema["/pool_list"]["GET"].validate_response(pool_list_response)

    random_pool = random.choice(pool_list_response.json())
    random_pool_id = random_pool["pool_id_bech32"]

    pool_info_response = post_request(
        f"{local_url}/pool_info", {"_pool_bech32_ids": [random_pool_id]}
    )
    api_schema["/pool_info"]["POST"].validate_response(pool_info_response)

    compare_pool_info_response = post_request(
        f"{compare_url}/pool_info", {"_pool_bech32_ids": [random_pool_id]}
    )

    assert pool_info_response.json() == compare_pool_info_response.json()

    pool_reward_address = pool_info_response.json()[0]["reward_addr"]


@pytest.mark.order(2)
def test_account_info_endpoint(api_schema, local_url, compare_url):

    account_info_response = get_request(
        f"{local_url}/account_info?_address={pool_reward_address}"
    )
    api_schema["/account_info"]["GET"].validate_response(account_info_response)

    compare_account_info_response = get_request(
        f"{compare_url}/account_info?_address={pool_reward_address}"
    )

    assert account_info_response.json() == compare_account_info_response.json()
