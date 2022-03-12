import schemathesis
import random
from helpers import get_request, post_request

schema = schemathesis.from_uri("https://guild.koios.rest/koiosapi.yaml")
pool_reward_address = ""


def test_pool_list_endpoint():
    global pool_reward_address

    pool_list_response = get_request("pool_list")
    schema["/pool_list"]["GET"].validate_response(pool_list_response)

    random_pool = random.choice(pool_list_response.json())
    random_pool_id = random_pool["pool_id_bech32"]

    pool_info_response = post_request(
        "pool_info", {"_pool_bech32_ids": [random_pool_id]}
    )
    schema["/pool_info"]["POST"].validate_response(pool_info_response)

    compare_pool_info_response = post_request(
        "pool_info", {"_pool_bech32_ids": [random_pool_id]}, is_local=False
    )

    assert pool_info_response.json() == compare_pool_info_response.json()

    pool_reward_address = pool_info_response.json()[0]["reward_addr"]


def test_account_info_endpoint():
    global pool_reward_address

    account_info_response = get_request(f"account_info?_address={pool_reward_address}")
    compare_account_info_response = get_request(
        f"account_info?_address={pool_reward_address}", is_local=False
    )

    assert account_info_response.json() == compare_account_info_response.json()
