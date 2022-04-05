import random
from tests.helpers.requests import get_request, post_request


def test_native_script_list_endpoint(api_schema, local_url, compare_url):
    script_response = get_request(f"{local_url}/native_script_list")
    api_schema["/native_script_list"]["GET"].validate_response(script_response)

    random_script = random.choice(script_response.json())
    random_script_creation_tx_hash = random_script["creation_tx_hash"]

    tx_response = post_request(
        f"{local_url}/tx_info", {"_tx_hashes": [random_script_creation_tx_hash]}
    )
    api_schema["/tx_info"]["POST"].validate_response(tx_response)

    tx_block_hash = tx_response.json()[0]["block_hash"]

    block_response = post_request(
        f"{local_url}/block_info", {"_block_hashes": [tx_block_hash]}
    )
    api_schema["/block_info"]["POST"].validate_response(block_response)

    compare_block_response = post_request(
        f"{compare_url}/block_info", {"_block_hashes": [tx_block_hash]}
    )

    assert block_response.json() == compare_block_response.json()
