import schemathesis
import random
from helpers import get_request, post_request

schema = schemathesis.from_uri("https://guild.koios.rest/koiosapi.yaml")


def test_native_script_endpoint():
    script_response = get_request("native_script_list")
    schema["/native_script_list"]["GET"].validate_response(script_response)

    random_script = random.choice(script_response.json())
    random_script_creation_tx_hash = random_script["creation_tx_hash"]

    tx_response = post_request(
        "tx_info", {"_tx_hashes": [random_script_creation_tx_hash]}
    )
    schema["/tx_info"]["POST"].validate_response(tx_response)

    tx_block_hash = tx_response.json()[0]["block_hash"]

    block_response = post_request("block_info", {"_block_hashes": [tx_block_hash]})
    schema["/block_info"]["POST"].validate_response(block_response)

    compare_block_response = post_request(
        "block_info", {"_block_hashes": [tx_block_hash]}, is_local=False
    )

    assert block_response.json() == compare_block_response.json()

def test_plutus_script_endpoint():
    # empty