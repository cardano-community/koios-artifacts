import schemathesis
import requests
import random

schema = schemathesis.from_path("../specs/results/koiosapi-guild.yaml")
local_url = "http://127.0.0.1:8053/api/v0"
compare_url = "https://guild.koios.rest/api/v0"

def test_script_endpoints():
    script_response = requests.get(f"{local_url}/native_script_list")
    schema["/native_script_list"]["GET"].validate_response(script_response)

    script_list = script_response.json()
    random_script = random.choice(script_list)
    random_script_creation_tx_hash = random_script['creation_tx_hash']

    tx_response = requests.post(f"{local_url}/tx_info", json={'_tx_hashes': [random_script_creation_tx_hash]})
    schema["/tx_info"]["POST"].validate_response(tx_response)

    tx_block_hash = tx_response.json()[0]['block_hash']

    block_response = requests.post(f"{local_url}/block_info", json={'_block_hashes': [tx_block_hash]})
    schema["/block_info"]["POST"].validate_response(block_response)

    compare_block_response = requests.post(f"{compare_url}/block_info", json={'_block_hashes': [tx_block_hash]})

    assert block_response.json() == compare_block_response.json()