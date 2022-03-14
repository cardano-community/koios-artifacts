import random
from helpers.requests import get_request
from helpers.api_schema import schema

random_epoch_no = ""


def test_epoch_info_endpoint():
    global random_epoch_no

    epoch_info_response = get_request("epoch_info")
    schema["/epoch_info"]["GET"].validate_response(epoch_info_response)

    random_epoch = random.choice(epoch_info_response.json())
    random_epoch_no = random_epoch["epoch_no"]

    compare_epoch_info_response = get_request(f"epoch_info?_epoch_no={random_epoch_no}")

    assert random_epoch == compare_epoch_info_response.json()[0]


def test_epoch_params_endpoint():
    global random_epoch_no

    epoch_params_response = get_request(f"epoch_params?_epoch_no={random_epoch_no}")
    schema["/epoch_params"]["GET"].validate_response(epoch_params_response)

    compare_epoch_params_response = get_request(
        f"epoch_params?_epoch_no={random_epoch_no}", is_local=False
    )

    assert epoch_params_response.json() == compare_epoch_params_response.json()
