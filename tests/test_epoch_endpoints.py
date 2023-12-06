import pytest
import random

from tests.helpers.requests import get_request

random_epoch_no = ""


@pytest.mark.order(1)
def test_epoch_info_endpoint(api_schema, local_url, compare_url):

    epoch_info_response = get_request(f"{local_url}/epoch_info")
    api_schema["/epoch_info"]["GET"].validate_response(epoch_info_response)

    random_epoch = random.choice(epoch_info_response.json())
    random_epoch_no = random_epoch["epoch_no"]

    compare_epoch_info_response = get_request(
        f"{compare_url}/epoch_info?_epoch_no={random_epoch_no}"
    )

    assert random_epoch == compare_epoch_info_response.json()[0]


@pytest.mark.order(2)
def test_epoch_params_endpoint(api_schema, compare_url, local_url):

    epoch_params_response = get_request(
        f"{local_url}/epoch_params?_epoch_no={random_epoch_no}"
    )
    api_schema["/epoch_params"]["GET"].validate_response(epoch_params_response)

    compare_epoch_params_response = get_request(
        f"{compare_url}/epoch_params?_epoch_no={random_epoch_no}"
    )

    assert epoch_params_response.json() == compare_epoch_params_response.json()
