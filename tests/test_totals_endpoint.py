import random
from helpers.requests import get_request
from helpers.api_schema import schema


def test_totals_endpoint():
    totals_response = get_request("totals")
    schema["/totals"]["GET"].validate_response(totals_response)

    random_totals = random.choice(totals_response.json())
    random_totals_epoch = random_totals["epoch_no"]

    compare_totals_response = get_request(
        f"genesis?epoch_no={random_totals_epoch}", is_local=False
    )

    assert random_totals == compare_totals_response.json()
