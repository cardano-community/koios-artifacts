import random
from tests.helpers.requests import get_request


def test_totals_endpoint(api_schema, local_url, compare_url):
    totals_response = get_request(f"{local_url}/totals")
    api_schema["/totals"]["GET"].validate_response(totals_response)

    random_totals = random.choice(totals_response.json())
    random_totals_epoch = random_totals["epoch_no"]

    compare_totals_response = get_request(
        f"{compare_url}/totals?epoch_no=eq.{random_totals_epoch}"
    )

    assert [random_totals] == compare_totals_response.json()
