import requests

from helpers.test_urls import local_url, compare_url


def get_request(endpoint="", is_local=True):
    if is_local:
        url = local_url + endpoint
    else:
        url = compare_url + endpoint

    response = requests.get(url)
    return response


def post_request(endpoint="", data={}, is_local=True):
    if is_local:
        url = local_url + endpoint
    else:
        url = compare_url + endpoint

    response = requests.post(url, json=data)
    return response
