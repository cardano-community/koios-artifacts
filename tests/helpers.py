import requests

local_url = "http://127.0.0.1:8053/api/v0/"
compare_url = "https://guild.koios.rest/api/v0/"


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

    response = requests.post(url, data)
    return response
