import requests


def get_request(url=""):
    response = requests.get(url)
    return response


def post_request(url="", data={}):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    response = requests.post(url, json=data, headers=headers)

    return response
