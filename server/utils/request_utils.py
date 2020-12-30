import requests


class RequestUtils:
    @staticmethod
    def get(url, cookies=None):
        if cookies is None:
            cookies = {}
        response = requests.get(url, cookies=cookies)
        return response

    @staticmethod
    def post(url, json_data=None):
        if json_data is None:
            json_data = {}
        response = requests.post(url, json=json_data)
        return response
