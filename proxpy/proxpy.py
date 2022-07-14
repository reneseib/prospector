import requests
import random
from bs4 import BeautifulSoup
import pandas as pd
import json


def generate_ip_list():
    # Source of IP addresses
    source = "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=speed&sort_type=desc"

    ip_data = requests.get(source)

    ip_json = json.loads(ip_data.text)

    ip_dict = dict(ip_json)

    ip_list = []

    for data in ip_dict["data"][:100]:
        if isinstance(data, dict):
            ip = []
            for key, value in data.items():
                if key == "ip":
                    ip.append(value)
                if key == "port":
                    ip.append(value)

                if len(ip) == 2:
                    proxy = ":".join(ip)
                    if proxy not in ip_list:
                        ip_list.append(proxy)

    return ip_list


class ProxyRequest(object):
    def __init__(self):
        self.ip_list = generate_ip_list()

    def get(self, url, headers=None):
        randint = random.randint(0, len(self.ip_list) - 1)
        proxies = {"http": f"http://{self.ip_list[randint]}"}
        response = requests.get(url, headers=headers, proxies=proxies)
        return response
