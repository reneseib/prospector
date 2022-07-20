import requests
import random
import json
import time


class ProxyRequest(object):
    @staticmethod
    def generate_ip_list():
        # Source of IP addresses
        source = "https://spys.me/socks.txt"

        ip_data = requests.get(source).text

        ip_data = ip_data.strip()
        ip_tmp_list = ip_data.split("\n")

        ip_list = []

        # Strip the intro from txt
        ip_tmp_list = ip_tmp_list[6:]
        for row in ip_tmp_list:
            # Only add High Anonymity servers
            if "-H" in row:
                row = row.split(" ")[0]
                ip_list.append(row)

        return ip_list

    def __init__(self):
        self.ip_list = ProxyRequest.generate_ip_list()
        self.setup_time = time.time()
        # TODO: Add function to refresh list after 60min

    def get(self, url, headers=None):
        randint = random.randint(0, len(self.ip_list) - 1)
        proxies = {"socks": f"socks5://{self.ip_list[randint]}"}
        response = requests.get(url, headers=headers, proxies=proxies)
        return response
