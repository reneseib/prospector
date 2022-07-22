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

    def update_ip_list(self):
        while True:
            if time.time() - self.last_update > 1800:
                # Update IP list
                self.ip_list = ProxyRequest.generate_ip_list()
                # Update last_update to now
                self.last_update = time.time()

    def __init__(self):
        self.ip_list = ProxyRequest.generate_ip_list()
        self.lastidx = []
        self.range = list(range(0, len(self.ip_list)))
        self.last_update = time.time()
        self.update = self.update_ip_list()

    def get_idx(self):
        if len(self.range) == 0:
            self.range = list(range(0, len(self.ip_list)))
        else:
            idx = self.range.pop(0)
            return idx

    def get(self, url, headers=None):
        idx = get_idx()
        proxies = {"socks": f"socks5://{self.ip_list[idx]}"}
        response = requests.get(url, headers=headers, proxies=proxies)
        return response
