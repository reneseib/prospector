import os, sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Stage 0 Imports
from pyrosm import OSM, get_data
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
from shapely import wkt
from pyrosm import OSM, get_data
import numpy as np
import requests

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def f_stage_6(regio, stage="6-added_slope"):
    """
    Stage 6:
    Adds the slope of the 'area'.
    """

    example_api_url = f"https://en-gb.topographic-map.com/?_path=api.maps.getElevation&latitude=50.44557715398033&longitude=10.091823051237167&version=2021013001"

    wsg84_lat = "50.44557785398033"
    wsg84_lon = "10.091123051237167"

    api_url = f"https://en-gb.topographic-map.com/?_path=api.maps.getElevation&latitude={wsg84_lat}&longitude={wsg84_lon}&version=2021013001"

    s = requests.Session()
    print("COOKIE BEFORE:", s.cookies.get_dict())
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "de,en-US;q=0.7,en;q=0.3",
        "Connection": "keep-alive",
        "Cookie": "cookies=%7B%22advertisements%22%3A1%2C%22statistics%22%3A1%7D; _ga_1R1DLNZJ6C=GS1.1.1657720029.1.0.1657720029.0; _ga=GA1.1.1681229070.1657720030; __gads=ID=22e817fc638bf283-22e56e03cdcd00df:T=1657720030:RT=1657720030:S=ALNI_MZ5Sj9e6rSUka93GGGxcdpEL3ZT7Q",
        "Host": "en-gb.topographic-map.com",
        "map": "d93",
        "Referer": "https://en-gb.topographic-map.com/maps/d93/Germany/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "TE": "trailers",
        "token": "3d2n4cyp0",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:103.0) Gecko/20100101 Firefox/103.0",
    }

    res = s.get(api_url, headers=headers)
    print(res.text.replace("&nbsp;m", ""))
    print("COOKIE AFTER:", s.cookies.get_dict())

    print("GREETINGS FROM STAGE 6")
    sys.exit()
    return None
