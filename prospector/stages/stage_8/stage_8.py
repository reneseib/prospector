# import os, sys
#
# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)
#
# # Stage 0 Imports
# from pyrosm import OSM, get_data
# import geopandas as gpd
# from shapely.geometry import Point, Polygon, MultiPolygon, mapping
# from shapely import wkt
# from pyrosm import OSM, get_data
# import numpy as np
# from pyproj import Geod
#
# # Custom imports
# from pconfig import config
# from proxpy.proxpy import ProxyRequest
#
# for dir in config["init"]["prospector_package_path"]:
#     sys.path.append(dir)
#
# from util import util
#
#
# for dir in config["init"]["proj_path"]:
#     if os.path.isdir(dir):
#         proj_path = dir
#
# main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
# src_data_dir = os.path.join(main_dir, "src_data")
# geo_data_dir = os.path.join(src_data_dir, "geo_data")
# results_dir = os.path.join(main_dir, "results")

import requests
import json


def get_solar_data(lat: float, lon: float) -> json:
    """
    Get request to solaratlas to fetch data, returns json
    """
    api_url = f"https://api.globalsolaratlas.info/data/lta?loc={lat},{lon}"

    response = requests.get(api_url)
    data = json.loads(response.content)["annual"]["data"]
    """
    Returns a dict like this:
    {'PVOUT_csi': 1036.3988037109375, 'DNI': 905.390625, 'GHI': 1055.328125, 'DIF': 565.359375, 'GTI_opta': 1226.7890625, 'OPTA': 36, 'TEMP': 8.5625, 'ELE': 331}
    """
    print(data)

    return None


# def f_stage_8(regio, stage="8-added_solar_data"):
#     """
#     Stage 8:
#     Adds lots of solar data of the area
#     """
#
#     print(regio)
#
#     # Load previous stage data
#     gdf = util.load_prev_stage_to_gdf(regio, stage)
#
#     # Create proxyserver instance to use in loop
#     proxy_server = ProxyRequest()
#
#     if len(gdf) > 0:
#
#         for i in range(len(gdf)):
#             print("")
#
#     return None


# proxy_server = ProxyRequest()
lat = 51.282494
lon = 9.001064
get_solar_data(lat, lon)
