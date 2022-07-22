import os, sys


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Stage 0 Imports
import geopandas as gpd
import numpy as np
import requests
import time
import js2py
import random
import timeit
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util
from proxpy.proxpy import ProxyRequest


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir
        sys.path.append(dir)

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def f_stage_6(regio, stage="6-added_slope"):
    """
    Stage 6:
    Adds the slope of the 'area'.
    """
    print(regio)

    # Load previous stage data
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    # Create proxyserver instance to use in loop
    proxy_server = ProxyRequest()

    if len(gdf) > 0 and not os.path.isfile(
        f"/common/ecap/prospector_data/results/stages/6-added_slope/{regio}/gpkg/{regio}-6-added_slope.gpkg"
    ):
        # First we scrape the elevation for all points and
        # store them in the gdf. Once stored, we will calculate the slopes.

        # 1.Create new, empty column for the elevation of the extrema points
        # and the centroid if it does not exist
        if not "points_ele" in gdf.columns:
            gdf["points_ele"] = None

        # Set counter to 0 at the beginng
        counter = 0

        # 2. Iterate over the gdf, get all points from extrema and centroid
        # into a tuple of tuples
        for i in range(len(gdf)):
            print(stage, " - ", regio, ":", round((i / len(gdf)) * 100, 2), "%")
            row = gdf.iloc[i]
            extrema = row["np_extrema_4326"]

            centroid = tuple(row["np_centroid_4326"])

            points = [tuple(x) for x in extrema]
            points.append(centroid)
            points = tuple(points)

            ele_results = [None] * 5

            # Iterate over points, make them to lat and lon
            for x in range(len(points)):
                point = points[x]

                # try:
                lat, lon = point
                # print(lat, lon)
                # except:
                #     print("/////////////////")
                #     print(point)
                #     raise

                save_threshold = round(len(gdf) * 0.1)
                # if len(gdf) < 100:
                #     save_threshold = 50

                token_gen_script = open(
                    "/common/ecap/prospector/stages/stage_6/token_generator_topo.js",
                    "r",
                ).read()

                get_token = js2py.eval_js(token_gen_script)

                token = get_token(5)

                user_agent_rotator = UserAgent(
                    limit=500,
                )

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
                    "token": token,
                    "User-Agent": user_agent_rotator.get_random_user_agent(),
                }

                # Make API-call, fetch and clean result as int()
                api_url = f"https://en-gb.topographic-map.com/?_path=api.maps.getElevation&latitude={lat}&longitude={lon}&version=2021013001"

                try:
                    res = proxy_server.get(api_url, headers=headers)

                    counter += 1

                    if res.status_code == 200:
                        counter += 1
                        res_height = res.text.replace("&nbsp;m", "")

                        # Store result in list
                        ele_results[x] = res_height
                        # print("HEIGHT: ", res_height)

                    else:
                        print("LAT - LON: ", lat, lon)
                        print("API URL: ", api_url)
                        print("token: ", token)
                except:
                    ele_results[x] = None

                # Add a little sleep
                sleeper = random.uniform(0.025, 0.2)
                time.sleep(sleeper)

            # Add results to gdf
            gdf.at[i, "points_ele"] = ", ".join(ele_results)

            """
            TODO: Figure out what produces the error/malformulation in the "points" after saving
            """
            # if counter == save_threshold * 5:
            #     interim_save = util.save_current_stage_to_file(gdf, regio, stage)
            #     print("Interim save")
            #     counter = 0

        # Save the final result
        final_save = util.save_current_stage_to_file(gdf, regio, stage)
        print(f"{regio}: all elevations saved")

        if final_save == True:
            return True
        else:
            return False
