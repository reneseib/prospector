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

    # Add column "centroid_ele" to gdf
    gdf["centroid_ele"] = None

    # Load centroid wkt string to shapely objects
    centroids_series = gdf["centroid"].apply(lambda x: wkt.loads(x))

    geom_series = gdf["geometry"]
    # print(centroids_series)

    centroid_frame = {"geometry": centroids_series}
    geom_frame = {"geometry": geom_series}

    # Make new GDF from the centroids_series to transform the coordinates
    centroid_gdf = gpd.GeoDataFrame(centroid_frame, geometry="geometry")
    geom_gdf = gpd.GeoDataFrame(geom_frame, geometry="geometry")

    # Centroid are in EPSG:25832 -> convert to EPSG:4326
    centroid_gdf = centroid_gdf.set_crs(25832).to_crs(4326)
    geom_gdf = geom_gdf.set_crs(25832).to_crs(4326)

    centroid_gdf["geometry"] = centroid_gdf["geometry"].apply(
        lambda x: np.array(x.coords.xy).astype(np.float64)
    )

    geom_gdf["geometry"] = geom_gdf["geometry"].apply(
        lambda geom: np.array(x.coords.xy).astype(np.float64)
    )

    # Get centroid, !!reverse!! coordinates and split them to lat and lon
    centroid_gdf["geometry"] = centroid_gdf["geometry"].apply(
        lambda x: np.array([x[1], x[0]])
    )
    # TODO: Split & reverse also for GEOM GDG!

    # TODO: Merge centroid and geometry points into one array of points

    # TODO: Iterate over all points in the distinct order so we can create the 2,2 array

    token_gen_script = open(
        "/common/ecap/prospector/stages/stage_6/token_generator_topo.js", "r"
    ).read()

    get_token = js2py.eval_js(token_gen_script)

    save_collection = []

    if len(centroid_gdf) == len(gdf):
        counter = 0

        proxy_server = ProxyRequest()

        save_threshold = round(len(gdf) * 0.1)
        if len(gdf) < 100:
            save_threshold = 50

        for i in range(len(centroid_gdf)):
            print(round((i / len(centroid_gdf)) * 100, 2), "%")
            geom = centroid_gdf.iloc[i, 0]
            lat = geom[0][0]
            lon = geom[1][0]

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

            t = timeit.default_timer()
            res = proxy_server.get(api_url, headers=headers)
            print("request: ", timeit.default_timer() - t)

            if res.status_code == 200:
                counter += 1
                res_height = res.text.replace("&nbsp;m", "")

                # gdf["centroid"] = gdf["centroid"].apply(lambda x: wkt.dumps(x))

                # Store result in GDF in new column "centroid_ele"
                gdf.at[i, "centroid_ele"] = res_height
                print("HEIGHT: ", res_height)

                # Sleep to not get us banned
                time.sleep(random.uniform(0.267, 0.865))

                if counter == save_threshold:
                    # Save gdf to disk after every api call so we don't loose
                    # data if connection is lost
                    stage_successfully_saved = util.save_current_stage_to_file(
                        gdf, regio, stage
                    )
                    counter = 0

                    save_collection.append(stage_successfully_saved)
                    print("saved status")

            else:
                print("LAT - LON: ", lat, lon)
                print("API URL: ", api_url)
                print("token: ", token)

            print(f"Next!")

        # Save again when finished, irrespective of the counter/save_threshold
        util.save_current_stage_to_file(gdf, regio, stage)
        return True

    return False
