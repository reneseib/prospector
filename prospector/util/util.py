#!/usr/bin/python3

from pconfig import config

import sys
import os
import geopandas as gpd
from math import radians, cos, sin, asin, sqrt

# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir


main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def progressbar(count, total, status=""):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = "=" * filled_len + "-" * (bar_len - filled_len)

    sys.stdout.write("[%s] %s%s ...%s\r" % (bar, percents, "%", status))
    sys.stdout.flush()


def get_last_stage_name(current_stage):
    all_stages = tuple(
        list(config["directories"]["prospector_data"]["results"]["stages"].keys())
    )
    current_idx = all_stages.index(current_stage)
    prev_idx = current_idx - 1
    return all_stages[prev_idx]


def load_prev_stage_to_gdf(regio, current_stage):
    # Get name of previous stage
    prev_stage = get_last_stage_name(current_stage)

    # Build path to the regio's last stage gpkg file
    last_stage_data = os.path.join(
        results_dir, "stages", prev_stage, regio, "gpkg", f"{regio}-{prev_stage}.gpkg"
    )

    # 2. Load file to GDF
    if os.path.isfile(last_stage_data):
        gdf = gpd.read_file(last_stage_data)
        gdf = gpd.GeoDataFrame(gdf)

        return gdf

    else:
        return gpd.GeoDataFrame([])


def save_current_stage_to_file(gdf, regio, current_stage):
    output_file_gpkg = os.path.join(
        results_dir,
        "stages",
        current_stage,
        regio,
        "gpkg",
        f"{regio}-{current_stage}.gpkg",
    )

    try:
        gdf.to_file(output_file_gpkg, driver="GPKG")
        return True

    except Exception as e:
        print(e)
        return False


def load_file_to_gdf(file_path, src_crs=4326, target_crs=25832):
    f = gpd.read_file(file_path)
    if src_crs is not None:
        gdf = gpd.GeoDataFrame(f).set_crs(src_crs)
    if target_crs is not None:
        gdf = gpd.GeoDataFrame(f).to_crs(src_crs)

    return gdf


def haversine(p1, p2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      IMPORTANT: ONLY WORKS WITH WSG84 / EPSG:4326 coordinates
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    """
    # print("P1", p1)
    # print("P2", p2)
    # sys.exit()

    lon1, lat1 = p1[::-1]
    lon2, lat2 = p2[::-1]

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r


def flatten_polygon(x):
    if isinstance(x, list):
        return [a for i in x for a in flatten_polygon(i)]
    elif isinstance(x, tuple):
        if len(x) == 2 and type(x[0]) == float:
            return [x]
        else:
            return [a for i in x for a in flatten_polygon(i)]
    #
    # else:
    #     return [x]
