import os, sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Stage 0 Imports
from pyrosm import OSM, get_data
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping
from shapely import wkt
from pyrosm import OSM, get_data
import numpy as np
from pyproj import Geod

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


def to_tuple(str):
    l = str.split(", ")
    l = [float(x) for x in l]
    t = tuple(l)
    return t


def f_stage_5(regio, stage="5-added_nearest_substation"):
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    power_file = "/common/ecap/resources/power/substations-germany.csv"

    df = pd.read_csv(power_file, delimiter=";", encoding="utf8")
    df["Koordinaten"] = df["Koordinaten"].apply(lambda x: to_tuple(x))
    print(df["Koordinaten"])

    """
    Stage 5:
    Adds the distance from the 'area' to the nearest substation.
    """
    print("GREETINGS FROM STAGE 5")
    return None
