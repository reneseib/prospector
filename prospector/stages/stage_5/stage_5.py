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
import timeit
import json

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
    t = tuple([l[1], l[0]])
    return t


def f_stage_5(regio, stage="5-added_nearest_substation"):
    """
    Stage 5:
    Adds the distance from the 'area' to the nearest the following points or areas:
        - substation
        - residential area
        - industrial area
        - wind park
        - solar park
    """
    print("")
    print(f">> {regio}: nearest substation")
    t0 = timeit.default_timer()

    gdf = util.load_prev_stage_to_gdf(regio, stage)

    power_file = "/common/ecap/resources/power/substations-germany.csv"

    power_df = pd.read_csv(power_file, delimiter=";", encoding="utf8")
    power_df["Koordinaten"] = power_df["Koordinaten"].apply(
        lambda x: Point(to_tuple(x))
    )
    # print(df["Koordinaten"])
    power_gdf = gpd.GeoDataFrame(power_df, geometry="Koordinaten")
    power_gdf["geometry"] = power_gdf["Koordinaten"]
    power_gdf = power_gdf.drop(columns=["Koordinaten"])
    power_gdf = power_gdf.set_crs(4326).to_crs(25832)

    print("")
    print("")

    gdf["nearest_subst_distance_km"] = None
    gdf["nearest_subst_info"] = None

    for i in range(len(gdf)):
        print(i, "/", len(gdf))
        # Iterate through all geometries/rows of gdf,
        # make a new gdf from the single geometries
        # and sjoin it with the power_gdf to get the
        # nearest station to that geomertry.
        geom = gdf.iloc[i, gdf.columns.get_loc("geometry")]
        geom_gdf = gpd.GeoDataFrame(
            [Point((0.0, 0.0))], columns=["geometry"], geometry="geometry"
        )
        geom_gdf.iat[0, 0] = geom

        distances = gpd.sjoin_nearest(geom_gdf, power_gdf, distance_col="distance")

        int_dict = {}

        gdfdict = distances[
            distances["distance"] == distances["distance"].min()
        ].to_dict()

        for k, v in gdfdict.items():
            for key, val in v.items():
                if "right" in str(k) or "left" in str(k):
                    continue
                else:
                    int_dict[k] = val

        gdf.at[i, "nearest_subst_info"] = json.dumps(int_dict)

        distances["nearest_subst_distance_km"] = distances["distance"].apply(
            lambda x: x / 1000
        )

        gdf.at[i, "nearest_subst_distance_km"] = distances[
            "nearest_subst_distance_km"
        ].min()

    stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)

    if stage_successfully_saved != False:

        print("Nearest substation & info added and GDF saved to file")
        print("")

        return True
    else:
        return False
