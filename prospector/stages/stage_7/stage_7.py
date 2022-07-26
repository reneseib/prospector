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


def f_stage_7(regio, stage="7-added_nearest_agrargen"):
    """
    Stage 7:
    Adds the name, details and distance of the area's
    nearest 'Agrargenossenschaft'
    """
    output_file_gpkg = os.path.join(
        results_dir,
        "stages",
        stage,
        regio,
        "gpkg",
        f"{regio}-{stage}.gpkg",
    )
    # Before starting the whole process, check if the output file already exists
    print(f"{regio}: Starting stage 7 now")

    agrar_file = "/common/ecap/prospector_data/src_data/entities/agrargenossenschaften/agrar-genossenschaften.gpkg"

    data = gpd.read_file(agrar_file)

    agro_gdf = gpd.GeoDataFrame(data)

    output_file_gpkg = os.path.join(
        results_dir,
        "stages",
        stage,
        regio,
        "gpkg",
        f"{regio}-{stage}.gpkg",
    )

    # Load previous stage data
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    agro_dist = gpd.sjoin_nearest(gdf, agro_gdf, distance_col="distance", how="inner")

    gdf["nearest_agrargen"] = None
    gdf["nearest_agrargen_info"] = None

    gdf = gdf.drop_duplicates(subset=["id"])
    agro_dist = agro_dist.drop_duplicates(subset=["id"])

    gdf = gdf.reset_index()
    agro_dist = agro_dist.reset_index()
    print(len(gdf))
    print(len(agro_dist))
    print("-----------------")
    if len(agro_dist) == len(gdf):
        for i in range(len(agro_dist)):
            print(i, "/", len(agro_dist), " - ", round((i / len(gdf)) * 100, 2), "%")
            distance = agro_dist.loc[i, "distance"]
            agro_row = agro_dist.loc[
                i,
                [
                    "firma",
                    "adresse",
                    "plz",
                    "stadt",
                    "tel",
                    "ackerland",
                    "hopfen",
                    "gruenland",
                    "wald",
                    "gemuese",
                    "energie",
                    "obst",
                    "blumen",
                    "gewuerze",
                    "wein",
                ],
            ]
            gdf.at[i, "nearest_agrargen_info"] = agro_row.to_json()
            gdf.at[i, "nearest_agrargen"] = distance

        print(f"{regio}: saving now")
        stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)
        if stage_successfully_saved == True:
            return True
        else:
            return False
    else:
        print("=========================================")
        print(f"ERROR: {regio} GDFs have not the same length!!")
        print("=========================================")

        return False
