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


def f_stage_2(regio, stage="2-added_centroids"):
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    if len(gdf) > 0:
        # THIS IS WHERE THE MAGIC HAPPENS
        print("Adding centroid now...")
        gdf["centroid"] = gdf["geometry"].centroid.apply(lambda x: wkt.dumps(x))

        # Save processing results to disk
        stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)

        if stage_successfully_saved != False:

            print("Centroid added and GDF saved to disk")
            print("")

            return True
        else:
            return False

    else:
        return False
