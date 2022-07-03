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
from pyproj import Geod, CRS

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


def f_stage_1(regio, stage="1-filtered_by_size"):
    gdf = util.load_prev_stage_to_gdf(regio, stage)

    # Source CRS depends on the state, Western Germany's state are 25832, Eastern 25833
    src_crs = config["epsg"][regio]

    if gdf.crs == None:
        # Apply the CRS to the GDF
        gdf = gdf.set_crs(src_crs, allow_override=True)

    if len(gdf) > 0:
        print("Starting stage processing...")
        # 1. Copy GDF and transform to planar projection
        trans_gdf = gdf.copy()
        trans_gdf = trans_gdf.to_crs(3035)

        # 2. Get planar areas in m2
        trans_gdf["area_m2"] = trans_gdf["geometry"].area

        # 3. Divide m2 values by 10.000 in order to get the hectares
        trans_gdf["area_ha"] = trans_gdf["area_m2"].apply(lambda x: (x / (10 * 1000)))

        # 4. Transform geometry back to original
        trans_gdf = trans_gdf.to_crs(src_crs)

        # 5. Filter by size - here: greater than 10 hectares
        gdf = trans_gdf[trans_gdf["area_ha"] > 10]

        # 6. Save processing results to disk
        stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)

        if stage_successfully_saved != False:
            print(f"Saved {regio} with areas\n")
            return True
        else:
            return False

    else:
        return False
