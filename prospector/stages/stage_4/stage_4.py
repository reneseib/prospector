import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

import multiprocessing
from pyrosm import OSM, get_data
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon, mapping, shape
from shapely import wkt
from pyrosm import OSM, get_data
import numpy as np
from math import radians, cos, sin, asin, sqrt

from pygeos import Geometry, distance


# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util

# import prot_areas

from display.display import Display


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def f_stage_4(regio, stage="4-added_nearest_protected_area"):
    """
    Stage 4:
    Calculates the distances to the nearest protected areas
    where 'area' does not intersect a protected area.
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
    if not os.path.isfile(output_file_gpkg):
        print(f"Working on {regio} now:")

        # Load file from previous stage to a GDF
        gdf = util.load_prev_stage_to_gdf(regio, stage)
        print(f"{regio}'s previous stage loaded - starting to iterate over GDF")

        if len(gdf) > 0:
            # Convert GDF always to epsg:25832
            gdf = gdf.set_crs(config["epsg"][regio], allow_override=True).to_crs(25832)

            all_overlap_cols = list(prot_areas.overlap_data.keys())

            for i in range(len(all_overlap_cols)):
                overlap_col = all_overlap_cols[i]
                overlap_file_name = prot_areas.prot_area_names[i]
                # Filter for non-overlapping rows
                gdf_non_overlapping = gdf[gdf[overlap_col] == False]

                pa_file = f"/common/ecap/prospector_data/src_data/protected_areas/gpkg/lsg_gesamt_de.gpkg"
