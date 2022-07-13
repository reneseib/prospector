import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

import multiprocessing
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

# import prot_areas


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def geom_intersects_pa(protected_area, geom, overlap_list):
    overlap_result = False

    # Check if any of these conditions are true
    res1 = protected_area["geometry"].contains(geom).any()
    res2 = protected_area["geometry"].within(geom).any()
    res3 = protected_area["geometry"].overlaps(geom).any()

    if res1 == True or res2 == True or res3 == True:
        overlap_result = True

    overlap_list.append(overlap_result)

    return None


def intersect_wrapper(regio, stage, gdf, overlap_data, i):
    key = list(overlap_data.keys())[i]

    print(f"Working on {key} in {regio}")

    overlap_list = []
    protected_area = overlap_data[key]["data"]

    print(f"Finding intersections for {key} now...")

    # Original function
    gdf["geometry"].apply(
        lambda geom: geom_intersects_pa(protected_area, geom, overlap_list)
    )

    # Append results to dataframe
    gdf[key] = overlap_list

    # Save processing results to disk
    stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)
    if stage_successfully_saved:
        print("Stage successfully saved to file")
        return True
    else:
        return False


def f_stage_3(regio, stage="3-filtered_by_intersection_protected_area"):
    """
    Stage 3:
    Checks for intersections with any kind of protected area.
    """
    print(f"Working on {regio} now:")
    # 1. Get path to all prot_area files
    prot_area_dir_path = os.path.join(src_data_dir, "protected_areas", "gpkg")

    # 2. Build output file path
    output_file_gpkg = os.path.join(
        results_dir,
        "stages",
        stage,
        regio,
        "gpkg",
        f"{regio}-{stage}.gpkg",
    )
    # 3. Before starting the whole process, check if the
    # output file already exists
    if not os.path.isfile(output_file_gpkg):

        # 4. Check if prot_area_dir exists
        if os.path.isdir(prot_area_dir_path):

            # 5. Load previous stage data from file
            gdf = util.load_prev_stage_to_gdf(regio, stage)

            if len(gdf) > 0:
                print(f"Loaded the previous stage data for {regio}")
                print("Checking for intersections now...")

                # 6. Check for intersections/overlaps and
                # save all interim results
                overlap_data = prot_areas.overlap_data

                all_stages_successful = []

                for i in range(len(overlap_data.keys())):
                    stage_successfully_saved = intersect_wrapper(
                        regio, stage, gdf, overlap_data, i
                    )
                    all_stages_successful.append(stage_successfully_saved)

                if all(all_stages_successful) != False:

                    print(f"All intersections added and saved to file")
                    print("")

                    return True

                else:
                    return False

            else:
                return False
        else:
            return False

    else:
        print(f"{output_file_gpkg} already exists. Skipping this one")
        return True
