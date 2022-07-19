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
import pandas as pd
from math import radians, cos, sin, asin, sqrt
import timeit
import re

from pygeos import Geometry, distance


# Custom imports
from pconfig import config

for dir in config["init"]["prospector_package_path"]:
    sys.path.append(dir)

from util import util

import prot_areas

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
        t = timeit.default_timer()
        print(f"Working on {regio} now:")

        # Load file from previous stage to a GDF
        gdf = util.load_prev_stage_to_gdf(regio, stage)
        print(f"{regio}'s previous stage loaded - starting to iterate over GDF")

        if len(gdf) > 0:
            # Set to their original CRS, project always to epsg:25832
            gdf = gdf.set_crs(config["epsg"][regio], allow_override=True).to_crs(25832)

            all_overlap_cols = list(prot_areas.overlap_data.keys())

            # Loop over all PA geometries against
            # the non-overlapping geometries
            for i in range(len(all_overlap_cols)):
                overlap_col = all_overlap_cols[i]
                overlap_file_name = prot_areas.prot_area_names[i]

                # Filter GDF for non-overlapping rows at this PA
                gdf_non_overlapping = gdf[gdf[overlap_col] == False]

                # Load PA data to gdf
                pa_file = f"/common/ecap/prospector_data/src_data/protected_areas/gpkg/{overlap_file_name}.gpkg"

                pa_data = gpd.read_file(pa_file)
                pa_gdf = gpd.GeoDataFrame(pa_data).set_crs(25832, allow_override=True)

                pa_drop_columns = [
                    "gml_id",
                    "OBJECTID",
                    "SHAPE_LENG",
                    "LEG_DATE",
                    "BL",
                    "CDDA_CODE",
                    "IUCN_KAT",
                    "FLAECHE",
                    "STATUS",
                    "MARIN_AREA",
                    "BIOGEO",
                    "WRRL",
                    "LINK",
                    "SITECODE",
                    "JAHR",
                ]

                for drop_col in pa_drop_columns:
                    if drop_col in pa_gdf.columns:
                        pa_gdf = pa_gdf.drop(columns=[drop_col])

                # sjoin and get distances
                dist_gdf = gpd.sjoin_nearest(
                    gdf_non_overlapping,
                    pa_gdf,
                    distance_col=f"{overlap_col.replace('_overlap','')}_distance",
                )

                pd_dist = pd.DataFrame(dist_gdf)

                pd_gdf = pd.DataFrame(gdf)
                try:
                    dist_target_cols = list(pd_dist.columns.difference(pd_gdf.columns))

                    dist_target_cols.append("id")

                    for col in dist_target_cols:
                        if "_left" in col or "_right" in col:
                            dist_target_cols.remove(col)

                    mrgd_gdf = pd.merge(
                        pd_gdf,
                        pd_dist[dist_target_cols],
                        how="outer",
                        on="id",
                    )

                except:
                    print("pd_gdf:", pd_gdf.columns)
                    print("pd_dist:", pd_dist.columns)
                    raise

                gdf = gpd.GeoDataFrame(mrgd_gdf)

                for col in gdf.columns:
                    if "_left" in col or "_right" in col:
                        gdf = gdf.drop(columns=[col])

                # Rename columns that got an '_x' or '_y' at the end

                # print("HERE WE GO")
                # all_cols = list(gdf.columns)
                # for i in range(len(gdf.columns)):
                #     old_col = all_cols.pop()
                #     pattern = re.compile(r"(left_x|left_y|right_x|right_y|_x|_y)")
                #
                #     new_col = re.sub(pattern, "", old_col)
                #
                #     if new_col not in all_cols and old_col != "id":
                #         gdf[new_col] = gdf[old_col]
                #         gdf = gdf.drop(columns=[old_col])
                #     elif "distance" in new_col and new_col not in all_cols:
                #         gdf[new_col] = gdf[old_col]
                #         # gdf = gdf.drop(columns=[old_col])
                #     elif old_col != "id":
                #         gdf = gdf.drop(columns=[old_col])

                print("Final gdf:", gdf.columns)

        print("for all PA:", timeit.default_timer() - t)
