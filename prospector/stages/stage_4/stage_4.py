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

                for col in gdf_non_overlapping.columns:
                    if "_left" in col or "_right" in col or "_x" in col or "_y" in col:
                        gdf_non_overlapping = gdf_non_overlapping.drop(columns=[col])

                print("Columns at start of iteration:")
                print(gdf_non_overlapping.columns)

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

                # TODO: Find solutions for this
                # Traceback (most recent call last):
                #     File 'main.py', line 20, in <module>
                #     prospector.staging([], stage='stage_2', finput=['input'])
                #     File '/common/ecap/prospector/prospector.py', line 145, in staging
                #     #         stage_finished = f(regio, stage=stage_name)
                #     File '/common/ecap/prospector/stages/stage_4/stage_4.py', line 130, in f_stage_4
                #     mrgd_gdf = pd.merge(
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/reshape/merge.py', line 122, in merge
                #     return op.get_result()
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/reshape/merge.py', line 725, in get_result
                #     result_data = concatenate_managers(
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/internals/concat.py', line 202, in concatenate_managers
                #     return _concat_managers_axis0(mgrs_indexers, axes, copy)
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/internals/concat.py', line 264, in _concat_managers_axis0
                #     mgrs_indexers = _maybe_reindex_columns_na_proxy(axes, mgrs_indexers)
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/internals/concat.py', line 306, in _maybe_reindex_columns_na_proxy
                #     mgr = mgr.reindex_indexer(
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/internals/managers.py', line 692, in reindex_indexer
                #     new_blocks = [
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/internals/managers.py', line 693, in <listcomp>
                #     blk.take_nd(
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/internals/blocks.py', line 1137, in take_nd
                #     new_values = algos.take_nd(
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/array_algos/take.py', line 117, in take_nd
                #     return _take_nd_ndarray(arr, indexer, axis, fill_value, allow_fill)
                #     File '/home/shellsquid/.local/lib/python3.8/site-packages/pandas/core/array_algos/take.py', line 158, in _take_nd_ndarray
                #     out = np.empty(out_shape, dtype=dtype)
                #     numpy.core._exceptions.MemoryError: Unable to allocate 256. GiB for an array with shape (8, 4294979403) and data type object

                except:
                    print("pd_gdf:", pd_gdf.columns)
                    print("pd_dist:", pd_dist.columns)
                    raise

                gdf = gpd.GeoDataFrame(mrgd_gdf)

                print(f"{regio} {overlap_col}: Columns after merge")
                showcols = []
                for col in gdf.columns:
                    if "distance" in col:
                        showcols.append(col)
                        pos = col.rfind("_") + 1
                        add_col = col[:pos] + "overlap"
                        showcols.append(add_col)
                print(gdf[showcols].head(20))
                print("\n\n")
                # print(gdf["lsg_overlap", "lsg_distance"].head(10))

                for col in gdf.columns:
                    if "_left" in col or "_right" in col or "_x" in col or "_y" in col:
                        gdf = gdf.drop(columns=[col])

        # Save processing results to disk
        stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)
        if stage_successfully_saved:
            print("Stage successfully saved to file")
            return True
        else:
            return False

        print("for all PA:", timeit.default_timer() - t)
