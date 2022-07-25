import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

import multiprocessing
import geopandas as gpd
from shapely import wkt
import numpy as np
import pandas as pd
import timeit
import re

# from pygeos import Geometry, distance


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
    print("starting now")

    # if not os.path.isfile(output_file_gpkg):
    # TODO: Reverse - Just for testing purposes
    if 1 == 1:
        t = timeit.default_timer()
        print(f"Working on {regio} now:")

        # Load file from previous stage to a GDF
        gdf = util.load_prev_stage_to_gdf(regio, stage)
        print(f"{regio}'s previous stage loaded - starting to iterate over GDF")

        # Convert gdf to 25832 if necessary
        if config["epsg"][regio] != 25832:
            gdf = gdf.set_crs(config["epsg"][regio], allow_override=True).to_crs(25832)

        if len(gdf) > 0:

            all_overlap_cols = list(prot_areas.overlap_data.keys())

            done_pa = []

            # Loop over all PA geometries against
            # the non-overlapping geometries
            for i in range(len(all_overlap_cols)):
                overlap_col = all_overlap_cols[i]
                overlap_file_name = prot_areas.prot_area_names[i]

                # Filter GDF for non-overlapping rows at this PA
                gdf_non_overlapping = gdf[gdf[overlap_col] != True]

                # Drop all unnecssary columns at the beginning
                for col in gdf_non_overlapping.columns:
                    if (
                        "_left" in col
                        or "_right" in col
                        or "_x" in col
                        or "_y" in col
                        or col.startswith("np_")
                    ):
                        gdf_non_overlapping = gdf_non_overlapping.drop(columns=[col])

                # To not oversize the gdf in memory, we need to drop
                # the done columns in the copy of gdf
                if len(done_pa) > 0:
                    gdf_non_overlapping = gdf_non_overlapping.drop(columns=done_pa)

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
                    "NAME",
                ]

                for drop_col in pa_drop_columns:
                    if drop_col in pa_gdf.columns:
                        pa_gdf = pa_gdf.drop(columns=[drop_col])

                # sjoin and get distances
                dist_gdf = gpd.sjoin_nearest(
                    gdf_non_overlapping,
                    pa_gdf,
                    distance_col=overlap_col.replace("_overlap", "_distance"),
                    how="left",
                )

                # Convert both GDF to pandas dataframe to make the merge
                # which isn't possible in geopandas
                pd_dist = pd.DataFrame(dist_gdf)
                pd_gdf = pd.DataFrame(gdf)

                try:
                    # Find the unique columns from pd_dist to keep in merge
                    dist_target_cols = list(pd_dist.columns.difference(pd_gdf.columns))

                    dist_target_cols.append("id")

                    # Remove duplicate columns with their weird extensions
                    for col in dist_target_cols:
                        if (
                            "_left" in col
                            or "_right" in col
                            or "_x" in col
                            or "_y" in col
                            or col.startswith("np_")
                        ):
                            dist_target_cols.remove(col)

                    # The actual merge on 'id' = merge rows with same 'id'
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

                # Convert back to geodataframe
                gdf = gpd.GeoDataFrame(mrgd_gdf)

                gdf = gdf.drop_duplicates(subset=["geometry"])

                # Show results
                print(f"{regio} {overlap_col}: Columns after merge")
                showcols = []

                # Just for visual check in terminal
                for col in gdf.columns:
                    if "distance" in col:
                        showcols.append(col)
                        pos = col.rfind("_") + 1
                        add_col = col[:pos] + "overlap"
                        showcols.append(add_col)
                print(gdf[showcols])
                print("\n\n")

                # Drop double and unnecssary columns
                for col in gdf.columns:
                    if "_left" in col or "_right" in col or "_x" in col or "_y" in col:
                        gdf = gdf.drop(columns=[col])

                # Once done, add the current overlap column to the "done" list
                # Also add the distance column of the overlap column so we can
                # drop them in the next iteration
                done_pa.append(overlap_col)

                pos = overlap_col.rfind("_") + 1
                overlap_dist_col = overlap_col[:pos] + "distance"
                done_pa.append(overlap_dist_col)

        # Convert back to original CRS
        # if gdf.crs == 25832 and 25832 != config["epsg"][regio]:
        gdf = gdf.set_crs(25832).to_crs(config["epsg"][regio])

        # Save processing results to disk
        stage_successfully_saved = util.save_current_stage_to_file(gdf, regio, stage)
        if stage_successfully_saved:
            cols = []
            for col in gdf.columns:
                if "overlap" in col or "distance" in col:
                    cols.append(col)
            print(gdf[cols].head(20))
            print("Stage successfully saved to file")
            return True
        else:
            return False

        print("for all PA:", timeit.default_timer() - t)

    else:
        # File already exists, return False
        return False
