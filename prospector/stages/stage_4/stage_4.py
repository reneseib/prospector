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
            # This is where the magic happens
            for i in range(0, len(gdf)):

                # We iterate over all GDF rows
                row = gdf.iloc[i]

                # Print status bar
                Display.status_bar(i, len(gdf))

                # Build a list with all overlap column names
                overlap_cols = list(prot_areas.overlap_data.keys())

                # We iterate over all column names and get their respective row value
                for col in overlap_cols:

                    # If the overlay col value is FALSE, we work with this row.
                    # Otherwise, we just pass to the next row
                    if row[col] == False:
                        print(col)
                        pa_abbr = col.split("_")[0]

                        # TO DO: Find the nearest protected area

                        # We just pass the col name to the overlap_data dict
                        # and key: "data" to get the already loaded
                        # prot area GDF
                        gdf_prot_area = prot_areas.overlap_data[col]["data"]

                        # We then have to make a GDF with a single point - the one we want to find the next prot area for
                        nlist = [wkt.loads(row["centroid"])]

                        centroid_gdf = gpd.GeoDataFrame(
                            nlist, columns=["geometry"], geometry="geometry"
                        )

                        # Via spatial join we find the nearest
                        # protected area to that geometry
                        gdf_distance = gpd.sjoin_nearest(
                            centroid_gdf,
                            gdf_prot_area,
                            distance_col="distance",
                            how="right",
                            # 'right' provides the whole prot_area gdf
                            # with distance values.
                        )

                        # We then sort by distance increasing
                        sd_gdf = gdf_distance.sort_values(
                            by=["distance"], ascending=True
                        )

                        # Then we fetch the 10 prot areas with the
                        # shortest distances and store to new gdf
                        sd_gdf = sd_gdf.head(10)
                        sd_gdf = sd_gdf.drop(columns=["distance"], axis=1)

                        if "index_left" in sd_gdf.columns:
                            sd_gdf = sd_gdf.drop(columns=["index_left"], axis=1)
                        if "index_right" in sd_gdf.columns:
                            sd_gdf = sd_gdf.drop(columns=["index_right"], axis=1)

                        # With the new gdf we make again spatial join,
                        # but this time with the original geometry and
                        # not just the centroid. That way we will limit the
                        # initial processing time and will apply the hardcore
                        # processing to only the ten closest areas.

                        # Make new gdf with the true geometry
                        geom_gdf = gpd.GeoDataFrame(
                            [row["geometry"]], columns=["geometry"], geometry="geometry"
                        )

                        final_distance_gdf = gpd.sjoin_nearest(
                            geom_gdf,
                            sd_gdf,
                            distance_col="distance",
                            how="right",
                            # 'right' provides the whole prot_area gdf
                            # with distance values.
                        )

                        # We extract the distance value, which already is
                        # in meters. Divide by 1000 to get distance in km.
                        pa_distance = final_distance_gdf.iloc[0]["distance"] / 1000

                        # We add the distance to the row in GDF
                        gdf.loc[i, f"nearest_{pa_abbr}"] = pa_distance

            # At last save all to disk
            stage_successfully_saved = util.save_current_stage_to_file(
                gdf, regio, stage
            )
            if stage_successfully_saved:
                print("")
                print(f"Stage 4 for {regio} successfully saved to disk")
                print("")

                return True
            else:
                return False
    else:
        print(f"Stage 4 for {regio} already exists. Skipping...")
        return True
