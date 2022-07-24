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


def f_stage_5(regio, stage="5-added_nearest_substation"):
    """
    Stage 5:
    Adds the distance from the 'area' to the nearest the following points and areas:
        - substation
        - residential area
        - commercial
        - industrial area
        - wind park
        - solar park
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
    print(f"Starting stage 5 now")

    # if not os.path.isfile(output_file_gpkg):
    # TODO: Reverse - Just for testing purposes

    """
    Actually we would need to add another loop here, to iterate over the different directories in the "others" dir, such as 'residential', 'commercial', 'wind' etc.

    For now, we just try it with a single target_osm file to check how that works
    """
    if 1 == 1:
        t = timeit.default_timer()
        print(f"Working on {regio} now:")

        # Load file from previous stage to a GDF
        gdf = util.load_prev_stage_to_gdf(regio, stage)
        print(f"{regio}'s previous stage loaded - starting to iterate over GDF")

        targets = {
            "solar": f"{regio}-solar_power",
            "wind": f"{regio}-wind_power",
            "hydro": f"{regio}hydro_power",
            "residential": f"{regio}-residential_area",
            "commercial": f"{regio}-commercial_area",
            "industrial": f"{regio}-industrial_area",
            "raildways": f"{regio}-railways",
            "roads": f"{regio}-roads",
        }

        for target, file_name in targets.items():

            target_osm = f"/common/ecap/prospector_data/src_data/others/{target}/{regio}/{file_name}.gpkg"

            target_gdf = gpd.read_file(target_osm)

            if len(gdf) > 0 and len(target_gdf) > 0:
                # Set to their original CRS, project always to epsg:25832 in case we run against something on national level which is always in 25832
                if config["epsg"][regio] != 25832:
                    print("need to convert those gdfs")
                    gdf = gdf.set_crs(
                        config["epsg"][regio], allow_override=True
                    ).to_crs(25832)
                    target_gdf = target_gdf.set_crs(
                        config["epsg"][regio], allow_override=True
                    ).to_crs(25832)

                print("starting sjoin now")

                dist_gdf = gpd.sjoin_nearest(gdf, target_gdf, distance_col="distance")

                print(dist_gdf["distance"])

            #     # Save processing results to disk
            #     stage_successfully_saved = util.save_current_stage_to_file(
            #         gdf, regio, stage
            #     )
            #     if stage_successfully_saved:
            #         cols = []
            #         for col in gdf.columns:
            #             if "distance" in col:
            #                 cols.append(col)
            #         print(gdf[cols].head(20))
            #         print("Stage successfully saved to file")
            #         return True
            #     else:
            #         return False
            #
            #     print("for all PA:", timeit.default_timer() - t)
            #
            # else:
            #     # File already exists, return False
            #     return False
