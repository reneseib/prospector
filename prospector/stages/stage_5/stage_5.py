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
        orig_gdf = util.load_prev_stage_to_gdf(regio, stage)

        gdf = orig_gdf.copy()

        # In case we run against national data (in 25832)
        # we would need to transform the geometries to 25832
        gdf = gdf.set_crs(config["epsg"][regio], allow_override=True).to_crs(25832)

        targets = {
            "solar": f"{regio}-solar_power",
            "wind": f"{regio}-wind_power",
            "hydro": f"{regio}-hydro_power",
            "residential": f"{regio}-residential_areas",
            "commercial": f"{regio}-commercial_areas",
            "industrial": f"{regio}-industrial_areas",
            "railways": f"{regio}-railways",
            "roads": f"{regio}-roads",
        }

        for target, file_name in targets.items():

            target_osm = f"/common/ecap/prospector_data/src_data/others/{target}/{regio}/{file_name}.gpkg"

            target_gdf = gpd.read_file(target_osm)

            if len(gdf) > 0 and len(target_gdf) > 0:
                # Set to their original CRS, project always to epsg:25832 in case we run against something on national level which is always in 25832

                for col in gdf.columns:
                    if (
                        "_left" in col
                        or "_right" in col
                        or "_x" in col
                        or "_y" in col
                        or col.startswith("np_")
                    ):
                        gdf = gdf.drop(columns=[col])

                if config["epsg"][regio] != 25832:
                    # In case we run against national data (in 25832)
                    # we would need to transform the geometries to 25832
                    target_gdf = target_gdf.set_crs(
                        config["epsg"][regio], allow_override=True
                    ).to_crs(25832)

                gdf["idx"] = gdf["id"]

                dist_gdf = gpd.sjoin_nearest(
                    gdf, target_gdf, distance_col="distance", how="left"
                )

                # Remove duplicates
                dist_gdf = dist_gdf.drop_duplicates(subset=["area_m2"])

                # for i in range(len(gdf)):
                #     idx = dist_gdf.loc[i, "idx"]
                #     id = gdf.loc[i, "id"]
                #     print(id)
                #     if not str(idx) == str(id):
                #         print(dist_gdf.loc[i])
                #         print(gdf.loc[i])
                #         os._exit(0)

                # Get back the normal id column
                dist_gdf["id"] = dist_gdf["idx"]
                dist_gdf = dist_gdf.drop(columns=["idx"])
                gdf = gdf.drop(columns=["idx"])

                # Convert to pandas dataframe to make the merge
                # which isn't possible in geopandas
                pd_dist = pd.DataFrame(dist_gdf)
                pd_gdf = pd.DataFrame(gdf)

                # pd_dist["id"] = pd_dist["id_left"]
                # pd_dist = pd_dist.drop(columns=["id_left", "id_right"])

                try:
                    dist_target_cols = list(pd_dist.columns.difference(pd_gdf.columns))

                    dist_target_cols.append("id")

                    # Remove duplicate columns
                    for col in dist_target_cols:
                        if (
                            "_left" in col
                            or "_right" in col
                            or "_x" in col
                            or "_y" in col
                            or col.startswith("np_")
                        ):
                            dist_target_cols.remove(col)

                    # Do the actual merge
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

                # Back to GDF & rename distance column
                gdf = gpd.GeoDataFrame(mrgd_gdf)
                gdf[f"nearest_{target}"] = gdf["distance"]
                gdf = gdf.drop(columns=["distance"])

                #
                # print(gdf.columns)
                # print(gdf[["id", f"nearest_{target}"]])
                gdf = gdf.drop_duplicates(
                    subset=["id", f"nearest_{target}", "geometry"]
                )

                print(gdf[["id", f"nearest_{target}"]])

                # Remove duplicate columns
                for col in gdf.columns:
                    if (
                        "_left" in col
                        or "_right" in col
                        or "_x" in col
                        or "_y" in col
                        or col.startswith("np_")
                    ):
                        gdf = gdf.drop(columns=[col])

    drop_cols = [
        "disused",
        "osm_type",
        "railway",
        "tags",
        "timestamp",
        "version",
        "nearest_railways",
        "access",
        "area",
        "bicycle",
        "bridge",
        "cycleway",
        "foot",
        "highway",
        "int_ref",
        "port",
        "junction",
        "lanes",
        "lit",
        "maxspeed",
        "motorcar",
        "motorroad",
        "name",
        "oneway",
        "overtaking",
        "psv",
        "ref",
        "segregated",
        "sidewalk",
        "smoothness",
        "surface",
        "tunnel",
        "turn",
        "width",
        "farmland",
        "meadow",
        "changeset",
        "power",
        "construction",
        "landuse",
        "residential",
        "commercial",
        "depot",
        "industrial",
        "busway",
        "footway",
        "motor_vehicle",
        "service",
        "tracktype",
    ]
    cols_to_drop = []
    for dcol in drop_cols:
        if dcol in gdf.columns:
            cols_to_drop.append(dcol)
    gdf = gdf.drop(columns=cols_to_drop)

    print(gdf.columns)
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
