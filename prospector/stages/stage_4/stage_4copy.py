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

import prot_areas

from display.display import Display


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")


def haversine_ultra(np_prot_area):
    # print(np_prot_area)
    for x in np.nditer(np_prot_area):
        print(x)
    return None
    # lon1, lat1 = p1[::-1]
    # lon2, lat2 = p2[::-1]
    #
    # # convert decimal degrees to radians
    # lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    #
    # # haversine formula
    # dlon = lon2 - lon1
    # dlat = lat2 - lat1
    # a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    # c = 2 * asin(sqrt(a))
    # r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    # return c * r


def find_distances(row, all_overlap_cols, pa_abbr, counter, max_len):
    # We iterate over all GDF rows
    # Get the geometry
    geom = Geometry(wkt.dumps(row.geometry))

    overlap_cols = [col for col in all_overlap_cols if row[col] == False]

    # Print status bar for the row iteration
    counter += 1
    # Display.status_bar(counter, len(gdf))
    print(counter / max_len)

    # We iterate over all prot area column names and
    # get their respective boolean value for this row.
    # If the overlap column value is FALSE, we work with this row.
    # Otherwise, we don't need the distance.

    for col in overlap_cols:
        # We just pass the column name to the overlap_data dict
        # with next key: "data" to retrieve the preloaded
        # GDF for this prot area.
        gdf_prot_area = prot_areas.overlap_data[col]["data"]

        # 0. Convert to EPSG:4326
        # Since we have to use our own haversine function for
        # distance calculations, which requires EPSG:4326
        # coordinates, we first need to transform the geometries

        gdf_prot_area = gdf_prot_area.to_crs(4326)
        gdf_prot_area["centroid"] = gdf_prot_area.centroid

        # gdf_prot_area["centroid"] = gdf_prot_area["centroid"].apply(
        #     lambda x: wkt.dumps(x)
        # )
        # gdf_prot_area["centroid"] = gdf_prot_area["centroid"].apply(
        #     lambda x: Geometry(x)
        # )

        # gdf_prot_area["centroid"].apply(lambda x: print(x, type(x)))

        # res = gdf_prot_area["centroid"].apply(lambda c: util.haversine(geom, c))
        #
        # pa_distance = res.min()

        # # os._exit(1)
        #
        prot_area_centroid_gdf = gpd.GeoDataFrame(
            [gdf_prot_area["centroid"]],
            columns=["geometry"],
            geometry="geometry",
        )

        nlist = [wkt.loads(row.centroid)]

        centroid_gdf = gpd.GeoDataFrame(
            nlist, columns=["geometry"], geometry="geometry"
        )

        try:
            # Via spatial join we find the distance between
            # geometry and all protected areas
            gdf_distance = gpd.sjoin_nearest(
                centroid_gdf,
                prot_area_centroid_gdf,
                distance_col="distance",
                how="right",
                # how='right' provides the whole prot_area gdf
                # with distance values.
            )
        except ValueError as err:
            print("CENTROID GDF:")
            print(centroid_gdf)
            print("")
            print("PROT AREA GDF:")
            print(gdf_prot_area)
            raise

        # We then sort by ascending distance
        sorted_dist_gdf = gdf_distance.sort_values(by=["distance"], ascending=True)

        # Then we fetch the 10 prot areas with the
        # shortest distances and store to a new gdf
        sorted_dist_gdf = sorted_dist_gdf.head(10)

        deletable = ["distance", "index_left", "index_right"]
        del_cols = [x for x in deletable if x in list(sorted_dist_gdf.columns)]

        for col in del_cols:
            sorted_dist_gdf = sorted_dist_gdf.drop(columns=[col])

        # With the new gdf we make again spatial join,
        # but this time with the original polygon and
        # not just the centroid. That way we will limit the
        # initial processing time and will apply the expensive
        # processing only to the ten closest areas.

        # Make new gdf with the true geometry
        geom_gdf = gpd.GeoDataFrame([geom], columns=["geometry"], geometry="geometry")

        # Make spatial join
        final_distance_gdf = gpd.sjoin_nearest(
            geom_gdf,
            sorted_dist_gdf,
            distance_col="distance",
            how="right",
            # With 'right' it returns the whole prot_area gdf
            # with distance values.
        )

        # Sort by distance ascending
        final_distance_gdf = final_distance_gdf.sort_values(
            by=["distance"], ascending=True
        )

        # We extract the distance value, which already is
        # in meters. Divide by 1000 to get distance in km.
        pa_distance = final_distance_gdf.iloc[0]["distance"] / 1000

        return pa_distance


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
            # Convert GDF to EPSG:4326
            if gdf.crs is None:
                gdf = gdf.set_crs(config["epsg"][regio], allow_override=True).to_crs(
                    4326
                )
            else:
                gdf = gdf.to_crs(4326)

            all_overlap_cols = list(prot_areas.overlap_data.keys())

            # First, we add some new columns to the GDF by iterating over
            # all overlap columns. To make them distinct, we need readable
            # column names.Then we fill them with 'None'.
            counter = 0

            for column in all_overlap_cols:
                pa_abbr = column.split("_")[0]
                gdf[f"nearest_{pa_abbr}"] = gdf.apply(
                    lambda row: find_distances(
                        row, all_overlap_cols, pa_abbr, counter, len(gdf)
                    ),
                    axis=1,
                )

            # After iterating over all rows, transform back to original CRS
            # and then save extended GDF to file
            gdf = gdf.to_crs(config["epsg"][regio])
            stage_successfully_saved = util.save_current_stage_to_file(
                gdf, regio, stage
            )
            if stage_successfully_saved:
                print("")
                print(f"Stage 4 for {regio} successfully saved to file")
                print("")

                return True
            else:
                return False
    else:
        print(f"Stage 4 for {regio} already exists. Skipping...")
        return True
