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

from concavehull import ConcaveHull as CH
from util import util

from display.display import Display


for dir in config["init"]["proj_path"]:
    if os.path.isdir(dir):
        proj_path = dir

main_dir = os.path.join(proj_path, list(config["directories"].keys())[0])
src_data_dir = os.path.join(main_dir, "src_data")
geo_data_dir = os.path.join(src_data_dir, "geo_data")
results_dir = os.path.join(main_dir, "results")

# How much is safe and so on - figure this params out!
sys.setrecursionlimit(5000)


def get_concave_hull(row):
    geom = row["geometry"]

    # Preset the return object `poly` to the input object that is
    # to be overwritten if a concave hull can be built. On error,
    # the input geometry will be returned.
    poly = geom

    if geom.type == MultiPolygon:
        try:
            # Make MultiPolygon into a list of lists
            poly_coordinates = [list(polygon.exterior.coords) for polygon in geom]
            coordslist = [y for x in poly_coordinates for y in x]

            # Flatten the list
            for poly in poly_coordinates:
                for val in poly:
                    coordslist.append(val)

            poly_coordinates = [list(x) for x in coordslist]
            poly_coordinates = np.array(poly_coordinates)

            hull = CH.concaveHull(
                poly_coordinates,
                round(len(poly_coordinates) / 5, 0),
            )
            if hull != False:
                hull = [Point(x[0], x[1]) for x in hull]
                poly = Polygon(hull)

            row["geometry"] = poly

            return row

        except:
            return row

    else:
        return row


def f_stage_0_5(regio, stage="0_5-convert-multipolygons-to-polygon"):
    output_file_gpkg = os.path.join(
        results_dir,
        "stages",
        stage,
        regio,
        "gpkg",
        f"{regio}-{stage}.gpkg",
    )
    # Before starting the whole process,
    # check if the output file already exists
    if not os.path.isfile(output_file_gpkg):
        print(f"\nWorking on {regio} now:")

        # Load file from previous stage to a GDF
        gdf = util.load_prev_stage_to_gdf(regio, stage)

        # Source CRS depends on the state, Western Germany's state are 25832, Eastern 25833
        src_crs = config["epsg"][regio]

        if gdf.crs != src_crs:
            # Apply the CRS to the GDF
            gdf = gdf.set_crs(src_crs).to_crs(src_crs)

        print(f"Previous stage 0 loaded - starting to iterate over GDF")

        if len(gdf) > 0:
            # Convert to list of dicts
            dict_gdf = gdf.to_dict("records")

            # Set up a pool for multiprocessing
            pool = multiprocessing.Pool(os.cpu_count() - 1)
            i = 0
            try:
                # Map all items of dict_gdf to the get_concave_hull function
                # so MultiPolygons will be converted to single Polygons
                gdf_polygons_only_results = pool.map(get_concave_hull, dict_gdf)
                if gdf_polygons_only_results:
                    gdf = gpd.GeoDataFrame(gdf_polygons_only_results)
                    print("Converted all MultiPolygons to single Polygons")
            except:
                raise

            finally:
                # To make sure processes are closed in the end,
                # even if errors happen
                print("Finished")
                pool.close()
                pool.join()

            try:
                gdf.to_file(output_file_gpkg, driver="GPKG")
                print(
                    f"Successfully filtered and saved results to\n{output_file_gpkg}\n"
                )
                return True
            except Exception as e:
                print("\n")
                raise
                print("\n")
                os._exit(1)
                return False
        else:
            print("Loaded GDF is len = 0")
            print("Something went wrong.")
            sys.exit()
