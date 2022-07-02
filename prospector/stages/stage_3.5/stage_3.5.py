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
            # First, we add some new columns to the GDF by iterating over
            # all overlap columns.
            # To make them distinct, we need readable column names.
            # Then we fill them with 'None'.
            overlap_cols = list(prot_areas.overlap_data.keys())
            for column in overlap_cols:
                pa_abbr = column.split("_")[0]
                gdf[f"nearest_{pa_abbr}"] = None

            # Iterate over GDF
            for i in range(0, len(gdf)):

                # We iterate over all GDF rows
                row = gdf.iloc[i]
                if type(row["geometry"]) == MultiPolygon:

                    # Convert to regular polygon!
                    apply_concave_hull = False
                    if apply_concave_hull == True:
                        new_geometry = []
                        for i in range(len(gdf)):
                            row = gdf.iloc[i]

                            if type(row["geometry"]) == MultiPolygon:
                                try:
                                    poly_coordinates = [
                                        list(polygon.exterior.coords)
                                        for polygon in row["geometry"]
                                    ]

                                    coordslist = [
                                        y for x in poly_coordinates for y in x
                                    ]

                                    for poly in poly_coordinates:
                                        for val in poly:
                                            coordslist.append(val)

                                    poly_coordinates = [list(x) for x in coordslist]
                                    poly_coordinates = np.array(poly_coordinates)

                                    hull = CH.concaveHull(
                                        poly_coordinates,
                                        round(len(poly_coordinates) / 5, 0),
                                    )
                                    hull = [Point(x[0], x[1]) for x in hull]

                                    new_geometry.append(Polygon(hull))
                                except:
                                    new_geometry.append(row["geometry"])
                                    pass

                            else:
                                new_geometry.append(row["geometry"])

                        tmp_gdf = gpd.GeoDataFrame(new_geometry, columns=["geometry"])

                        if len(new_geometry) > 0:
                            gdf["geometry"] = tmp_gdf["geometry"]
