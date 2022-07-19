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
                print("gdf_non_overlapping:", gdf_non_overlapping.columns)
                # Load PA data to gdf
                pa_file = f"/common/ecap/prospector_data/src_data/protected_areas/gpkg/{overlap_file_name}.gpkg"

                pa_data = gpd.read_file(pa_file)
                pa_gdf = gpd.GeoDataFrame(pa_data).set_crs(25832, allow_override=True)

                # print(pa_gdf.columns)
                # sys.exit()

                # sjoin and get distances
                dist_gdf = gpd.sjoin_nearest(
                    gdf_non_overlapping,
                    pa_gdf,
                    distance_col=f"{overlap_col.replace('_overlap','')}_distance",
                )

                print("dist_gdf", dist_gdf.columns)

                pd_dist = pd.DataFrame(dist_gdf)

                pd_gdf = pd.DataFrame(gdf)

                mrgd_gdf = pd.merge(pd_gdf, pd_dist, how="outer", on="id")

                gdf = gpd.GeoDataFrame(mrgd_gdf)

                # Drop double columns
                drop_cols = [
                    "landuse_y",
                    "timestamp_y",
                    "version_y",
                    "tags_y",
                    "osm_type_y",
                    "changeset_y",
                    "area_m2_y",
                    "area_ha_y",
                    "centroid_y",
                    "lsg_overlap_y",
                    "nsg_overlap_y",
                    "biosphaere_overlap_y",
                    "fauna_flora_overlap_y",
                    "nationalparks_overlap_y",
                    "naturmonumente_overlap_y",
                    "vogelschutz_overlap_y",
                    "geometry_y",
                    "index_right",
                    "gml_id",
                    "OBJECTID",
                    "SHAPE_LENG",
                    "LEG_DATE",
                    "BL",
                    "CDDA_CODE",
                    "IUCN_KAT",
                    "FLAECHE",
                    "STATUS",
                ]

                gdf = gdf.drop(columns=drop_cols)

                # Rename columns that got an '_x' at the end back to normal
                rename_cols = [
                    "landuse_x",
                    "id",
                    "timestamp_x",
                    "version_x",
                    "tags_x",
                    "osm_type_x",
                    "changeset_x",
                    "area_m2_x",
                    "area_ha_x",
                    "centroid_x",
                    "lsg_overlap_x",
                    "nsg_overlap_x",
                    "biosphaere_overlap_x",
                    "fauna_flora_overlap_x",
                    "nationalparks_overlap_x",
                    "naturmonumente_overlap_x",
                    "vogelschutz_overlap_x",
                    "geometry_x",
                    "lsg_distance",
                    "NAME",
                ]

                for old_col in rename_cols:
                    new_col = old_col.replace("_x", "")
                    gdf[new_col] = gdf[old_col]

                gdf = gdf.drop(columns=[rename_cols])

                print("Final gdf:", gdf.columns)

                sys.exit()
