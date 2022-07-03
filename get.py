import os
import sys


import geopandas as gpd

maindir = "/common/ecap/prospector_data/results/stages/"

stages = [
    "2-added_centroids",
    "3-filtered_by_intersection_protected_area",
    "4-added_nearest_protected_area",
]

regions = [
    "baden_wuerttemberg",
    "bayern",
    "brandenburg",
    "berlin",
    "bremen",
    "hamburg",
    "hessen",
    "mecklenburg_vorpommern",
    "niedersachsen",
    "nordrhein_westfalen",
    "rheinland_pfalz",
    "saarland",
    "sachsen_anhalt",
    "sachsen",
    "schleswig_holstein",
    "thueringen",
]

# for stage in stages:
#     stage_path = os.path.join(maindir, stage)
#     for regio in regions:
#         regio_dir = os.path.join(stage_path, regio)
#
#         gpkg_dir = os.path.join(regio_dir, "gpkg")
#         for file in os.listdir(gpkg_dir):
#             file_path = os.path.join(gpkg_dir, file)
#             print(file_path)
#             os.remove(file_path)
