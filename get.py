import os
import sys

print(sys.getrecursionlimit())
#
#
# import geopandas as gpd
#
# maindir = "/common/ecap/prospector_data/results/stages/"
#
# stage05 = "0_5-convert-multipolygons-to-polygon"
#
#
# regions = [
#     "baden_wuerttemberg",
#     "bayern",
#     "brandenburg",
#     "berlin",
#     "bremen",
#     "hamburg",
#     "hessen",
#     "mecklenburg_vorpommern",
#     "niedersachsen",
#     "nordrhein_westfalen",
#     "rheinland_pfalz",
#     "saarland",
#     "sachsen_anhalt",
#     "sachsen",
#     "schleswig_holstein",
#     "thueringen",
# ]
#
# stage05_dir = os.path.join(maindir, stage05)
# if os.path.isdir(stage05_dir) != True:
#     os.mkdir(stage05_dir)
#
#
# for regio in regions:
#     regio_dir = os.path.join(stage05_dir, regio)
#     if os.path.isdir(regio_dir) != True:
#         os.mkdir(regio_dir)
#
#     csv_dir = os.path.join(regio_dir, "csv")
#     gpkg_dir = os.path.join(regio_dir, "gpkg")
#
#     if os.path.isdir(csv_dir) != True:
#         os.mkdir(csv_dir)
#
#     if os.path.isdir(gpkg_dir) != True:
#         os.mkdir(gpkg_dir)
