import os
import sys
from shapely.geometry import Point
from math import radians, cos, sin, asin, sqrt


def flatten(x):
    if isinstance(x, list):
        return [a for i in x for a in flatten(i)]
    else:
        return [x]


list1 = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, [16, 17, 18]]]

r = flatten(list1)
print(r)

#
# def lreverse(liste):
#     return liste[::-1]
#
#
# p_1 = lreverse()
# p_2 = lreverse(list(p2.coords)[0])
#
#
# lon1, lat1 = p_1
# lon2, lat2 = p_2
#
# print("P1")
# print(lon1, lat1)
#
# print("P2")
# print(lon2, lat2)
#
# print("DISTANCE")
# print(haversine(p1, p2))


# import geopandas as gpd
#
# maindir = "/common/ecap/prospector_data/results/stages/"
#
# stages = [
#     "2-added_centroids",
#     "3-filtered_by_intersection_protected_area",
#     "4-added_nearest_protected_area",
# ]
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
# # for stage in stages:
# #     stage_path = os.path.join(maindir, stage)
# #     for regio in regions:
# #         regio_dir = os.path.join(stage_path, regio)
# #
# #         gpkg_dir = os.path.join(regio_dir, "gpkg")
# #         for file in os.listdir(gpkg_dir):
# #             file_path = os.path.join(gpkg_dir, file)
# #             print(file_path)
# #             os.remove(file_path)
