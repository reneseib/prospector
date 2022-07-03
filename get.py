import os
import sys
from shapely.geometry import Point
from math import radians, cos, sin, asin, sqrt


def haversine(p1, p2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    IMPORTANT: ONLY WORKS WITH WSG84 / EPSG:4326 coordinates
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    """

    lon1, lat1 = list(p1.coords)[0][::-1]
    lon2, lat2 = list(p2.coords)[0][::-1]

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r


p1 = Point(49.1527825978, 3.45377150274)
p2 = Point(49.1530465065, 3.45400990762)

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

print("DISTANCE")
print(haversine(p1, p2))


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
