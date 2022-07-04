import timeit
import random
from shapely.geometry import Point
from shapely import wkt
import numpy as np
import geopandas as gpd
import os
import sys
from math import radians, cos, sin, asin, sqrt
from numba import njit
from multiprocessing import Process, Manager

# geoms = open("testdata.txt", "r").read()

file_name = "baden_wuerttemberg-3-filtered_by_intersection_protected_area.gpkg"
file_dir = "/common/ecap/prospector_data/results/stages/3-filtered_by_intersection_protected_area/baden_wuerttemberg/gpkg"

file_path = os.path.join(file_dir, file_name)

data = gpd.read_file(file_path)

gdf = gpd.GeoDataFrame(data).set_crs(25832, allow_override=True).to_crs(4326)

geoms = gdf.iloc[0]["geometry"].exterior.coords

geoms = [Point(x) for x in geoms]

g = [np.asarray((p.coords))[0] for p in geoms]

arr = np.empty(
    (len(g), 2),
    dtype=[("all", np.float64)],
)
arr[:] = g


@njit(fastmath=True)
def haversine(p1, p2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      IMPORTANT: ONLY WORKS WITH WSG84 / EPSG:4326 coordinates
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    """

    if not np.array_equal(p1, p2):

        lon1, lat1 = p1[::-1]
        lon2, lat2 = p2[::-1]

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371
        # r = Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
        return c * r


def go(arr, point, times):
    # print("starting cycle")
    start = timeit.default_timer()
    for p in arr:
        p = np.array([p[0][0], p[1][0]])
        distance = haversine(p, point)

    end = timeit.default_timer() - start
    times.append(end)
    return distance


times = []

gdf["centroid"] = gdf["centroid"].apply(lambda x: wkt.loads(x))

tmp_gdf = gpd.GeoDataFrame(
    list(gdf["centroid"]), columns=["geometry"], geometry="geometry"
)
tmp_gdf = tmp_gdf.set_crs(25832).to_crs(4326)
gdf["centroid"] = tmp_gdf["geometry"]

gdf["distance"] = gdf["centroid"].apply(
    lambda x: go(arr, np.asarray(x.coords)[0], times)
)

print(gdf["distance"].min())
print(gdf["distance"].max())
#
# for i in range(len(gdf)):
#     go(arr, point, times)
# print("NJIT - With fastmath")
# print(times)
# print("AVG: ", (sum(times) / len(times)))
# print("MIN: ", min(times))
# print("MAX: ", max(times))
#
#
# #
# # def lreverse(liste):
# #     return liste[::-1]
# #
# #
# # p_1 = lreverse()
# # p_2 = lreverse(list(p2.coords)[0])
# #
# #
# # lon1, lat1 = p_1
# # lon2, lat2 = p_2
# #
# # print("P1")
# # print(lon1, lat1)
# #
# # print("P2")
# # print(lon2, lat2)
# #
# # print("DISTANCE")
# # print(haversine(p1, p2))
#
#
# # import geopandas as gpd
# #
# # maindir = "/common/ecap/prospector_data/results/stages/"
# #
# # stages = [
# #     "2-added_centroids",
# #     "3-filtered_by_intersection_protected_area",
# #     "4-added_nearest_protected_area",
# # ]
# #
# # regions = [
# #     "baden_wuerttemberg",
# #     "bayern",
# #     "brandenburg",
# #     "berlin",
# #     "bremen",
# #     "hamburg",
# #     "hessen",
# #     "mecklenburg_vorpommern",
# #     "niedersachsen",
# #     "nordrhein_westfalen",
# #     "rheinland_pfalz",
# #     "saarland",
# #     "sachsen_anhalt",
# #     "sachsen",
# #     "schleswig_holstein",
# #     "thueringen",
# # ]
# #
# # # for stage in stages:
# # #     stage_path = os.path.join(maindir, stage)
# # #     for regio in regions:
# # #         regio_dir = os.path.join(stage_path, regio)
# # #
# # #         gpkg_dir = os.path.join(regio_dir, "gpkg")
# # #         for file in os.listdir(gpkg_dir):
# # #             file_path = os.path.join(gpkg_dir, file)
# # #             print(file_path)
# # #             os.remove(file_path)
